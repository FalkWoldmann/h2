use crate::frame::{self, Reason};
use crate::proto::Error;

use bytes::BytesMut;

use std::io;
use std::task::Poll;

/// Initial capacity of the read buffer, and the smallest read size.
const INITIAL_READ_SIZE: usize = 8 * 1024;

/// Largest read size the adaptive strategy grows to.
const MAX_READ_SIZE: usize = 64 * 1024;

/// Length of the payload length field at the start of the frame header.
const LENGTH_FIELD_LEN: usize = 3;

/// Bytes read from the connection that have not been split into frames yet.
///
/// It decides how much room each read gets, but does no I/O itself:
/// [`FrameBuf::poll_frame`] takes the read to perform.
#[derive(Debug, Default)]
pub(super) struct FrameBuf {
    buf: BytesMut,

    /// Total length (head and payload) of the frame at the front of `buf`,
    /// set once its length field has been checked against the max frame size
    /// and kept while the frame is incomplete
    frame_len: Option<usize>,

    read_size: ReadSize,
}

impl FrameBuf {
    /// Splits the next frame off, head included, reading with `read` until
    /// one is complete.
    ///
    /// `read` appends to the buffer it is given and returns how many bytes it
    /// read, 0 meaning EOF. At EOF, a partial frame left in the buffer is an
    /// error.
    #[inline]
    pub(super) fn poll_frame(
        &mut self,
        max_frame_size: usize,
        mut read: impl FnMut(&mut BytesMut) -> Poll<io::Result<usize>>,
    ) -> Poll<Option<Result<BytesMut, Error>>> {
        loop {
            if let Some(frame) = self.split_frame(max_frame_size)? {
                return Poll::Ready(Some(Ok(frame)));
            }

            if ready!(self.read_with(&mut read))? == 0 {
                let remaining = !self.buf.is_empty();
                return Poll::Ready(remaining.then(|| Err(bytes_remaining())));
            }
        }
    }

    /// Splits the frame at the front off, head included, if it is complete.
    #[inline]
    fn split_frame(&mut self, max_frame_size: usize) -> Result<Option<BytesMut>, Error> {
        let frame_len = match self.frame_len.take() {
            Some(frame_len) => frame_len,
            None => match self.peek_frame_len(max_frame_size)? {
                Some(frame_len) => frame_len,
                None => return Ok(None),
            },
        };

        if self.buf.len() < frame_len {
            self.frame_len = Some(frame_len);
            return Ok(None);
        }

        self.read_size.record_frame(frame_len);
        Ok(Some(self.buf.split_to(frame_len)))
    }

    /// Reads the length of the frame at the front from its head, once the
    /// length field is buffered, and checks it against the max frame size.
    #[inline]
    fn peek_frame_len(&self, max_frame_size: usize) -> Result<Option<usize>, Error> {
        let (a, b, c) = match self.buf.get(..LENGTH_FIELD_LEN) {
            Some(&[a, b, c]) => (a, b, c),
            _ => return Ok(None),
        };

        let payload_len = u32::from_be_bytes([0, a, b, c]) as usize;
        if payload_len > max_frame_size {
            return Err(frame_too_large(payload_len, max_frame_size));
        }

        Ok(Some(payload_len + frame::HEADER_LEN))
    }

    /// Makes room for a read, lets `read` append to the buffer, and adapts
    /// the next read size to how much it read.
    ///
    /// Only called once `split_frame` found no complete frame, which is what
    /// `read_room` relies on.
    ///
    /// When `read` is pending with nothing buffered, a buffer that grew past
    /// its initial size is freed, so idle connections don't keep it.
    #[inline]
    fn read_with(
        &mut self,
        read: impl FnOnce(&mut BytesMut) -> Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        let len = self.buf.len();
        let (needed, capacity) = read_room(len, self.frame_len, self.read_size.get());
        if self.spare() < needed {
            self.buf.reserve(capacity - len);
        }

        let offered = self.spare();
        let res = read(&mut self.buf);
        match res {
            Poll::Ready(Ok(n)) => self.read_size.record_read(offered, n),
            Poll::Ready(Err(_)) => {}
            Poll::Pending => self.release_if_idle(),
        }
        res
    }

    fn spare(&self) -> usize {
        self.buf.capacity() - self.buf.len()
    }

    fn release_if_idle(&mut self) {
        if self.buf.is_empty() && self.buf.capacity() > INITIAL_READ_SIZE {
            self.buf = BytesMut::new();
            self.read_size.reset();
        }
    }
}

/// Returns the room the next read needs with `len` bytes buffered, and the
/// capacity to grow to if the buffer has less.
///
/// `frame_len` is the length of the frame at the front if its length field is
/// buffered, and then exceeds `len`. Otherwise `len` is below
/// `LENGTH_FIELD_LEN`.
///
/// Growing copies the bytes already buffered into a new allocation whenever
/// frames split off earlier still share the current one. So the buffer only
/// grows at a frame boundary, where at most a partial length field is
/// buffered, or when the frame being read does not fit. Otherwise a short read
/// finishes the frame in place.
#[inline]
fn read_room(len: usize, frame_len: Option<usize>, read_size: usize) -> (usize, usize) {
    match frame_len {
        // A frame that is large next to a read gets a full read after it, so
        // the frames that follow land in the same allocation.
        Some(frame_len) if frame_len > read_size / 2 => (frame_len - len, frame_len + read_size),
        Some(frame_len) => (frame_len - len, read_size),
        None => (read_size - len, read_size),
    }
}

// Errors are built out of line so that `poll_frame` stays small enough to
// inline. Marking the branches with `std::hint::cold_path` instead does not
// achieve that.

#[cold]
fn frame_too_large(payload_len: usize, max_frame_size: usize) -> Error {
    proto_err!(conn: "frame size {} over max {}", payload_len, max_frame_size);
    Error::library_go_away(Reason::FRAME_SIZE_ERROR)
}

#[cold]
fn bytes_remaining() -> Error {
    io::Error::new(io::ErrorKind::Other, "bytes remaining on stream").into()
}

/// Adaptive read size, modeled on hyper's HTTP/1 read strategy: it doubles
/// when a read fills the space offered, and halves after two reads in a row
/// that use less than half of it.
///
/// Unlike hyper, growth is also capped at a few times the average frame
/// length. Large reads pay off for large frames: they avoid a read and a
/// buffer copy per frame. For small frames, one read of the initial size
/// already covers hundreds of frames, so a larger buffer would only cost
/// memory.
#[derive(Debug)]
struct ReadSize {
    next: usize,
    decrease_now: bool,
    /// Moving average of the length of recent frames, head included
    avg_frame_len: usize,
}

impl Default for ReadSize {
    fn default() -> Self {
        ReadSize {
            next: INITIAL_READ_SIZE,
            decrease_now: false,
            avg_frame_len: 0,
        }
    }
}

impl ReadSize {
    fn get(&self) -> usize {
        let max = (self.avg_frame_len * 4).clamp(INITIAL_READ_SIZE, MAX_READ_SIZE);
        self.next.min(max)
    }

    /// Starts over from the smallest read size, keeping the frame average.
    fn reset(&mut self) {
        *self = ReadSize {
            avg_frame_len: self.avg_frame_len,
            ..ReadSize::default()
        };
    }

    /// Records a frame of `len` bytes, weighting it 1/8 in the average.
    #[inline]
    fn record_frame(&mut self, len: usize) {
        self.avg_frame_len = self.avg_frame_len - self.avg_frame_len / 8 + len / 8;
    }

    /// Records a read of `n` bytes into `offered` bytes of spare capacity.
    fn record_read(&mut self, offered: usize, n: usize) {
        let size = self.get();

        // Less than `size` is offered while part of a frame is buffered, so a
        // read that fills what it was offered counts as full too.
        if n >= size || (n == offered && n >= size / 2) {
            self.next = (size * 2).min(MAX_READ_SIZE);
            self.decrease_now = false;
        } else if offered < size {
            // A read limited by the space offered says nothing about the size
            // the socket could deliver.
        } else if n < size / 2 {
            if self.decrease_now {
                self.next = (size / 2).max(INITIAL_READ_SIZE);
            }
            self.decrease_now = !self.decrease_now;
        } else {
            self.decrease_now = false;
        }
    }
}

/// Proofs, checked with [Kani](https://model-checking.github.io/kani/) by
/// running `cargo kani --lib`, that the read sizing arithmetic holds for all
/// inputs.
#[cfg(kani)]
mod verification {
    use super::*;
    use crate::frame::MAX_MAX_FRAME_SIZE;

    /// Longest frame, head included, that any max frame size setting allows
    const MAX_FRAME_LEN: usize = MAX_MAX_FRAME_SIZE as usize + frame::HEADER_LEN;

    fn is_reachable(size: &ReadSize) -> bool {
        (INITIAL_READ_SIZE..=MAX_READ_SIZE).contains(&size.next)
            && size.avg_frame_len <= MAX_FRAME_LEN
    }

    /// Any `ReadSize` in a state its methods can reach from the default one.
    fn any_reachable() -> ReadSize {
        let size = ReadSize {
            next: kani::any(),
            decrease_now: kani::any(),
            avg_frame_len: kani::any(),
        };
        kani::assume(is_reachable(&size));
        size
    }

    // By induction, these four cover every sequence of calls.

    #[kani::proof]
    fn default_is_reachable() {
        assert!(is_reachable(&ReadSize::default()));
    }

    #[kani::proof]
    fn record_frame_stays_reachable() {
        let mut size = any_reachable();
        let before = size.avg_frame_len;
        // `split_frame` only records frames within the max frame size
        let len = kani::any();
        kani::assume((frame::HEADER_LEN..=MAX_FRAME_LEN).contains(&len));

        size.record_frame(len);

        assert!(is_reachable(&size));
        assert!(size.avg_frame_len <= before.max(len));
    }

    #[kani::proof]
    fn record_read_stays_reachable() {
        let mut size = any_reachable();
        // A read can't fill more than the room it was offered
        let (offered, n): (usize, usize) = kani::any();
        kani::assume(n <= offered);

        let before = size.get();
        size.record_read(offered, n);

        assert!(is_reachable(&size));
        kani::cover!(size.get() > before, "grows");
        kani::cover!(size.get() < before, "shrinks");
    }

    #[kani::proof]
    fn reset_stays_reachable() {
        let mut size = any_reachable();
        size.reset();
        assert!(is_reachable(&size));
    }

    #[kani::proof]
    fn read_size_is_in_bounds() {
        let size = any_reachable();
        assert!((INITIAL_READ_SIZE..=MAX_READ_SIZE).contains(&size.get()));
    }

    /// Given what `read_with` guarantees, the room a read needs is never 0,
    /// growing to the capacity makes that room, and the capacity is bounded.
    #[kani::proof]
    fn read_room_is_sound() {
        let len = kani::any();
        let frame_len: Option<usize> = kani::any();
        let read_size = kani::any();
        kani::assume((INITIAL_READ_SIZE..=MAX_READ_SIZE).contains(&read_size));
        match frame_len {
            Some(frame_len) => kani::assume(len < frame_len && frame_len <= MAX_FRAME_LEN),
            None => kani::assume(len < LENGTH_FIELD_LEN),
        }

        let (needed, capacity) = read_room(len, frame_len, read_size);
        kani::cover!(frame_len.is_none(), "at a frame boundary");
        kani::cover!(capacity > read_size, "a large frame");
        kani::cover!(
            frame_len.is_some() && capacity == read_size,
            "a small frame"
        );

        assert!(needed > 0);
        assert!(capacity >= len + needed);
        assert!(capacity >= INITIAL_READ_SIZE);
        assert!(capacity <= MAX_FRAME_LEN + MAX_READ_SIZE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::DEFAULT_MAX_FRAME_SIZE;
    use bytes::BufMut;

    const MAX_FRAME_SIZE: usize = DEFAULT_MAX_FRAME_SIZE as usize;

    /// Encodes frames with payloads of the given lengths.
    fn frames(payload_lens: &[usize]) -> Vec<u8> {
        let mut data = Vec::new();
        for &len in payload_lens {
            data.put_uint(len as u64, LENGTH_FIELD_LEN);
            data.put_bytes(0, frame::HEADER_LEN - LENGTH_FIELD_LEN + len);
        }
        data
    }

    /// Feeds `data` through `buf` in reads of at most `chunk` bytes, until a
    /// read is pending.
    ///
    /// Returns the number of frames split off and of reads.
    fn feed(buf: &mut FrameBuf, mut data: &[u8], chunk: usize) -> (usize, usize) {
        let (mut frames, mut reads) = (0, 0);
        let mut read = |buf: &mut BytesMut| {
            if data.is_empty() {
                return Poll::Pending;
            }
            let n = (buf.capacity() - buf.len()).min(chunk).min(data.len());
            buf.extend_from_slice(&data[..n]);
            data = &data[n..];
            reads += 1;
            Poll::Ready(Ok(n))
        };

        while let Poll::Ready(frame) = buf.poll_frame(MAX_FRAME_SIZE, &mut read) {
            frame.expect("stream ended").expect("split error");
            frames += 1;
        }
        (frames, reads)
    }

    #[test]
    fn large_frames_take_few_reads() {
        let mut buf = FrameBuf::default();
        let (frames, reads) = feed(&mut buf, &frames(&[16_384; 64]), usize::MAX);
        assert_eq!(frames, 64);
        // A fixed 8 KiB buffer takes two reads for each of these frames.
        // Allow at most one read per two frames, ramp-up included.
        assert!(reads <= 32, "{} reads for 64 frames", reads);
    }

    #[test]
    fn small_frames_keep_initial_read_size() {
        let mut buf = FrameBuf::default();
        assert_eq!(feed(&mut buf, &frames(&[4; 4096]), usize::MAX).0, 4096);
        assert_eq!(buf.read_size.get(), INITIAL_READ_SIZE);
        // Nothing grew, so the buffer is kept while idle.
        assert!((1..=INITIAL_READ_SIZE).contains(&buf.buf.capacity()));
    }

    #[test]
    fn grown_buffer_is_released_when_idle() {
        let mut buf = FrameBuf::default();
        assert_eq!(feed(&mut buf, &frames(&[16_384; 16]), usize::MAX).0, 16);
        assert_eq!(buf.buf.capacity(), 0);
        assert_eq!(buf.read_size.get(), INITIAL_READ_SIZE);
    }

    #[test]
    fn partial_frame_is_kept_when_idle() {
        let mut buf = FrameBuf::default();
        let data = frames(&[16_384; 2]);
        let partial = frame::HEADER_LEN + 16_384 + 100;
        assert_eq!(feed(&mut buf, &data[..partial], usize::MAX).0, 1);
        assert_eq!(buf.buf.len(), 100);
    }

    #[test]
    fn frame_split_across_small_reads() {
        let mut buf = FrameBuf::default();
        let data = frames(&[16_384, 4, 16_384]);
        assert_eq!(feed(&mut buf, &data, 1_000).0, 3);
    }

    #[test]
    fn frame_over_max_size_is_rejected() {
        let mut buf = FrameBuf::default();
        buf.buf.extend_from_slice(&[0, 64, 1]);
        assert!(buf.split_frame(MAX_FRAME_SIZE).is_err());
    }

    #[test]
    fn read_size_policy() {
        let mut size = ReadSize::default();
        for _ in 0..64 {
            size.record_frame(16_393);
        }

        // Doubles on full reads, up to the max
        let mut steps = vec![size.get()];
        for _ in 0..4 {
            let next = size.get();
            size.record_read(next, next);
            steps.push(size.get());
        }
        assert_eq!(steps, [8 << 10, 16 << 10, 32 << 10, 64 << 10, 64 << 10]);

        // A read limited by the space offered is ignored
        size.record_read(100, 50);
        assert_eq!(size.get(), 64 << 10);

        // Halves after two small reads in a row
        size.record_read(64 << 10, 1_000);
        assert_eq!(size.get(), 64 << 10);
        size.record_read(64 << 10, 1_000);
        assert_eq!(size.get(), 32 << 10);

        // A read in range cancels a pending decrease
        size.record_read(32 << 10, 1_000);
        size.record_read(32 << 10, 20 << 10);
        size.record_read(32 << 10, 1_000);
        assert_eq!(size.get(), 32 << 10);

        // Small frames cap the size again
        for _ in 0..64 {
            size.record_frame(13);
        }
        assert_eq!(size.get(), INITIAL_READ_SIZE);

        size.reset();
        assert_eq!(size.get(), INITIAL_READ_SIZE);
    }

    mod properties {
        use super::*;
        use bytes::BufMut;
        use proptest::collection::vec;
        use proptest::prelude::*;
        use std::ops::Range;

        #[derive(Clone, Copy, Debug)]
        enum Op {
            /// A read of at most this many bytes
            Read(usize),
            Pending,
        }

        fn read() -> impl Strategy<Value = Op> {
            prop_oneof![1usize..=16, 1usize..=4_096, 1usize..=100_000].prop_map(Op::Read)
        }

        /// Read plans, cycled while there is data left. Each starts with a
        /// read, so a plan always makes progress.
        fn ops() -> impl Strategy<Value = Vec<Op>> {
            (
                read(),
                vec(prop_oneof![4 => read(), 1 => Just(Op::Pending)], 0..16),
            )
                .prop_map(|(first, mut rest)| {
                    rest.insert(0, first);
                    rest
                })
        }

        /// Payload lengths: mostly small, sometimes up to `max`, and `max`
        /// itself.
        fn payload_lens(max: usize) -> impl Strategy<Value = Vec<usize>> {
            let len = prop_oneof![4 => 0..64usize, 3 => 0..4_096.min(max + 1), 1 => 0..=max, 1 => Just(max)];
            vec(len, 0..24)
        }

        /// Encodes frames with distinct contents, returning where each one is.
        fn encode(payload_lens: &[usize]) -> (Vec<u8>, Vec<Range<usize>>) {
            let mut data = Vec::new();
            let mut frames = Vec::new();
            for (i, &len) in payload_lens.iter().enumerate() {
                let start = data.len();
                data.put_uint(len as u64, LENGTH_FIELD_LEN);
                data.put_u8(i as u8);
                data.put_u8(0);
                data.put_u32(i as u32);
                data.extend((0..len).map(|j| (i * 31 + j) as u8));
                frames.push(start..data.len());
            }
            (data, frames)
        }

        /// Reads `data` through `buf`, performing the ops of the plan in turn,
        /// and checks the room offered to each read.
        ///
        /// Returns the frames split off, and the error that ended the stream,
        /// if any.
        fn drive(
            buf: &mut FrameBuf,
            mut data: &[u8],
            ops: &[Op],
            max_frame_size: usize,
        ) -> (Vec<BytesMut>, Result<(), Error>) {
            // Growth requests at most a frame and a read. The allocator may
            // round that up to twice as much.
            let capacity_bound = 2 * (max_frame_size + frame::HEADER_LEN + MAX_READ_SIZE);
            let mut ops = ops.iter().cycle();
            let mut read = |buf: &mut BytesMut| {
                let spare = buf.capacity() - buf.len();
                assert!(spare > 0, "a read must have room for a byte");
                assert!(
                    buf.capacity() <= capacity_bound,
                    "capacity {}",
                    buf.capacity()
                );
                // At a frame boundary there is room for a full read, and
                // otherwise for the frame at the front, going by its length
                // field.
                if buf.len() < LENGTH_FIELD_LEN {
                    assert!(
                        buf.capacity() >= INITIAL_READ_SIZE,
                        "{} bytes at a boundary",
                        buf.capacity()
                    );
                } else if let Some(&[a, b, c]) = buf.get(..LENGTH_FIELD_LEN) {
                    let frame_len = u32::from_be_bytes([0, a, b, c]) as usize + frame::HEADER_LEN;
                    assert!(
                        buf.capacity() >= frame_len,
                        "{} bytes for a {} byte frame",
                        buf.capacity(),
                        frame_len
                    );
                }

                match ops.next() {
                    Some(Op::Pending) => Poll::Pending,
                    Some(&Op::Read(max)) => {
                        let n = spare.min(max).min(data.len());
                        buf.extend_from_slice(&data[..n]);
                        data = &data[n..];
                        Poll::Ready(Ok(n))
                    }
                    None => unreachable!("plans are never empty"),
                }
            };

            let mut frames = Vec::new();
            loop {
                match buf.poll_frame(max_frame_size, &mut read) {
                    Poll::Ready(Some(Ok(frame))) => frames.push(frame),
                    Poll::Ready(Some(Err(e))) => return (frames, Err(e)),
                    Poll::Ready(None) => return (frames, Ok(())),
                    Poll::Pending => {}
                }
            }
        }

        proptest! {
            /// However the stream is cut into reads, the frames come out
            /// whole, in order and unchanged, even while earlier frames
            /// still share the buffer.
            #[test]
            fn frames_survive_any_read_pattern(
                (max_frame_size, lens) in prop_oneof![Just(MAX_FRAME_SIZE), 16_384usize..=200_000]
                    .prop_flat_map(|max| (Just(max), payload_lens(max))),
                ops in ops(),
            ) {
                let (data, expected) = encode(&lens);
                let mut buf = FrameBuf::default();

                let (frames, res) = drive(&mut buf, &data, &ops, max_frame_size);

                prop_assert!(res.is_ok(), "{:?}", res);
                prop_assert_eq!(frames.len(), expected.len());
                for (frame, range) in frames.iter().zip(expected) {
                    prop_assert_eq!(&frame[..], &data[range]);
                }
            }

            /// EOF is an error exactly when the stream ended inside a frame.
            #[test]
            fn truncated_stream_fails_inside_a_frame(
                lens in payload_lens(MAX_FRAME_SIZE),
                cut in any::<prop::sample::Index>(),
                ops in ops(),
            ) {
                let (data, expected) = encode(&lens);
                let cut = cut.index(data.len() + 1);
                let mut buf = FrameBuf::default();

                let (frames, res) = drive(&mut buf, &data[..cut], &ops, MAX_FRAME_SIZE);

                let complete = expected.iter().filter(|range| range.end <= cut).count();
                prop_assert_eq!(frames.len(), complete);
                let at_boundary = cut == 0 || expected.iter().any(|range| range.end == cut);
                prop_assert_eq!(res.is_ok(), at_boundary, "{:?}", res);
            }

            /// A frame over the max size is rejected once its length field is
            /// buffered, and the frames before it come out intact.
            #[test]
            fn oversized_frame_is_rejected_in_place(
                lens in payload_lens(1_000),
                excess in 1usize..=100_000,
                ops in ops(),
            ) {
                let max_frame_size = 1_000;
                let lens: Vec<usize> = lens.into_iter().map(|len| len.min(max_frame_size)).collect();
                let (mut data, expected) = encode(&lens);
                data.put_uint((max_frame_size + excess) as u64, LENGTH_FIELD_LEN);
                let mut buf = FrameBuf::default();

                let (frames, res) = drive(&mut buf, &data, &ops, max_frame_size);

                prop_assert!(
                    matches!(res, Err(Error::GoAway(_, Reason::FRAME_SIZE_ERROR, _))),
                    "{:?}",
                    res
                );
                prop_assert_eq!(frames.len(), expected.len());
                for (frame, range) in frames.iter().zip(expected) {
                    prop_assert_eq!(&frame[..], &data[range]);
                }
            }

            /// The read size stays within its bounds, and the frame average
            /// never exceeds the largest frame seen.
            #[test]
            fn read_size_stays_in_bounds(
                events in vec(prop_oneof![
                    (frame::HEADER_LEN..=frame::HEADER_LEN + (1 << 24)).prop_map(Ok),
                    (1usize..=1 << 20, any::<prop::sample::Index>())
                        .prop_map(|(offered, n)| Err(Some((offered, n.index(offered + 1))))),
                    Just(Err(None)),
                ], 0..200),
            ) {
                let mut size = ReadSize::default();
                let mut largest_frame = 0;
                for event in events {
                    match event {
                        Ok(len) => {
                            largest_frame = largest_frame.max(len);
                            size.record_frame(len);
                        }
                        Err(Some((offered, n))) => size.record_read(offered, n),
                        Err(None) => size.reset(),
                    }
                    prop_assert!((INITIAL_READ_SIZE..=MAX_READ_SIZE).contains(&size.get()));
                    prop_assert!((INITIAL_READ_SIZE..=MAX_READ_SIZE).contains(&size.next));
                    prop_assert!(size.avg_frame_len <= largest_frame);
                }
            }
        }
    }
}

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
/// [`FrameBuf::read_with`] takes the read to perform.
#[derive(Debug)]
pub(super) struct FrameBuf {
    buf: BytesMut,

    /// Total length (head and payload) of the frame at the front of `buf`,
    /// set once its length field has been checked against the max frame size
    frame_len: Option<usize>,

    read_size: ReadSize,
}

impl FrameBuf {
    pub(super) fn new() -> Self {
        FrameBuf {
            buf: BytesMut::with_capacity(INITIAL_READ_SIZE),
            frame_len: None,
            read_size: ReadSize::default(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Splits the frame at the front off, head included, if it is complete.
    #[inline]
    pub(super) fn split_frame(&mut self, max_frame_size: usize) -> Result<Option<BytesMut>, Error> {
        let frame_len = match self.frame_len {
            Some(frame_len) => frame_len,
            None => {
                let (a, b, c) = match self.buf.get(..LENGTH_FIELD_LEN) {
                    Some(&[a, b, c]) => (a, b, c),
                    _ => return Ok(None),
                };

                let payload_len = u32::from_be_bytes([0, a, b, c]) as usize;
                if payload_len > max_frame_size {
                    return Err(frame_too_large(payload_len, max_frame_size));
                }

                *self.frame_len.insert(payload_len + frame::HEADER_LEN)
            }
        };

        if self.buf.len() < frame_len {
            return Ok(None);
        }

        self.frame_len = None;
        self.read_size.record_frame(frame_len);
        Ok(Some(self.buf.split_to(frame_len)))
    }

    /// Makes room for a read, lets `read` append to the buffer, and adapts
    /// the next read size to how much it read.
    ///
    /// When `read` is pending with nothing buffered, a buffer that grew past
    /// its initial size is freed, so idle connections don't keep it.
    #[inline]
    pub(super) fn read_with(
        &mut self,
        read: impl FnOnce(&mut BytesMut) -> Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        let offered = self.reserve();
        let res = read(&mut self.buf);
        match res {
            Poll::Ready(Ok(n)) => self.read_size.record_read(offered, n),
            Poll::Ready(Err(_)) => {}
            Poll::Pending => self.release_if_idle(),
        }
        res
    }

    /// Makes room in the buffer for the next read and returns how much there
    /// is.
    ///
    /// Growing copies the bytes already buffered into a new allocation
    /// whenever frames split off earlier still share the current one. So the
    /// buffer only grows at a frame boundary, where at most a partial length
    /// field is buffered, or when the frame being read does not fit.
    /// Otherwise a short read finishes the frame in place.
    fn reserve(&mut self) -> usize {
        let len = self.buf.len();
        let read_size = self.read_size.get();

        // The room the next read needs, and the capacity to grow to if the
        // buffer has less.
        let (needed, capacity) = match self.frame_len {
            // A frame that is large next to a read gets a full read after it,
            // so the frames that follow land in the same allocation.
            Some(frame_len) if frame_len > read_size / 2 => {
                (frame_len - len, frame_len + read_size)
            }
            Some(frame_len) => (frame_len - len, read_size),
            // `len` is below `LENGTH_FIELD_LEN`, so below `read_size`
            None => (read_size - len, read_size),
        };

        if self.spare() < needed {
            self.buf.reserve(capacity - len);
        }

        // `needed` is never 0, so there is always room for a byte and a read
        // of 0 bytes means EOF.
        debug_assert!(self.spare() > 0);
        self.spare()
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

#[cold]
fn frame_too_large(payload_len: usize, max_frame_size: usize) -> Error {
    proto_err!(conn: "frame size {} over max {}", payload_len, max_frame_size);
    Error::library_go_away(Reason::FRAME_SIZE_ERROR)
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

    /// Feeds `data` through `buf` in reads of at most `chunk` bytes, the way
    /// `FramedRead` does, until a read is pending.
    ///
    /// Returns the number of frames split off and of reads.
    fn feed(buf: &mut FrameBuf, mut data: &[u8], chunk: usize) -> (usize, usize) {
        let (mut frames, mut reads) = (0, 0);
        loop {
            while buf.split_frame(MAX_FRAME_SIZE).unwrap().is_some() {
                frames += 1;
            }

            let res = buf.read_with(|buf| {
                if data.is_empty() {
                    return Poll::Pending;
                }
                let n = (buf.capacity() - buf.len()).min(chunk).min(data.len());
                buf.extend_from_slice(&data[..n]);
                data = &data[n..];
                Poll::Ready(Ok(n))
            });

            if res.is_pending() {
                return (frames, reads);
            }
            reads += 1;
        }
    }

    #[test]
    fn large_frames_take_few_reads() {
        let mut buf = FrameBuf::new();
        let (frames, reads) = feed(&mut buf, &frames(&[16_384; 64]), usize::MAX);
        assert_eq!(frames, 64);
        // A fixed 8 KiB buffer takes two reads for each of these frames.
        // Allow at most one read per two frames, ramp-up included.
        assert!(reads <= 32, "{} reads for 64 frames", reads);
    }

    #[test]
    fn small_frames_keep_initial_read_size() {
        let mut buf = FrameBuf::new();
        assert_eq!(feed(&mut buf, &frames(&[4; 4096]), usize::MAX).0, 4096);
        assert_eq!(buf.read_size.get(), INITIAL_READ_SIZE);
        // Nothing grew, so the buffer is kept while idle.
        assert!((1..=INITIAL_READ_SIZE).contains(&buf.buf.capacity()));
    }

    #[test]
    fn grown_buffer_is_released_when_idle() {
        let mut buf = FrameBuf::new();
        assert_eq!(feed(&mut buf, &frames(&[16_384; 16]), usize::MAX).0, 16);
        assert_eq!(buf.buf.capacity(), 0);
        assert_eq!(buf.read_size.get(), INITIAL_READ_SIZE);
    }

    #[test]
    fn partial_frame_is_kept_when_idle() {
        let mut buf = FrameBuf::new();
        let data = frames(&[16_384; 2]);
        let partial = frame::HEADER_LEN + 16_384 + 100;
        assert_eq!(feed(&mut buf, &data[..partial], usize::MAX).0, 1);
        assert_eq!(buf.buf.len(), 100);
    }

    #[test]
    fn frame_split_across_small_reads() {
        let mut buf = FrameBuf::new();
        let data = frames(&[16_384, 4, 16_384]);
        assert_eq!(feed(&mut buf, &data, 1_000).0, 3);
    }

    #[test]
    fn frame_over_max_size_is_rejected() {
        let mut buf = FrameBuf::new();
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
}

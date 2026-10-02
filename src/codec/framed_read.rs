use crate::frame::{self, Frame, Kind, Reason};
use crate::frame::{
    DEFAULT_MAX_FRAME_SIZE, DEFAULT_SETTINGS_HEADER_TABLE_SIZE, MAX_MAX_FRAME_SIZE,
};
use crate::proto::Error;

use crate::hpack;

use futures_core::Stream;

use bytes::{Buf, BytesMut};

use std::future::Future;
use std::io;

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt};

// 16 MB "sane default" taken from golang http2
const DEFAULT_SETTINGS_MAX_HEADER_LIST_SIZE: usize = 16 << 20;

/// Initial capacity of the read buffer, and the smallest read size.
const INITIAL_READ_CAPACITY: usize = 8 * 1024;

/// Largest read size the adaptive strategy grows to.
const MAX_READ_SIZE: usize = 64 * 1024;

/// Length of the payload length field at the start of the frame header.
const LENGTH_FIELD_LEN: usize = 3;

#[derive(Debug)]
pub struct FramedRead<T> {
    inner: T,

    /// Bytes read from `inner` that have not been split into frames yet
    buf: BytesMut,

    /// Total length (head and payload) of the frame at the front of `buf`,
    /// set once its length field has been checked against the max frame size
    frame_len: Option<usize>,

    /// An error was returned, so the next poll returns `None`
    has_errored: bool,

    /// Adapts how much buffer space is offered to each read
    read_size: ReadSize,

    max_frame_size: usize,

    decoder: FrameDecoder,
}

#[derive(Debug)]
struct FrameDecoder {
    // hpack decoder state
    hpack: hpack::Decoder,

    max_header_list_size: usize,

    max_continuation_frames: usize,

    partial: Option<Partial>,
}

/// Partially loaded headers frame
#[derive(Debug)]
struct Partial {
    /// Empty frame
    frame: Continuable,

    /// Partial header payload
    buf: BytesMut,

    continuation_frames_count: usize,
}

#[derive(Debug)]
enum Continuable {
    Headers(frame::Headers),
    PushPromise(frame::PushPromise),
}

impl<T> FramedRead<T> {
    pub fn new(inner: T) -> FramedRead<T> {
        let max_frame_size = DEFAULT_MAX_FRAME_SIZE as usize;
        FramedRead {
            inner,
            buf: BytesMut::with_capacity(INITIAL_READ_CAPACITY),
            frame_len: None,
            has_errored: false,
            read_size: ReadSize::new(),
            max_frame_size,
            decoder: FrameDecoder::new(max_frame_size),
        }
    }

    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Returns the current max frame size setting
    #[inline]
    pub fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }

    /// Updates the max frame size setting.
    ///
    /// Must be within 16,384 and 16,777,215.
    #[inline]
    pub fn set_max_frame_size(&mut self, val: usize) {
        assert!(DEFAULT_MAX_FRAME_SIZE as usize <= val && val <= MAX_MAX_FRAME_SIZE as usize);
        self.max_frame_size = val;
        // Update max CONTINUATION frames too, since its based on this
        self.decoder.set_max_frame_size(val);
    }

    /// Update the max header list size setting.
    #[inline]
    pub fn set_max_header_list_size(&mut self, val: usize) {
        self.decoder
            .set_max_header_list_size(val, self.max_frame_size());
    }

    /// Update the header table size setting.
    #[inline]
    pub fn set_header_table_size(&mut self, val: usize) {
        self.decoder.set_header_table_size(val);
    }
}

fn calc_max_continuation_frames(header_max: usize, frame_max: usize) -> usize {
    // At least this many frames needed to use max header list size
    let min_frames_for_list = (header_max / frame_max).max(1);
    // Some padding for imperfectly packed frames
    // 25% without floats
    let padding = min_frames_for_list >> 2;
    min_frames_for_list.saturating_add(padding).max(5)
}

impl FrameDecoder {
    fn new(max_frame_size: usize) -> Self {
        let max_header_list_size = DEFAULT_SETTINGS_MAX_HEADER_LIST_SIZE;
        FrameDecoder {
            hpack: hpack::Decoder::new(DEFAULT_SETTINGS_HEADER_TABLE_SIZE),
            max_header_list_size,
            max_continuation_frames: calc_max_continuation_frames(
                max_header_list_size,
                max_frame_size,
            ),
            partial: None,
        }
    }

    fn set_max_frame_size(&mut self, val: usize) {
        self.max_continuation_frames = calc_max_continuation_frames(self.max_header_list_size, val);
    }

    fn set_max_header_list_size(&mut self, val: usize, max_frame_size: usize) {
        self.max_header_list_size = val;
        // Update max CONTINUATION frames too, since its based on this
        self.max_continuation_frames = calc_max_continuation_frames(val, max_frame_size);
    }

    fn set_header_table_size(&mut self, val: usize) {
        self.hpack.queue_size_update(val);
    }

    fn decode(&mut self, bytes: BytesMut) -> Result<Option<Frame>, Error> {
        decode_frame(self, bytes)
    }
}

/// Decodes a frame.
///
/// This function is intentionally de-generified and outlined because it is very large.
fn decode_frame(decoder: &mut FrameDecoder, mut bytes: BytesMut) -> Result<Option<Frame>, Error> {
    let span = tracing::trace_span!("FramedRead::decode_frame", offset = bytes.len());
    let _e = span.enter();

    tracing::trace!("decoding frame from {}B", bytes.len());

    // Parse the head
    let head = frame::Head::parse(&bytes);

    if decoder.partial.is_some() && head.kind() != Kind::Continuation {
        proto_err!(conn: "expected CONTINUATION, got {:?}", head.kind());
        return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
    }

    let kind = head.kind();

    tracing::trace!(frame.kind = ?kind);

    macro_rules! header_block {
        ($frame:ident, $head:ident, $bytes:ident) => ({
            // Drop the frame header
            $bytes.advance(frame::HEADER_LEN);

            // Parse the header frame w/o parsing the payload
            let (mut frame, mut payload) = match frame::$frame::load($head, $bytes) {
                Ok(res) => res,
                Err(frame::Error::InvalidDependencyId) => {
                    proto_err!(stream: "invalid HEADERS dependency ID");
                    // A stream cannot depend on itself. An endpoint MUST
                    // treat this as a stream error (Section 5.4.2) of type
                    // `PROTOCOL_ERROR`.
                    return Err(Error::library_reset($head.stream_id(), Reason::PROTOCOL_ERROR));
                },
                Err(e) => {
                    proto_err!(conn: "failed to load frame; err={:?}", e);
                    return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
                }
            };

            let is_end_headers = frame.is_end_headers();

            // Load the HPACK encoded headers
            match frame.load_hpack(&mut payload, decoder.max_header_list_size, &mut decoder.hpack) {
                Ok(_) => {},
                Err(frame::Error::Hpack(hpack::DecoderError::NeedMore(_))) if !is_end_headers => {},
                Err(frame::Error::MalformedMessage) => {
                    let id = $head.stream_id();
                    proto_err!(stream: "malformed header block; stream={:?}", id);
                    return Err(Error::library_reset(id, Reason::PROTOCOL_ERROR));
                },
                Err(frame::Error::HeaderListWayTooLarge) => {
                    proto_err!(conn: "decoded header list size over abuse limit");
                    return Err(Error::library_go_away_data(
                        Reason::ENHANCE_YOUR_CALM,
                        "header_list_way_too_large",
                    ));
                },
                Err(e) => {
                    proto_err!(conn: "failed HPACK decoding; err={:?}", e);
                    return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
                }
            }

            if is_end_headers {
                frame.into()
            } else {
                tracing::trace!("loaded partial header block");
                // Defer returning the frame
                decoder.partial = Some(Partial {
                    frame: Continuable::$frame(frame),
                    buf: payload,
                    continuation_frames_count: 0,
                });

                return Ok(None);
            }
        });
    }

    let frame = match kind {
        Kind::Settings => {
            let res = frame::Settings::load(head, &bytes[frame::HEADER_LEN..]);

            res.map_err(|e| {
                proto_err!(conn: "failed to load SETTINGS frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::Ping => {
            let res = frame::Ping::load(head, &bytes[frame::HEADER_LEN..]);

            res.map_err(|e| {
                proto_err!(conn: "failed to load PING frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::WindowUpdate => {
            let res = frame::WindowUpdate::load(head, &bytes[frame::HEADER_LEN..]);

            res.map_err(|e| {
                proto_err!(conn: "failed to load WINDOW_UPDATE frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::Data => {
            bytes.advance(frame::HEADER_LEN);
            let res = frame::Data::load(head, bytes.freeze());

            // TODO: Should this always be connection level? Probably not...
            res.map_err(|e| {
                proto_err!(conn: "failed to load DATA frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::Headers => header_block!(Headers, head, bytes),
        Kind::Reset => {
            let res = frame::Reset::load(head, &bytes[frame::HEADER_LEN..]);
            res.map_err(|e| {
                proto_err!(conn: "failed to load RESET frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::GoAway => {
            let res = frame::GoAway::load(head, &bytes[frame::HEADER_LEN..]);
            res.map_err(|e| {
                proto_err!(conn: "failed to load GO_AWAY frame; err={:?}", e);
                Error::library_go_away(Reason::PROTOCOL_ERROR)
            })?
            .into()
        }
        Kind::PushPromise => header_block!(PushPromise, head, bytes),
        Kind::Priority => {
            if head.stream_id() == 0 {
                // Invalid stream identifier
                proto_err!(conn: "invalid stream ID 0");
                return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
            }

            match frame::Priority::load(head, &bytes[frame::HEADER_LEN..]) {
                Ok(frame) => frame.into(),
                Err(frame::Error::InvalidDependencyId) => {
                    // A stream cannot depend on itself. An endpoint MUST
                    // treat this as a stream error (Section 5.4.2) of type
                    // `PROTOCOL_ERROR`.
                    let id = head.stream_id();
                    proto_err!(stream: "PRIORITY invalid dependency ID; stream={:?}", id);
                    return Err(Error::library_reset(id, Reason::PROTOCOL_ERROR));
                }
                Err(e) => {
                    proto_err!(conn: "failed to load PRIORITY frame; err={:?};", e);
                    return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
                }
            }
        }
        Kind::Continuation => {
            let is_end_headers = (head.flag() & 0x4) == 0x4;

            let mut partial = match decoder.partial.take() {
                Some(partial) => partial,
                None => {
                    proto_err!(conn: "received unexpected CONTINUATION frame");
                    return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
                }
            };

            // The stream identifiers must match
            if partial.frame.stream_id() != head.stream_id() {
                proto_err!(conn: "CONTINUATION frame stream ID does not match previous frame stream ID");
                return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
            }

            // Check for CONTINUATION flood
            if is_end_headers {
                partial.continuation_frames_count = 0;
            } else {
                let cnt = partial.continuation_frames_count + 1;
                if cnt > decoder.max_continuation_frames {
                    tracing::debug!(
                        "too_many_continuations, max = {}",
                        decoder.max_continuation_frames
                    );
                    return Err(Error::library_go_away_data(
                        Reason::ENHANCE_YOUR_CALM,
                        "too_many_continuations",
                    ));
                } else {
                    partial.continuation_frames_count = cnt;
                }
            }

            // Extend the buf
            if partial.buf.is_empty() {
                partial.buf = bytes.split_off(frame::HEADER_LEN);
            } else {
                if partial.frame.is_over_size() {
                    // If there was left over bytes previously, they may be
                    // needed to continue decoding, even though we will
                    // be ignoring this frame. This is done to keep the HPACK
                    // decoder state up-to-date.
                    //
                    // Still, we need to be careful, because if a malicious
                    // attacker were to try to send a gigantic string, such
                    // that it fits over multiple header blocks, we could
                    // grow memory uncontrollably again, and that'd be a shame.
                    //
                    // Instead, we use a simple heuristic to determine if
                    // we should continue to ignore decoding, or to tell
                    // the attacker to go away.
                    if partial.buf.len() + bytes.len() > decoder.max_header_list_size {
                        proto_err!(conn: "CONTINUATION frame header block size over ignorable limit");
                        return Err(Error::library_go_away(Reason::COMPRESSION_ERROR));
                    }
                }
                partial.buf.extend_from_slice(&bytes[frame::HEADER_LEN..]);
            }

            match partial.frame.load_hpack(
                &mut partial.buf,
                decoder.max_header_list_size,
                &mut decoder.hpack,
            ) {
                Ok(_) => {}
                Err(frame::Error::Hpack(hpack::DecoderError::NeedMore(_))) if !is_end_headers => {}
                Err(frame::Error::MalformedMessage) => {
                    let id = head.stream_id();
                    proto_err!(stream: "malformed CONTINUATION frame; stream={:?}", id);
                    return Err(Error::library_reset(id, Reason::PROTOCOL_ERROR));
                }
                Err(frame::Error::HeaderListWayTooLarge) => {
                    proto_err!(conn: "decoded CONTINUATION header list size over abuse limit");
                    return Err(Error::library_go_away_data(
                        Reason::ENHANCE_YOUR_CALM,
                        "header_list_way_too_large",
                    ));
                }
                Err(e) => {
                    proto_err!(conn: "failed HPACK decoding; err={:?}", e);
                    return Err(Error::library_go_away(Reason::PROTOCOL_ERROR));
                }
            }

            if is_end_headers {
                partial.frame.into()
            } else {
                decoder.partial = Some(partial);
                return Ok(None);
            }
        }
        Kind::Unknown => {
            // Unknown frames are ignored
            return Ok(None);
        }
    };

    Ok(Some(frame))
}

impl<T> FramedRead<T>
where
    T: AsyncRead + Unpin,
{
    /// Reads from `inner` until a complete frame, head included, is buffered
    /// and splits it off.
    ///
    /// After returning an error, the next call returns `None`. At EOF, a
    /// partial frame left in the buffer is an error.
    fn poll_next_frame(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<BytesMut, Error>>> {
        if self.has_errored {
            self.has_errored = false;
            return Poll::Ready(None);
        }

        let res = self.poll_next_frame_inner(cx);
        if let Poll::Ready(Some(Err(_))) = res {
            self.has_errored = true;
        }
        res
    }

    fn poll_next_frame_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<BytesMut, Error>>> {
        loop {
            if let Some(bytes) = self.split_frame()? {
                return Poll::Ready(Some(Ok(bytes)));
            }

            let offered = self.reserve_for_read();
            let n = match poll_read_buf(&mut self.inner, cx, &mut self.buf) {
                Poll::Ready(res) => res?,
                Poll::Pending => {
                    self.release_if_idle();
                    return Poll::Pending;
                }
            };
            self.read_size.record(offered, n);
            if n == 0 {
                return if self.buf.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "bytes remaining on stream",
                    )
                    .into())))
                };
            }
        }
    }

    /// Splits the frame at the front of the buffer off, if it is complete.
    fn split_frame(&mut self) -> Result<Option<BytesMut>, Error> {
        let frame_len = match self.frame_len {
            Some(frame_len) => frame_len,
            None => {
                if self.buf.len() < LENGTH_FIELD_LEN {
                    return Ok(None);
                }

                let payload_len =
                    u32::from_be_bytes([0, self.buf[0], self.buf[1], self.buf[2]]) as usize;
                if payload_len > self.max_frame_size {
                    proto_err!(conn: "frame size {} over max {}", payload_len, self.max_frame_size);
                    return Err(Error::library_go_away(Reason::FRAME_SIZE_ERROR));
                }

                let frame_len = payload_len + frame::HEADER_LEN;
                self.frame_len = Some(frame_len);
                frame_len
            }
        };

        if self.buf.len() < frame_len {
            return Ok(None);
        }

        self.frame_len = None;
        self.read_size.frame(frame_len);
        Ok(Some(self.buf.split_to(frame_len)))
    }

    /// Frees a read buffer that grew past its initial size once nothing is
    /// buffered, so idle connections don't keep it.
    fn release_if_idle(&mut self) {
        if self.buf.is_empty() && self.buf.capacity() > INITIAL_READ_CAPACITY {
            self.buf = BytesMut::new();
            self.read_size.reset();
        }
    }

    /// Makes room in `buf` for the next read and returns how much there is.
    ///
    /// Growing copies the bytes already buffered into a new allocation
    /// whenever frames split off earlier still share the current one. So the
    /// buffer only grows at a frame boundary, where at most a partial length
    /// field is buffered, or when the frame being read does not fit.
    /// Otherwise a short read finishes the frame in place.
    fn reserve_for_read(&mut self) -> usize {
        let len = self.buf.len();
        let spare = self.buf.capacity() - len;
        let read_size = self.read_size.get();

        let (needed, total) = match self.frame_len {
            // A frame that is large next to a read gets a full read after it,
            // so the frames that follow land in the same allocation.
            Some(frame_len) if frame_len > read_size / 2 => {
                (frame_len - len, frame_len + read_size)
            }
            Some(frame_len) => (frame_len - len, read_size),
            // `len` is below `LENGTH_FIELD_LEN`, so below `read_size`
            None => (read_size - len, read_size),
        };

        if spare < needed {
            self.buf.reserve(total - len);
        }

        // `needed` is never 0, so there is always room for a byte and a read
        // of 0 bytes means EOF.
        debug_assert!(self.buf.capacity() > len);
        self.buf.capacity() - len
    }
}

impl<T> Stream for FramedRead<T>
where
    T: AsyncRead + Unpin,
{
    type Item = Result<Frame, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let span = tracing::trace_span!("FramedRead::poll_next");
        let _e = span.enter();
        loop {
            tracing::trace!("poll");
            let bytes = match ready!(self.poll_next_frame(cx)) {
                Some(res) => res?,
                None => return Poll::Ready(None),
            };

            tracing::trace!(read.bytes = bytes.len());
            if let Some(frame) = self.decoder.decode(bytes)? {
                tracing::debug!(?frame, "received");
                return Poll::Ready(Some(Ok(frame)));
            }
        }
    }
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

impl ReadSize {
    fn new() -> Self {
        ReadSize {
            next: INITIAL_READ_CAPACITY,
            decrease_now: false,
            avg_frame_len: 0,
        }
    }

    fn get(&self) -> usize {
        self.next.min(self.max())
    }

    /// Starts over from the smallest read size, keeping the frame average.
    fn reset(&mut self) {
        self.next = INITIAL_READ_CAPACITY;
        self.decrease_now = false;
    }

    fn max(&self) -> usize {
        (self.avg_frame_len * 4).clamp(INITIAL_READ_CAPACITY, MAX_READ_SIZE)
    }

    /// Records a frame of `len` bytes, weighting it 1/8 in the average.
    fn frame(&mut self, len: usize) {
        self.avg_frame_len = self.avg_frame_len - self.avg_frame_len / 8 + len / 8;
    }

    /// Records a read of `n` bytes into `offered` bytes of spare capacity.
    fn record(&mut self, offered: usize, n: usize) {
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
                self.next = (size / 2).max(INITIAL_READ_CAPACITY);
                self.decrease_now = false;
            } else {
                self.decrease_now = true;
            }
        } else {
            self.decrease_now = false;
        }
    }
}

/// Reads from `io` into the spare capacity of `buf`.
fn poll_read_buf<T: AsyncRead + Unpin>(
    io: &mut T,
    cx: &mut Context<'_>,
    buf: &mut BytesMut,
) -> Poll<io::Result<usize>> {
    // `read_buf` is cancel safe, so a new future can be polled on every call
    // and dropped when it returns `Pending`.
    std::pin::pin!(io.read_buf(buf)).poll(cx)
}

// ===== impl Continuable =====

impl Continuable {
    fn stream_id(&self) -> frame::StreamId {
        match *self {
            Continuable::Headers(ref h) => h.stream_id(),
            Continuable::PushPromise(ref p) => p.stream_id(),
        }
    }

    fn is_over_size(&self) -> bool {
        match *self {
            Continuable::Headers(ref h) => h.is_over_size(),
            Continuable::PushPromise(ref p) => p.is_over_size(),
        }
    }

    fn load_hpack(
        &mut self,
        src: &mut BytesMut,
        max_header_list_size: usize,
        decoder: &mut hpack::Decoder,
    ) -> Result<(), frame::Error> {
        match *self {
            Continuable::Headers(ref mut h) => h.load_hpack(src, max_header_list_size, decoder),
            Continuable::PushPromise(ref mut p) => p.load_hpack(src, max_header_list_size, decoder),
        }
    }
}

impl<T> From<Continuable> for Frame<T> {
    fn from(cont: Continuable) -> Self {
        match cont {
            Continuable::Headers(mut headers) => {
                headers.set_end_headers();
                headers.into()
            }
            Continuable::PushPromise(mut push) => {
                push.set_end_headers();
                push.into()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use std::sync::Arc;
    use std::task::Wake;
    use tokio::io::ReadBuf;

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    /// Serves `data` in reads of at most `chunk` bytes, then is pending.
    struct Reader {
        data: Vec<u8>,
        pos: usize,
        chunk: usize,
        reads: usize,
    }

    impl AsyncRead for Reader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.pos == self.data.len() {
                return Poll::Pending;
            }
            let n = buf
                .remaining()
                .min(self.chunk)
                .min(self.data.len() - self.pos);
            let pos = self.pos;
            buf.put_slice(&self.data[pos..pos + n]);
            self.pos += n;
            self.reads += 1;
            Poll::Ready(Ok(()))
        }
    }

    fn framed(frames: &[(u8, usize)], chunk: usize) -> FramedRead<Reader> {
        let mut data = Vec::new();
        for &(kind, len) in frames {
            data.put_uint(len as u64, 3);
            data.put_u8(kind);
            data.put_u8(0);
            data.put_u32(if kind == 0 { 1 } else { 0 });
            if kind == 8 {
                // A WINDOW_UPDATE increment of 0 is a protocol error
                data.put_u32(1);
            } else {
                data.put_bytes(0, len);
            }
        }
        FramedRead::new(Reader {
            data,
            pos: 0,
            chunk,
            reads: 0,
        })
    }

    /// Polls until the reader is pending, returning the number of frames.
    fn read_all(framed: &mut FramedRead<Reader>) -> usize {
        let waker = Arc::new(NoopWake).into();
        let mut cx = Context::from_waker(&waker);
        let mut frames = 0;
        while let Poll::Ready(frame) = Pin::new(&mut *framed).poll_next(&mut cx) {
            frame.expect("stream ended").expect("decode error");
            frames += 1;
        }
        frames
    }

    const DATA_16K: (u8, usize) = (0, 16_384);
    const WINDOW_UPDATE: (u8, usize) = (8, 4);

    #[test]
    fn large_frames_take_few_reads() {
        let mut framed = framed(&[DATA_16K; 64], usize::MAX);
        assert_eq!(read_all(&mut framed), 64);
        // A fixed 8 KiB buffer takes two reads for each of these frames.
        // Allow at most one read per two frames, ramp-up included.
        assert!(
            framed.inner.reads <= 32,
            "{} reads for 64 frames",
            framed.inner.reads
        );
    }

    #[test]
    fn small_frames_keep_initial_read_size() {
        let mut framed = framed(&[WINDOW_UPDATE; 4096], usize::MAX);
        assert_eq!(read_all(&mut framed), 4096);
        assert_eq!(framed.read_size.get(), INITIAL_READ_CAPACITY);
        // Nothing grew, so the buffer is kept while idle.
        assert!(framed.buf.capacity() > 0);
        assert!(framed.buf.capacity() <= INITIAL_READ_CAPACITY);
    }

    #[test]
    fn grown_buffer_is_released_when_idle() {
        let mut framed = framed(&[DATA_16K; 16], usize::MAX);
        assert_eq!(read_all(&mut framed), 16);
        assert_eq!(framed.buf.capacity(), 0);
        assert_eq!(framed.read_size.get(), INITIAL_READ_CAPACITY);
    }

    #[test]
    fn partial_frame_is_kept_when_idle() {
        let mut framed = framed(&[DATA_16K; 2], usize::MAX);
        framed.inner.data.truncate(16_393 + 100);
        assert_eq!(read_all(&mut framed), 1);
        assert_eq!(framed.buf.len(), 100);
    }

    #[test]
    fn frame_split_across_small_reads() {
        let mut framed = framed(&[DATA_16K, WINDOW_UPDATE, DATA_16K], 1_000);
        assert_eq!(read_all(&mut framed), 3);
    }

    #[test]
    fn read_size_policy() {
        let mut size = ReadSize::new();
        for _ in 0..64 {
            size.frame(16_393);
        }

        // Doubles on full reads, up to the max
        let mut steps = vec![size.get()];
        for _ in 0..4 {
            let next = size.get();
            size.record(next, next);
            steps.push(size.get());
        }
        assert_eq!(steps, [8 << 10, 16 << 10, 32 << 10, 64 << 10, 64 << 10]);

        // A read limited by the space offered is ignored
        size.record(100, 50);
        assert_eq!(size.get(), 64 << 10);

        // Halves after two small reads in a row
        size.record(64 << 10, 1_000);
        assert_eq!(size.get(), 64 << 10);
        size.record(64 << 10, 1_000);
        assert_eq!(size.get(), 32 << 10);

        // A read in range cancels a pending decrease
        size.record(32 << 10, 1_000);
        size.record(32 << 10, 20 << 10);
        size.record(32 << 10, 1_000);
        assert_eq!(size.get(), 32 << 10);

        // Small frames cap the size again
        for _ in 0..64 {
            size.frame(13);
        }
        assert_eq!(size.get(), INITIAL_READ_CAPACITY);

        size.reset();
        assert_eq!(size.get(), INITIAL_READ_CAPACITY);
    }
}

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

/// Capacity of the read buffer between frames, and the least room for a read
/// past a frame that does not fit it.
const READ_SIZE: usize = 8 * 1024;

/// Most room for frames following a frame that does not fit the read buffer.
const MAX_READ_AHEAD: usize = 64 * 1024;

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

    /// `buf` was grown past `READ_SIZE` for a large frame
    grown: bool,

    /// An error was returned, so the next poll returns `None`
    has_errored: bool,

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
            buf: BytesMut::new(),
            frame_len: None,
            grown: false,
            has_errored: false,
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

            self.reserve_for_read();
            let res = poll_read_buf(&mut self.inner, cx, &mut self.buf);
            if res.is_pending() && self.grown && self.buf.is_empty() {
                // Don't keep a buffer grown for large frames while idle
                self.buf = BytesMut::new();
                self.grown = false;
            }
            if ready!(res)? == 0 {
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
        Ok(Some(self.buf.split_to(frame_len)))
    }

    /// Makes room for the next read, so that a read of 0 bytes means EOF.
    ///
    /// Frames split off earlier may still share the buffer, and growing it
    /// then copies what is buffered into a new allocation. So it only grows
    /// when the frame at the front does not fit, or when there is no room
    /// left between frames.
    fn reserve_for_read(&mut self) {
        let len = self.buf.len();
        let spare = self.buf.capacity() - len;
        // `split_frame` found no complete frame, so a known frame length
        // exceeds `len`, and otherwise `len` is below `LENGTH_FIELD_LEN`.
        let capacity = match self.frame_len {
            // Room for up to four more frames of the same length. DATA frames
            // mostly come in runs at the max frame size, and whole frames
            // leave no partial frame to copy into the next allocation.
            Some(frame_len) if spare < frame_len - len => {
                let more = frame_len * (MAX_READ_AHEAD / frame_len).min(4);
                frame_len + more.max(READ_SIZE)
            }
            None if spare < frame::HEADER_LEN => READ_SIZE,
            _ => return,
        };
        self.buf.reserve(capacity - len);
        self.grown |= capacity > READ_SIZE;
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

/// Reads from `io` into the spare capacity of `buf`.
fn poll_read_buf<T: AsyncRead + Unpin>(
    io: &mut T,
    cx: &mut Context<'_>,
    buf: &mut BytesMut,
) -> Poll<io::Result<usize>> {
    // `read_buf` is cancel safe, so a new future can be polled on every call
    // and dropped when it returns `Pending`.
    let read = io.read_buf(buf);
    tokio::pin!(read);
    read.poll(cx)
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
    use quickcheck::{Arbitrary, Gen, QuickCheck};
    use std::collections::VecDeque;
    use std::ops::Range;
    use std::sync::Arc;
    use std::task::{Wake, Waker};
    use tokio::io::ReadBuf;

    /// Reads `data` at most `max_read` bytes at a time, and is pending once
    /// at each offset in `breaks`, like a socket that was drained.
    struct Script {
        data: Vec<u8>,
        pos: usize,
        max_read: usize,
        breaks: VecDeque<usize>,
        reads: usize,
    }

    impl Script {
        fn new(data: Vec<u8>, max_read: usize, breaks: impl IntoIterator<Item = usize>) -> Self {
            let mut breaks: Vec<usize> = breaks.into_iter().collect();
            breaks.sort_unstable();
            breaks.dedup();
            Script {
                data,
                pos: 0,
                max_read,
                breaks: breaks.into(),
                reads: 0,
            }
        }
    }

    impl AsyncRead for Script {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            assert!(buf.remaining() > 0, "a read must have room");
            if self.breaks.front() == Some(&self.pos) {
                self.breaks.pop_front();
                return Poll::Pending;
            }
            let end = self.breaks.front().copied().unwrap_or(self.data.len());
            let n = buf.remaining().min(self.max_read).min(end - self.pos);
            let pos = self.pos;
            buf.put_slice(&self.data[pos..pos + n]);
            self.pos += n;
            self.reads += (n > 0) as usize;
            Poll::Ready(Ok(()))
        }
    }

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    /// Encodes frames with payloads of the given lengths and distinct
    /// contents, returning where each one is.
    fn encode(payload_lens: &[usize]) -> (Vec<u8>, Vec<Range<usize>>) {
        let mut data = Vec::new();
        let mut frames = Vec::new();
        for (i, &len) in payload_lens.iter().enumerate() {
            let start = data.len();
            data.put_uint(len as u64, LENGTH_FIELD_LEN);
            data.put_u8(0);
            data.put_u8(0);
            data.put_u32(i as u32);
            data.extend((0..len).map(|j| (i * 31 + j) as u8));
            frames.push(start..data.len());
        }
        (data, frames)
    }

    /// Polls until a read is pending, keeping every frame like the recv
    /// queues of a connection do. Returns whether the stream ended.
    fn poll_until_pending(framed: &mut FramedRead<Script>, frames: &mut Vec<BytesMut>) -> bool {
        let waker = Waker::from(Arc::new(NoopWaker));
        let mut cx = Context::from_waker(&waker);
        loop {
            match framed.poll_next_frame(&mut cx) {
                Poll::Ready(Some(Ok(frame))) => frames.push(frame),
                Poll::Ready(Some(Err(e))) => panic!("{:?}", e),
                Poll::Ready(None) => return true,
                Poll::Pending => return false,
            }
        }
    }

    #[test]
    fn large_frames_take_few_reads() {
        let (data, _) = encode(&[16_384; 64]);
        let mut framed = FramedRead::new(Script::new(data, usize::MAX, None));
        let mut frames = Vec::new();
        assert!(poll_until_pending(&mut framed, &mut frames));
        assert_eq!(frames.len(), 64);
        // Room for whole frames lets one read cover several of them. Reads
        // of 8 KiB, as before, take two for each frame, and room for a single
        // frame one.
        assert!(framed.inner.reads <= 40, "{} reads", framed.inner.reads);
    }

    #[test]
    fn small_frames_reuse_room_after_pending() {
        let (data, frames_at) = encode(&[100; 20]);
        let idle = frames_at[9].end;
        let mut framed = FramedRead::new(Script::new(data, usize::MAX, [idle]));
        let mut frames = Vec::new();
        assert!(!poll_until_pending(&mut framed, &mut frames));
        assert!(poll_until_pending(&mut framed, &mut frames));
        assert_eq!(frames.len(), 20);
        // The frames before the pending read are still held, yet the frames
        // after it are read into the same allocation, right after them.
        let (before, after) = (&frames[9], &frames[10]);
        assert_eq!(before.as_ptr().wrapping_add(before.len()), after.as_ptr());
    }

    #[test]
    fn partial_frames_that_fit_do_not_grow_the_buffer() {
        let (data, _) = encode(&[100; 50]);
        let mut framed = FramedRead::new(Script::new(data, 100, None));
        let mut frames = Vec::new();
        assert!(poll_until_pending(&mut framed, &mut frames));
        assert_eq!(frames.len(), 50);
        // Reads end inside frames, but every frame fits in the initial room,
        // so the buffer never grows past it.
        let consumed: usize = frames.iter().map(|frame| frame.len()).sum();
        assert_eq!(framed.buf.capacity() + consumed, READ_SIZE);
    }

    #[test]
    fn grown_buffer_is_released_when_idle() {
        let (data, frames_at) = encode(&[16_384, 16_384, 10]);
        let idle = frames_at[1].end;
        let mut framed = FramedRead::new(Script::new(data, usize::MAX, [idle]));
        let mut frames = Vec::new();
        assert!(!poll_until_pending(&mut framed, &mut frames));
        assert_eq!(frames.len(), 2);
        assert_eq!(framed.buf.capacity(), 0);
        assert!(poll_until_pending(&mut framed, &mut frames));
        assert_eq!(frames.len(), 3);
    }

    #[derive(Clone, Debug)]
    struct ReadPlan {
        payload_lens: Vec<usize>,
        max_frame_size: usize,
        max_read: usize,
        breaks: Vec<usize>,
        /// Whether each frame is kept or dropped right away, cycled
        keep: Vec<bool>,
    }

    impl Arbitrary for ReadPlan {
        fn arbitrary(g: &mut Gen) -> Self {
            let max_frame_size = *g.choose(&[16_384, 100_000]).unwrap();
            let frames = usize::arbitrary(g) % 24;
            let payload_lens = (0..frames)
                .map(|_| usize::arbitrary(g) % g.choose(&[64, 4_096, max_frame_size + 1]).unwrap())
                .collect();
            let max_read = *g
                .choose(&[100, 1_000, 16_384, 100_000, usize::MAX])
                .unwrap();
            let breaks = (0..usize::arbitrary(g) % 8)
                .map(|_| usize::arbitrary(g))
                .collect();
            let keep = (0..1 + usize::arbitrary(g) % 8)
                .map(|_| bool::arbitrary(g))
                .collect();
            ReadPlan {
                payload_lens,
                max_frame_size,
                max_read,
                breaks,
                keep,
            }
        }
    }

    /// However the stream is cut into reads and pending reads, frames come
    /// out whole, in order and unchanged. Frames that are kept alive, and so
    /// share the buffer with later reads, stay unchanged too.
    #[test]
    fn frames_survive_any_read_pattern() {
        fn prop(plan: ReadPlan) -> bool {
            let (data, frames_at) = encode(&plan.payload_lens);
            let breaks = plan.breaks.iter().map(|b| b % (data.len() + 1));
            let mut framed = FramedRead::new(Script::new(data.clone(), plan.max_read, breaks));
            framed.set_max_frame_size(plan.max_frame_size);
            let waker = Waker::from(Arc::new(NoopWaker));
            let mut cx = Context::from_waker(&waker);
            let mut kept = Vec::new();
            let mut next = 0;
            loop {
                match framed.poll_next_frame(&mut cx) {
                    Poll::Ready(Some(Ok(frame))) => {
                        match frames_at.get(next) {
                            Some(at) if frame[..] == data[at.clone()] => {}
                            _ => return false,
                        }
                        if plan.keep[next % plan.keep.len()] {
                            kept.push((frame, frames_at[next].clone()));
                        }
                        next += 1;
                    }
                    Poll::Ready(Some(Err(_))) => return false,
                    Poll::Ready(None) => break,
                    Poll::Pending => {}
                }
            }
            next == frames_at.len() && kept.into_iter().all(|(f, at)| f[..] == data[at])
        }
        QuickCheck::new()
            .tests(500)
            .quickcheck(prop as fn(ReadPlan) -> bool)
    }
}

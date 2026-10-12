//! The I/O traits h2 reads from and writes to.
//!
//! [`client::handshake`](crate::client::handshake) and
//! [`server::handshake`](crate::server::handshake) take any value that
//! implements [`Read`], [`Write`] and [`Unpin`]. With the `tokio` feature (on
//! by default), every type implementing tokio's `AsyncRead` and `AsyncWrite`
//! implements them.
//!
//! The traits work on h2's own buffers, so a completion-based implementation
//! (io_uring, IOCP) can take a buffer, submit it to the kernel and hand it
//! back once the operation completes, instead of copying through a buffer of
//! its own.

use bytes::{Bytes, BytesMut};

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Reads bytes from a source into h2's read buffer.
pub trait Read {
    /// Appends bytes to `buf`, in its spare capacity, and returns how many
    /// were read. `Ok(0)` means EOF.
    ///
    /// # Lending
    ///
    /// An implementation may detach the spare capacity of `buf` (with
    /// `split_off(buf.len())`) when it submits a read, so the kernel can
    /// write into it after this call returns `Pending`. It must put it back
    /// (with `unsplit`), with the new bytes appended, before returning
    /// `Ready`, and on an error too. The bytes already in `buf` stay in
    /// place.
    ///
    /// After `Pending`, h2 does not change `buf` and passes the same `buf` to
    /// the next call.
    fn poll_read_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut BytesMut,
    ) -> Poll<io::Result<usize>>;
}

/// Writes bytes from h2's write buffer to a sink.
pub trait Write {
    /// Writes from `buf` and returns how many bytes were written.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>;

    /// Writes from `bufs`, in order, and returns how many bytes were written.
    ///
    /// The default writes the first non-empty buffer.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let buf = bufs
            .iter()
            .find(|b| !b.is_empty())
            .map_or(&[][..], |b| &**b);
        self.poll_write(cx, buf)
    }

    /// Whether `poll_write_vectored` is more efficient than `poll_write`.
    fn is_write_vectored(&self) -> bool {
        false
    }

    /// Whether this writer needs owned buffers, as completion-based I/O does.
    ///
    /// When `true`, h2 writes only through [`Write::poll_write_owned`].
    fn needs_owned_buffers(&self) -> bool {
        false
    }

    /// Writes from `bufs`, in order, and returns how many bytes were written.
    ///
    /// `bufs` are reference counted, so an implementation can clone them into
    /// an operation that outlives this call. After `Pending`, h2 passes the
    /// same `bufs` again. An implementation must drop its clones before it
    /// returns `Ready`, so h2 can reuse the memory.
    ///
    /// The default writes with [`Write::poll_write_vectored`].
    fn poll_write_owned(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[Bytes],
    ) -> Poll<io::Result<usize>> {
        const MAX_BUFS: usize = 8;
        let mut slices = [IoSlice::new(&[]); MAX_BUFS];
        let mut n = 0;
        for (slot, buf) in slices.iter_mut().zip(bufs.iter().filter(|b| !b.is_empty())) {
            *slot = IoSlice::new(buf);
            n += 1;
        }
        self.poll_write_vectored(cx, &slices[..n])
    }

    /// Flushes buffered bytes to the sink.
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    /// Shuts down the write side.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

#[cfg(feature = "tokio")]
mod tokio_impls {
    use super::{Read, Write};

    use bytes::BytesMut;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

    use std::future::Future;
    use std::io::{self, IoSlice};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    impl<T: AsyncRead + ?Sized> Read for T {
        #[inline]
        fn poll_read_buf(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut BytesMut,
        ) -> Poll<io::Result<usize>> {
            let mut io = self;
            // `read_buf` is cancel safe, so a new future can be polled on
            // every call and dropped when it returns `Pending`.
            std::pin::pin!(io.read_buf(buf)).poll(cx)
        }
    }

    impl<T: AsyncWrite + ?Sized> Write for T {
        #[inline]
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            AsyncWrite::poll_write(self, cx, buf)
        }

        #[inline]
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            AsyncWrite::poll_write_vectored(self, cx, bufs)
        }

        #[inline]
        fn is_write_vectored(&self) -> bool {
            AsyncWrite::is_write_vectored(self)
        }

        #[inline]
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            AsyncWrite::poll_flush(self, cx)
        }

        #[inline]
        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            AsyncWrite::poll_shutdown(self, cx)
        }
    }
}

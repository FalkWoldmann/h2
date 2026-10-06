//! Runs a client and server over an in-memory pipe that implements only
//! `h2::io` and behaves like completion-based I/O: it takes h2's buffer when
//! a read or write is submitted, completes on a later poll and hands the
//! buffer back. h2 must not touch a buffer while it is lent.

use h2_support::prelude::{h2, http, Bytes, BytesMut};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct Queue {
    bytes: VecDeque<u8>,
    reader: Option<Waker>,
    closed: bool,
}

/// One end of the pipe. Not `Send`, like compio's streams.
struct End {
    rx: Rc<RefCell<Queue>>,
    tx: Rc<RefCell<Queue>>,
    /// The buffer h2 lent to the read in flight.
    lent: Option<BytesMut>,
    /// The buffer h2 lent to the write in flight.
    lent_write: Option<BytesMut>,
    stats: Rc<RefCell<Stats>>,
}

#[derive(Default, Debug)]
struct Stats {
    submitted: usize,
    completed: usize,
    /// Polls with a read in flight where h2 passed something other than the
    /// empty buffer it was left with.
    touched: usize,
    writes_submitted: usize,
    writes_completed: usize,
    write_touched: usize,
    borrowed_writes: usize,
}

fn pipe() -> (End, End, Rc<RefCell<Stats>>) {
    let a = Rc::new(RefCell::new(Queue::default()));
    let b = Rc::new(RefCell::new(Queue::default()));
    let stats = Rc::new(RefCell::new(Stats::default()));
    (
        End {
            rx: a.clone(),
            tx: b.clone(),
            lent: None,
            lent_write: None,
            stats: stats.clone(),
        },
        End {
            rx: b,
            tx: a,
            lent: None,
            lent_write: None,
            stats: stats.clone(),
        },
        stats,
    )
}

impl h2::io::Read for End {
    fn poll_read_buf(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut BytesMut,
    ) -> Poll<io::Result<usize>> {
        let this = &mut *self;
        let Some(mut owned) = this.lent.take() else {
            // Submit: take the buffer and complete on a later poll, even if
            // bytes are already there, so every read goes through lending.
            this.lent = Some(std::mem::take(buf));
            this.stats.borrow_mut().submitted += 1;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        };
        if buf.capacity() != 0 || !buf.is_empty() {
            this.stats.borrow_mut().touched += 1;
        }
        let mut rx = this.rx.borrow_mut();
        if rx.bytes.is_empty() && !rx.closed {
            rx.reader = Some(cx.waker().clone());
            this.lent = Some(owned);
            return Poll::Pending;
        }
        let n = (owned.capacity() - owned.len()).min(rx.bytes.len());
        owned.extend(rx.bytes.drain(..n));
        *buf = owned;
        this.stats.borrow_mut().completed += 1;
        Poll::Ready(Ok(n))
    }
}

impl h2::io::Write for End {
    fn needs_owned_buffers(&self) -> bool {
        true
    }

    fn poll_write_owned(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut BytesMut,
    ) -> Poll<io::Result<usize>> {
        let this = &mut *self;
        let Some(owned) = this.lent_write.take() else {
            this.lent_write = Some(std::mem::take(buf));
            this.stats.borrow_mut().writes_submitted += 1;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        };
        if buf.capacity() != 0 || !buf.is_empty() {
            this.stats.borrow_mut().write_touched += 1;
        }
        // Complete with a partial write now and then.
        let n = if owned.len() > 1000 {
            owned.len() - 7
        } else {
            owned.len()
        };
        let mut tx = this.tx.borrow_mut();
        tx.bytes.extend(&owned[..n]);
        if let Some(w) = tx.reader.take() {
            w.wake();
        }
        *buf = owned;
        this.stats.borrow_mut().writes_completed += 1;
        Poll::Ready(Ok(n))
    }

    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.stats.borrow_mut().borrowed_writes += 1;
        let mut tx = self.tx.borrow_mut();
        tx.bytes.extend(buf);
        if let Some(w) = tx.reader.take() {
            w.wake();
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut tx = self.tx.borrow_mut();
        tx.closed = true;
        if let Some(w) = tx.reader.take() {
            w.wake();
        }
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lending_pipe_echo() {
    let (client_io, server_io, stats) = pipe();
    let local = tokio::task::LocalSet::new();

    local.spawn_local(async move {
        let mut conn = h2::server::handshake(server_io).await.unwrap();
        while let Some(req) = std::future::poll_fn(|cx| conn.poll_accept(cx)).await {
            let (req, mut respond) = req.unwrap();
            tokio::task::spawn_local(async move {
                let mut body = req.into_body();
                let mut got = BytesMut::new();
                while let Some(chunk) = std::future::poll_fn(|cx| body.poll_data(cx)).await {
                    let chunk = chunk.unwrap();
                    body.flow_control().release_capacity(chunk.len()).unwrap();
                    got.extend_from_slice(&chunk);
                }
                let mut send = respond
                    .send_response(http::Response::new(()), false)
                    .unwrap();
                send.send_data(got.freeze(), true).unwrap();
            });
        }
    });

    let body: Bytes = (0..200_000u32).map(|i| i as u8).collect::<Vec<u8>>().into();
    let expected = body.clone();
    let done = local
        .run_until(async move {
            let (mut client, conn) = h2::client::handshake(client_io).await.unwrap();
            tokio::task::spawn_local(async move { conn.await.unwrap() });
            let req = http::Request::post("http://localhost/").body(()).unwrap();
            let (resp, mut send) = client.send_request(req, false).unwrap();
            // Send the body in pieces as capacity becomes available.
            let mut rest = body;
            while !rest.is_empty() {
                send.reserve_capacity(rest.len());
                let cap = std::future::poll_fn(|cx| send.poll_capacity(cx))
                    .await
                    .unwrap()
                    .unwrap();
                let chunk = rest.split_to(cap.min(rest.len()));
                send.send_data(chunk, rest.is_empty()).unwrap();
            }
            let resp = resp.await.unwrap();
            let mut body = resp.into_body();
            let mut got = BytesMut::new();
            while let Some(chunk) = std::future::poll_fn(|cx| body.poll_data(cx)).await {
                let chunk = chunk.unwrap();
                body.flow_control().release_capacity(chunk.len()).unwrap();
                got.extend_from_slice(&chunk);
            }
            got.freeze()
        })
        .await;

    assert_eq!(done, expected);
    let stats = stats.borrow();
    println!(
        "echoed {} bytes over a lending, tokio-free pipe: {:?}",
        done.len(),
        stats
    );
    assert_eq!(stats.touched, 0, "h2 touched a lent buffer");
    assert_eq!(stats.write_touched, 0, "h2 touched a lent write buffer");
}

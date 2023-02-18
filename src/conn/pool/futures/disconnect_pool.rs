// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::ready;

use crate::{
    conn::pool::{Pool, QUEUE_END_ID},
    error::Error,
};

use std::sync::atomic;

/// Future that disconnects this pool from a server and resolves to `()`.
///
/// **Note:** This Future won't resolve until all active connections, taken from it,
/// are dropped or disonnected. Also all pending and new `GetConn`'s will resolve to error.
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct DisconnectPool<'a> {
    pool: &'a mut Pool,
    drop: bool,
}

impl<'a> DisconnectPool<'a> {
    pub(crate) fn new(pool: &'a mut Pool) -> Self {
        Self {
            pool,
            drop: true,
        }
    }
}

impl Future for DisconnectPool<'_> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.pool.inner.close.store(true, atomic::Ordering::Release);
        let mut exchange = self.pool.inner.exchange.lock().unwrap();
        exchange.spawn_futures_if_needed(&self.pool.inner);
        exchange.waiting.push(cx.waker().clone(), QUEUE_END_ID);
        drop(exchange);

        if self.pool.inner.closed.load(atomic::Ordering::Acquire) {
            Poll::Ready(Ok(()))
        } else {
            match self.drop {
                true => match self.pool.drop.send(None) {
                    Ok(_) => {
                        // Recycler is alive. Waiting for it to finish.
                        self.drop = false;
                        Poll::Ready(Ok(ready!(Box::pin(self.pool.drop.closed()).as_mut().poll(cx))))
                    }
                    Err(_) => {
                        // Recycler seem dead. No one will wake us.
                        self.drop = false;
                        Poll::Ready(Ok(()))
                    }
                },
                false => Poll::Pending,
            }
        }
    }
}

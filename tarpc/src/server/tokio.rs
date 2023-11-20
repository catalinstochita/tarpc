use super::{Channel, Requests, Serve};
use futures::{prelude::*, ready, task::*};
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use crate::client::call_shutdown;

/// A future that drives the server by [spawning](tokio::spawn) a [`TokioChannelExecutor`](TokioChannelExecutor)
/// for each new channel. Returned by
/// [`Incoming::execute`](crate::server::incoming::Incoming::execute).
#[must_use]
#[pin_project]
#[derive(Debug)]
pub struct TokioServerExecutor<T, S> {
    #[pin]
    inner: T,
    serve: S,
}

impl<T, S> TokioServerExecutor<T, S> {
    pub(crate) fn new(inner: T, serve: S) -> Self {
        Self { inner, serve }
    }
}

/// A future that drives the server by [spawning](tokio::spawn) each [response
/// handler](super::InFlightRequest::execute) on tokio's default executor. Returned by
/// [`Channel::execute`](crate::server::Channel::execute).
#[must_use]
#[pin_project]
#[derive(Debug)]
pub struct TokioChannelExecutor<T, S, F:FnOnce() + Send + 'static> {
    #[pin]
    inner: T,
    serve: S,
    shutdown_callback:Arc<Mutex<Option<F>>>
}

impl<T, S> TokioServerExecutor<T, S> {
    fn inner_pin_mut<'a>(self: &'a mut Pin<&mut Self>) -> Pin<&'a mut T> {
        self.as_mut().project().inner
    }
}

impl<T, S, F:FnOnce() + Send + 'static> TokioChannelExecutor<T, S, F> {
    fn inner_pin_mut<'a>(self: &'a mut Pin<&mut Self>) -> Pin<&'a mut T> {
        self.as_mut().project().inner
    }
}

// Send + 'static execution helper methods.

impl<C> Requests<C>
where
    C: Channel,
    C::Req: Send + 'static,
    C::Resp: Send + 'static,
{
    /// Executes all requests using the given service function. Requests are handled concurrently
    /// by [spawning](::tokio::spawn) each handler on tokio's default executor.
    pub fn execute<S,F>(self, serve: S,shutdown_callback:F) -> TokioChannelExecutor<Self, S, F>
    where
        S: Serve<C::Req, Resp = C::Resp> + Send + 'static,
        F: FnOnce() + Send + 'static,
    {
        TokioChannelExecutor { inner: self, serve, shutdown_callback:Arc::new(Mutex::new(Some(shutdown_callback))) }
    }
}

impl<St, C, Se> Future for TokioServerExecutor<St, Se>
where
    St: Sized + Stream<Item = C>,
    C: Channel + Send + 'static,
    C::Req: Send + 'static,
    C::Resp: Send + 'static,
    Se: Serve<C::Req, Resp = C::Resp> + Send + 'static + Clone,
    Se::Fut: Send,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        while let Some(channel) = ready!(self.inner_pin_mut().poll_next(cx)) {
            tokio::spawn(channel.execute(self.serve.clone(), ||{}));
        }
        tracing::info!("Server shutting down.");
        Poll::Ready(())
    }
}

impl<C, S, F> Future for TokioChannelExecutor<Requests<C>, S, F>
where
    C: Channel + 'static,
    C::Req: Send + 'static,
    C::Resp: Send + 'static,
    S: Serve<C::Req, Resp = C::Resp> + Send + 'static + Clone,
    S::Fut: Send,
    F: FnOnce() + Send + 'static
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        while let Some(response_handler) = ready!(self.inner_pin_mut().poll_next(cx)) {
            match response_handler {
                Ok(resp) => {
                    let server = self.serve.clone();
                    tokio::spawn(async move {
                        resp.execute(server).await;
                    });
                }
                Err(e) => {
                    tracing::warn!("Requests stream errored out: {}", e);
                    call_shutdown(self.shutdown_callback.clone());
                    break;
                }
            }
        }
        Poll::Ready(())
    }
}

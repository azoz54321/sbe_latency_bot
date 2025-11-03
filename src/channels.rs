use crossbeam_channel::{bounded, Receiver, RecvError, Sender, TryRecvError, TrySendError};

pub fn spsc_channel<T>(capacity: usize) -> (SpscSender<T>, SpscReceiver<T>) {
    let (tx, rx) = bounded(capacity);
    (SpscSender { inner: tx }, SpscReceiver { inner: rx })
}

pub struct SpscSender<T> {
    inner: Sender<T>,
}

impl<T> SpscSender<T> {
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.inner.try_send(value)
    }
}

pub struct SpscReceiver<T> {
    inner: Receiver<T>,
}

impl<T> SpscReceiver<T> {
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }

    #[inline]
    #[allow(dead_code)]
    pub fn recv(&self) -> Result<T, RecvError> {
        self.inner.recv()
    }

    #[inline]
    pub fn into_inner(self) -> Receiver<T> {
        self.inner
    }
}

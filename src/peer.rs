use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicBool, Arc};

enum Choking {
    Choked,
    Unchoked,
}

impl From<u8> for Choking {
    fn from(value: u8) -> Self {
        match value {
            0 => Choking::Unchoked,
            1 => Choking::Choked,
            _ => unreachable!(),
        }
    }
}

enum Interest {
    Interested,
    Uninterested,
}

impl From<u8> for Interest {
    fn from(value: u8) -> Self {
        match value {
            0 => Interest::Uninterested,
            1 => Interest::Interested,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone)]
struct State(Arc<AtomicBool>);

impl State {
    fn new(state: bool) -> Self {
        Self(Arc::new(AtomicBool::new(state)))
    }

    fn set(&self, state: bool) {
        self.store(state, Ordering::Release);
    }

    fn get(&self) -> bool {
        self.load(Ordering::Acquire)
    }
}

impl Deref for State {
    type Target = Arc<AtomicBool>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct Peer {
    shoked: State,
    interested: State,
}

impl Peer {
    pub fn new() -> Self {
        Self {
            shoked: State::new(true),
            interested: State::new(false),
        }
    }
}

use std::fmt;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};

macro_rules! define_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub u64);

        impl $name {
            pub fn new(id: u64) -> Self {
                Self(id)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }
    };
}

define_id!(ConnectionId);
define_id!(SubscriptionId);
define_id!(MessageId);
define_id!(QueueGroupId);

pub struct IdGenerator {
    counter: AtomicU64,
}

impl IdGenerator {
    pub fn new(start: u64) -> Self {
        Self {
            counter: AtomicU64::new(start),
        }
    }

    pub fn next(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn connection_id_create_and_compare() {
        let a = ConnectionId::new(1);
        let b = ConnectionId::new(2);
        let c = ConnectionId::new(1);
        assert_eq!(a, c);
        assert_ne!(a, b);
    }

    #[test]
    fn ids_as_hashmap_keys() {
        let mut conns: HashMap<ConnectionId, &str> = HashMap::new();
        let mut subs: HashMap<SubscriptionId, &str> = HashMap::new();
        conns.insert(ConnectionId::new(1), "conn-1");
        subs.insert(SubscriptionId::new(1), "sub-1");
        assert_eq!(conns.get(&ConnectionId::new(1)), Some(&"conn-1"));
        assert_eq!(subs.get(&SubscriptionId::new(1)), Some(&"sub-1"));
    }

    #[test]
    fn id_generator_sequential() {
        let gen = IdGenerator::new(100);
        assert_eq!(gen.next(), 100);
        assert_eq!(gen.next(), 101);
        assert_eq!(gen.next(), 102);
    }

    #[test]
    fn id_display_format() {
        let id = ConnectionId::new(42);
        assert_eq!(format!("{id}"), "ConnectionId(42)");
    }
}

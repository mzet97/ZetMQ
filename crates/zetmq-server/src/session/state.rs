#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum SessionState {
    New,
    Connected,
    Draining,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionState {
    New,
    Connected,
    Draining,
    Closed,
}

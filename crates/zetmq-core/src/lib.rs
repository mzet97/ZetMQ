//! ZetMQ Core - Domain types and broker logic.
//! This crate has NO dependency on TCP or network I/O.

pub mod broker;
pub mod delivery;
pub mod error;
pub mod id;
pub mod message;
pub mod metrics;
pub mod queue_group;
pub mod routing;
pub mod subject;
pub mod subject_pattern;
pub mod subscription;

pub use broker::BrokerCore;
pub use delivery::{DeliveryHandle, DeliveryMessage, DeliveryStatus};
pub use error::{CoreError, RoutingError};
pub use id::{ConnectionId, IdGenerator, MessageId, QueueGroupId, SubscriptionId};
pub use message::Message;
pub use metrics::CoreMetrics;
pub use queue_group::QueueGroupName;
pub use routing::RoutingEngine;
pub use subject::Subject;
pub use subject_pattern::SubjectPattern;
pub use subscription::Subscription;

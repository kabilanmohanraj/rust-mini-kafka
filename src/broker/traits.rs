//
// Broker specific traits
//

use crate::common::kafka_protocol::KafkaBody;
use crate::common::traits::Codec;
use crate::errors::BrokerError;

pub trait RequestProcess {
    fn process(&self) -> Result<KafkaBody, BrokerError>;
}

pub trait Request: Codec + RequestProcess {}

// blanket implementation for all types that implement Codec and RequestProcess
impl<T> Request for T where T: Codec + RequestProcess {}
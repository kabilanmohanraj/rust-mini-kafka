pub enum KafkaError {
    BrokerError(BrokerError),
    IoError(std::io::Error),
    DecodeError,
    EncodeError,
}

pub enum BrokerError {
    NoError,
    UnknownError,
    UnsupportedVersion,
    UnknownTopicOrPartition,
}

// TODO: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java
// Error handling - todo()
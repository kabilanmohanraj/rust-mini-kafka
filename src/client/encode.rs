use crate::common::kafka_protocol::{ApiVersionsRequest, DescribeTopicPartitionsRequest, FetchRequest};
use crate::common::traits::Encodable;

impl Encodable for ApiVersionsRequest {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend(self.client_software_name.encode());
        buf.extend(self.client_software_version.encode());

        buf
    }
}

impl Encodable for DescribeTopicPartitionsRequest {
    fn encode(&self) -> Vec<u8> {
        let buf = Vec::new();
        // TODO:
        // for topic in &self.topics {
        //     buf.extend(topic.name.encode());
        // }
        // buf.extend(self.response_partition_limit.to_be_bytes());
        // for cursor in &self.cursor {
        //     buf.extend(cursor.topic_name.encode());
        //     buf.extend(cursor.partition_index.to_be_bytes());
        // }
        // buf.extend(self.tagged_fields.encode());

        buf
    }
}

impl Encodable for FetchRequest {
    fn encode(&self) -> Vec<u8> {
        let buf = Vec::new();
        // TODO:
        buf
    }
}
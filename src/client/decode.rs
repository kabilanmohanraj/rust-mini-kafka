use crate::errors::KafkaError;
use crate::common::kafka_protocol::{ApiVersionsResponse, DescribeTopicPartitionsResponse, FetchResponse, RequestContext, TaggedFields};
use crate::common::primitive_types::CompactArray;
use crate::common::traits::Decodable;

impl Decodable for ApiVersionsResponse {
    fn decode(_buf: &[u8], _: &RequestContext) -> Result<(Self, usize), KafkaError> {

        Ok((ApiVersionsResponse {
            error_code: 0,
            api_versions: vec![],
            throttle_time_ms: 0,
            tagged_fields: TaggedFields(None)
        }, 0))
    }
}

impl Decodable for DescribeTopicPartitionsResponse {
    fn decode(_buf: &[u8], _: &RequestContext) -> Result<(Self, usize), KafkaError> {
        Ok( (DescribeTopicPartitionsResponse {
            throttle_time_ms: 0,
            topics: CompactArray { data: vec![] },
            next_cursor: None,
            tagged_fields: TaggedFields(None)
        }, 0) )
    }
}

impl Decodable for FetchResponse {
    fn decode(_buf: &[u8], _: &RequestContext) -> Result<(Self, usize), KafkaError> {
        Ok( (FetchResponse {
            throttle_time_ms: 0,
            error_code: 0,
            session_id: 0,
            responses: CompactArray { data: vec![] },
            tagged_fields: TaggedFields(None)
        }, 0) )
    }
}
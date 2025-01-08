use uuid::Uuid;

use crate::common::traits::Decodable;
use crate::errors::{BrokerError, KafkaError};
use crate::common::kafka_protocol::{ApiVersionsRequest, Cursor, DescribeTopicPartitionsRequest, FetchRequest, FetchRequestPartition, FetchRequestTopic, ForgottenTopicData, KafkaBody, KafkaHeader, KafkaMessage, RequestContext, RequestHeader, RequestTopic, TaggedFields};
use crate::common::primitive_types::{CompactString, CompactArray};



impl Decodable for KafkaMessage {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        // decode message size
        let message_size = match buf[0..4].try_into() {
            Ok(temp) => i32::from_be_bytes(temp),
            Err(_) => return Err(KafkaError::DecodeError)
        };
        offset += 4;

        // decode header information
        let (request_header, header_byte_length) = RequestHeader::decode(&buf[offset..])?;
        offset += header_byte_length;

        // decode message body
        let request: KafkaBody = match request_header.api_key {
            1 => {
                match FetchRequest::decode(&buf[offset..], request_context) {
                    Ok((kmessage, _)) => KafkaBody::Request(Box::new(kmessage)),
                    Err(_) => return Err(KafkaError::DecodeError)
                }
            },
            18 => {
                match ApiVersionsRequest::decode(&buf[offset..], request_context) {
                    Ok((kmessage, _)) => KafkaBody::Request(Box::new(kmessage)),
                    Err(_) => return Err(KafkaError::DecodeError)
                }
            },
            75 => {
                match DescribeTopicPartitionsRequest::decode(&buf[offset..], request_context) {
                    Ok((kmessage, _)) => KafkaBody::Request(Box::new(kmessage)),
                    Err(_) => return Err(KafkaError::DecodeError)
                }
            },
            _ => { return Err(KafkaError::BrokerError(BrokerError::UnknownError)) }
        };

        return Ok( (KafkaMessage {
            size: message_size,
            header: KafkaHeader::Request(request_header),
            body: request,
        }, 0) )
    }
}

impl Decodable for ApiVersionsRequest {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        let client_software_name = match CompactString::decode(&buf[offset..], request_context) {
            Ok((client_software_name, data_length)) => {
                offset += data_length;
                client_software_name
            }
            Err(_) => return Err(KafkaError::DecodeError)
        };

        let client_software_version = match CompactString::decode(&buf[offset..], request_context) {
            Ok((client_software_version, data_length)) => {
                offset += data_length;
                client_software_version
            }
            Err(_) => return Err(KafkaError::DecodeError)
        };
        
        
        let tagged_fields = match TaggedFields::decode(&buf[offset..], request_context) {
            Ok((tagged_fields, _)) => {
                // offset += data_length;
                tagged_fields
            }
            Err(_) => return Err(KafkaError::DecodeError)
        };

        Ok(  
            (ApiVersionsRequest {
                client_software_name,
                client_software_version,
                tagged_fields
            }, 0)
        )
    }
}

// DescribeTopicPartitions Request
impl Decodable for DescribeTopicPartitionsRequest {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        // decode request topics
        let (request_topics, topics_bytes) = CompactArray::<RequestTopic>::decode(&buf[offset..], request_context)?;
        offset += topics_bytes;

        println!("Request topic name: {:?}", request_topics.data[0].name.data);
        
        // decode response partition limit
        let temp: &[u8; 4] = match buf[offset..offset+4].try_into() {
            Ok(val) => val,
            Err(_) => return Err(KafkaError::DecodeError),
        };
        let response_partition_limit = i32::from_be_bytes(*temp);
        offset += 4;

        // decode cursor
        let cursor: Option<Cursor> = if buf[offset] == 0xFF {
            offset += 1;
            None
        } else {
            match Cursor::decode(&buf[offset..], request_context) {
                Ok((cursor, cursor_len)) => {
                    offset += cursor_len;
                    Some(cursor)
                }, 
                Err(_) => return Err(KafkaError::DecodeError)
            }
        };

        let (tagged_fields, tf_len) = TaggedFields::decode(&buf[offset..], request_context)?;
        offset += tf_len;

        Ok( (DescribeTopicPartitionsRequest {
            topics: request_topics,
            response_partition_limit,
            cursor,
            tagged_fields
        }, offset) )
    }
}

impl Decodable for RequestTopic {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        let (name, name_len) = CompactString::decode(&buf[offset..], request_context)?;
        offset += name_len;

        let (tagged_fields, tf_len) = TaggedFields::decode(&buf[offset..], request_context)?;
        offset += tf_len;

        Ok( (RequestTopic {
            name,
            tagged_fields
        }, offset) )
    }
}

impl Decodable for Cursor {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        let (topic_name, topic_name_len) = CompactString::decode(&buf[offset..], request_context)?;
        offset += topic_name_len;

        let temp: &[u8; 4] = &buf[offset..offset+4].try_into().expect("Could not get partition index from buffer...\n");
        let partition_index = i32::from_be_bytes(*temp);
        offset += 4;

        let tagged_fields = TaggedFields(None);

        Ok( (Cursor {
            topic_name,
            partition_index,
            tagged_fields
        }, offset) )
    }
}

// Fetch Request
impl Decodable for FetchRequest {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        macro_rules! read_bytes {
            ($num_bytes:expr) => {{
                if(buf.len() < offset + $num_bytes) {
                    println!("Insufficient bytes to decode FetchRequest...");
                    return Err(KafkaError::DecodeError);
                }

                let bytes = &buf[offset..offset + $num_bytes];
                offset += $num_bytes;
                bytes
            }};
        }

        println!("Decoding FetchRequest...");

        let max_wait_ms = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Max wait ms: {:?}", max_wait_ms);

        let min_bytes = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Min bytes: {:?}", min_bytes);

        let max_bytes = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Max bytes: {:?}", max_bytes);

        let isolation_level = i8::from_be_bytes(read_bytes!(1).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Isolation level: {:?}", isolation_level);

        let session_id = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Session id: {:?}", session_id);

        let session_epoch = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Session epoch: {:?}", session_epoch);

        let (topics, topics_len) = CompactArray::<FetchRequestTopic>::decode(&buf[offset..], request_context)?;
        offset += topics_len;

        let (forgotten_topics_data, ftd_byte_len) = CompactArray::<ForgottenTopicData>::decode(&buf[offset..], request_context)?;
        offset += ftd_byte_len;

        let (rack_id, rid_byte_len) = CompactString::decode(&buf[offset..], request_context)?;
        offset += rid_byte_len;
        println!("Rack id: {:?}", rack_id.data);

        let (tagged_fields, tf_len) = TaggedFields::decode(&buf[offset..], request_context)?;
        offset += tf_len;

        Ok( (FetchRequest {
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics_data,
            rack_id,
            tagged_fields
        }, offset) )
    }
}

impl Decodable for FetchRequestTopic {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        macro_rules! read_bytes {
            ($num_bytes:expr) => {{
                if(buf.len() < offset + $num_bytes) {
                    println!("Insufficient bytes to decode FetchRequestTopic...");
                    return Err(KafkaError::DecodeError);
                }

                let bytes = &buf[offset..offset + $num_bytes];
                offset += $num_bytes;
                bytes
            }};
        }

        println!("Decoding FetchRequestTopic...");

        let topic_id = Uuid::from_slice(read_bytes!(16)).map_err(|_| KafkaError::DecodeError)?;
        println!("  Topic id: {:?}", topic_id);

        let (partitions, partitions_len) = CompactArray::<FetchRequestPartition>::decode(&buf[offset..], request_context)?;
        offset += partitions_len;

        let (tagged_fields, tf_len) = TaggedFields::decode(&buf[offset..], request_context)?;
        offset += tf_len;

        Ok( (FetchRequestTopic {
            topic_id,
            partitions,
            tagged_fields
        }, offset) )
    }
}

impl Decodable for FetchRequestPartition {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        macro_rules! read_bytes {
            ($num_bytes:expr) => {{
                if(buf.len() < offset + $num_bytes) {
                    println!("Insufficient bytes to decode FetchRequestPartition...");
                    return Err(KafkaError::DecodeError);
                }

                let bytes = &buf[offset..offset + $num_bytes];
                offset += $num_bytes;
                bytes
            }};
        }

        println!("      Decoding FetchRequestPartition...");

        let partition = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("      Partition: {:?}", partition);

        let current_leader_epoch = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("      Current leader epoch: {:?}", current_leader_epoch);

        let fetch_offset = i64::from_be_bytes(read_bytes!(8).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("      Fetch offset: {:?}", fetch_offset);

        let last_fetched_epoch = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("      Last fetched epoch: {:?}", last_fetched_epoch);

        let log_start_offset = i64::from_be_bytes(read_bytes!(8).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("      Log start offset: {:?}", log_start_offset);

        let partition_max_bytes = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("      Partition max bytes: {:?}", partition_max_bytes);

        let (tagged_fields, tf_len) = TaggedFields::decode(&buf[offset..], request_context)?;
        offset += tf_len;

        Ok( (FetchRequestPartition {
            partition,
            current_leader_epoch,
            fetch_offset,
            last_fetched_epoch,
            log_start_offset,
            partition_max_bytes,
            tagged_fields
        }, offset) )
    }
}

impl Decodable for ForgottenTopicData {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        macro_rules! read_bytes {
            ($num_bytes:expr) => {{
                if(buf.len() < offset + $num_bytes) {
                    println!("Insufficient bytes to decode ForgottenTopicData...");
                    return Err(KafkaError::DecodeError);
                }

                let bytes = &buf[offset..offset + $num_bytes];
                offset += $num_bytes;
                bytes
            }};
        }

        println!("  Decoding ForgottenTopicData...");

        let topic_id = Uuid::from_slice(read_bytes!(16)).map_err(|_| KafkaError::DecodeError)?;
        println!("  Topic id: {:?}", topic_id);

        let (partitions, partitions_len) = CompactArray::<i32>::decode(&buf[offset..], request_context)?;
        offset += partitions_len;
        println!("  Partitions: {:?}", partitions.data);

        let (tagged_fields, tf_len) = TaggedFields::decode(&buf[offset..], request_context)?;
        offset += tf_len;

        Ok( (ForgottenTopicData {
            topic_id,
            partitions,
            tagged_fields
        }, offset) )
    }
}
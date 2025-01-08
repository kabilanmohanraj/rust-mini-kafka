use std::result::Result::Ok;
use crc32c::crc32c;

use crate::errors::KafkaError;
use super::{kafka_protocol::{RequestContext, TaggedFields}, primitive_types::{CompactArray, CompactString, SVarInt, UnsignedVarInt}, traits::{Decodable, Encodable}};

pub enum RecordValue {
    TopicRecord(TopicRecord),
    PartitionRecord(PartitionRecord),
    FeatureLevelRecord(FeatureLevelRecord),
    RawBytesRecord(RawBytesRecord),
}

impl Encodable for RecordValue {
    fn encode(&self) -> Vec<u8> {
        match self {
            RecordValue::TopicRecord(topic_record) => topic_record.encode(),
            RecordValue::PartitionRecord(partition_record) => partition_record.encode(),
            RecordValue::FeatureLevelRecord(feature_level_record) => feature_level_record.encode(),
            RecordValue::RawBytesRecord(raw_bytes) => raw_bytes.data.to_vec(),
        }
    }
}

impl Decodable for RecordValue {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;

        // Helper function to decode metadata records
        let decode_metadata_record = |record_type: i8, buf: &[u8], request_context: &RequestContext| -> Result<(Self, usize), KafkaError> {
            match record_type {
                2 => {
                    let (topic_record, topic_record_size) =
                        TopicRecord::decode(buf, request_context).map_err(|_| KafkaError::DecodeError)?;
                    Ok( (RecordValue::TopicRecord(topic_record), topic_record_size) )
                }
                3 => {
                    let (partition_record, partition_record_size) =
                        PartitionRecord::decode(buf, request_context).map_err(|_| KafkaError::DecodeError)?;
                    Ok( (RecordValue::PartitionRecord(partition_record), partition_record_size) )
                }
                12 => {
                    let (feature_level_record, feature_level_record_size) =
                        FeatureLevelRecord::decode(buf, request_context).map_err(|_| KafkaError::DecodeError)?;
                    Ok( (RecordValue::FeatureLevelRecord(feature_level_record), feature_level_record_size) )
                }
                _ => {
                    println!("Unrecognized metadata record type: {}", record_type);
                    Err(KafkaError::DecodeError)
                }
            }
        };

        if let Some(context_map) = request_context.as_ref() {
            if context_map.get("is_metadata_request") == Some(&"true".to_string()) {
                let second_byte: u8 = *buf.get(offset + 1).ok_or(KafkaError::DecodeError)?;
                let record_type = second_byte as i8;

                let (record_value, size) = decode_metadata_record(record_type, &buf[offset..], request_context)?;
                offset += size;
                return Ok( (record_value, offset) );
            }
        }

        // Decode as raw bytes if not a metadata request
        let (record_value, rc_len) = RawBytesRecord::decode(&buf[offset..], request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += rc_len;
        Ok((RecordValue::RawBytesRecord(record_value), offset))
    }
}


pub struct RecordHeader {
    pub header_key: CompactString,
    pub value: Vec<u8>,
}

impl Encodable for RecordHeader {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend(&self.header_key.encode());
        buf.extend(&self.value);

        buf
    }
}

impl Decodable for RecordHeader {
    fn decode(buf: &[u8], _: &RequestContext) -> Result<(RecordHeader, usize), KafkaError> {
        println!("  Decoding record header...");
        let mut offset = 0;

        let (header_key, header_key_size) = CompactString::decode(&buf[offset..], &RequestContext::None).map_err(|_| KafkaError::DecodeError)?;
        offset += header_key_size;

        let value = &buf[offset..];
        offset += value.len();

        Ok((RecordHeader {
            header_key,
            value: value.to_vec(),
        }, offset))
    }
}

pub struct Record {
    pub attributes: i8,
    pub timestamp_delta: SVarInt,
    pub offset_delta: SVarInt,
    pub key: Option<Vec<u8>>,
    pub value: RecordValue,
    pub headers: Vec<RecordHeader>,
}

impl Encodable for Record {
    fn encode(&self) -> Vec<u8> {
        let mut temp_buf: Vec<u8> = Vec::new();

        temp_buf.push(self.attributes as u8);
        temp_buf.extend(&self.timestamp_delta.encode());
        temp_buf.extend(&self.offset_delta.encode());

        // encode key
        match &self.key {
            Some(key) => {
                temp_buf.extend(SVarInt::new(key.len() as i32).encode());
                temp_buf.extend(key);
            },
            None => {
                temp_buf.extend(SVarInt::new(-1 as i32).encode());
            }
        }

        // encode value
        // we need to encode the record value first to get the length because, record is not a Vec<u8> but a type like TopicRecord, PartitionRecord, etc.
        let value_encoded = &self.value.encode();
        temp_buf.extend(SVarInt::new(value_encoded.len() as i32).encode());
        temp_buf.extend(value_encoded);

        // encode headers
        temp_buf.extend(UnsignedVarInt::new(self.headers.len() as u32).encode());
        for header in &self.headers {
            temp_buf.extend(header.encode());
        }

        let mut buf: Vec<u8> = Vec::new();
        buf.extend(SVarInt::new(temp_buf.len() as i32).encode());
        buf.extend(&temp_buf);

        buf
    }
}

impl Decodable for Record {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(Self, usize), KafkaError> {
        println!("  Decoding record...");
        let mut offset = 0;

        macro_rules! read_bytes {
            ($size:expr) => {{
            if buf.len() < offset + $size {
                println!("Insufficient data to decode MetadataRecord");
                return Err(KafkaError::DecodeError);
            }
            let bytes = &buf[offset..offset + $size];
            offset += $size;
            bytes
            }};
        }

        let empty_request_context = &RequestContext::None;

        let (record_size, record_size_byte_len) = SVarInt::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += record_size_byte_len;
        println!("  Record size: {}", record_size.data);

        let attributes = i8::from_be_bytes(read_bytes!(1).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("  Attributes: {}", attributes);

        let (timestamp_delta, td_byte_len) = SVarInt::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += td_byte_len;
        println!("  Timestamp delta: {}", timestamp_delta.data);

        let (offset_delta, od_byte_len) = SVarInt::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += od_byte_len;
        println!("  Offset delta: {}", offset_delta.data);

        let (key_size, key_size_byte_len) = SVarInt::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += key_size_byte_len;
        println!("  Key size: {}, {}", key_size.data, key_size_byte_len);
        let key = if key_size.data == -1 { // -1 means null
            None
        } else {
            let key = &buf[offset..offset + key_size.data as usize];
            offset += key_size.data as usize;
            Some(key.to_vec())
        };

        let (value_size, value_size_byte_len) = SVarInt::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += value_size_byte_len;
        println!("  Value size: {}, #bytes: {}", value_size.data, value_size_byte_len);

        let (value, _) = RecordValue::decode(&buf[offset..offset+value_size.data as usize], request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += value_size.data as usize;

        let (headers_size, hs_byte_len) = UnsignedVarInt::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += hs_byte_len;
        println!("  Headers size: {}, {}", headers_size.data, hs_byte_len);

        let mut headers = Vec::new();
        for _ in 0..headers_size.data {
            let (header, header_size) = RecordHeader::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
            offset += header_size;
            headers.push(header);
        }

        Ok((Record {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        }, offset))
    }
}

#[derive(Debug)]
pub struct RecordValueMetadata {
    pub frame_version: i8,
    pub record_type: i8,
    pub version: i8,
}

impl Encodable for RecordValueMetadata {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(self.frame_version as u8);
        buf.push(self.record_type as u8);
        buf.push(self.version as u8);

        buf
    }
}

impl Decodable for RecordValueMetadata {
    fn decode(buf: &[u8], _: &RequestContext) -> Result<(Self, usize), KafkaError> {
        let mut offset = 0;
        macro_rules! read_bytes {
            ($size:expr) => {{
            if buf.len() < offset + $size {
                println!("Insufficient data to decode RecordValueMetadata");
                return Err(KafkaError::DecodeError);
            }
            let bytes = &buf[offset..offset + $size];
            offset += $size;
            bytes
            }};
        }

        let value_metadata = RecordValueMetadata {
            frame_version: i8::from_be_bytes(read_bytes!(1).try_into().map_err(|_| KafkaError::DecodeError)?),
            record_type: i8::from_be_bytes(read_bytes!(1).try_into().map_err(|_| KafkaError::DecodeError)?),
            version: i8::from_be_bytes(read_bytes!(1).try_into().map_err(|_| KafkaError::DecodeError)?),
        };

        println!("      Frame version: {}", value_metadata.frame_version);
        println!("      Record type: {}", value_metadata.record_type);
        println!("      Version: {}", value_metadata.version);

        Ok( (value_metadata, offset) )
    }
}

// {
//     "apiKey": 2,
//     "type": "metadata",
//     "name": "TopicRecord",
//     "validVersions": "0",
//     "flexibleVersions": "0+",
//     "fields": [
//       { "name": "Name", "type": "string", "versions": "0+", "entityType": "topicName",
//         "about": "The topic name." },
//       { "name": "TopicId", "type": "uuid", "versions": "0+",
//         "about": "The unique ID of this topic." }
//     ]
// }
pub struct TopicRecord {
    pub value_metadata: RecordValueMetadata,
    pub topic_name: CompactString,
    pub topic_id: uuid::Uuid,
    pub tagged_fields: TaggedFields
}

impl Encodable for TopicRecord {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend(&self.value_metadata.encode());
        buf.extend(&self.topic_name.encode());
        buf.extend(self.topic_id.as_bytes());
        buf.extend(&self.tagged_fields.encode());

        buf
    }
}

impl Decodable for TopicRecord {
    fn decode(buf: &[u8], _: &RequestContext) -> Result<(TopicRecord, usize), KafkaError> {
        println!("      Decoding topic record...");
        let mut offset = 0;

        let empty_request_context = &RequestContext::None;

        let (value_metadata, vm_byte_len) = RecordValueMetadata::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += vm_byte_len;

        let (topic_name, topic_name_size) = CompactString::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += topic_name_size;

        let topic_id = uuid::Uuid::from_slice(&buf[offset..offset+16]).map_err(|_| KafkaError::DecodeError)?;
        offset += 16;

        let (tagged_fields, tf_byte_len) = TaggedFields::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += tf_byte_len;

        Ok((TopicRecord {
            value_metadata,
            topic_name,
            topic_id,
            tagged_fields,
        }, offset))
    }
}

// {
//     "apiKey": 3,
//     "type": "metadata",
//     "name": "PartitionRecord",
//     // Version 1 adds Directories for KIP-858
//     // Version 2 implements Eligible Leader Replicas and LastKnownElr as described in KIP-966.
//     "validVersions": "0-2",
//     "flexibleVersions": "0+",
//     "fields": [
//       { "name": "PartitionId", "type": "int32", "versions": "0+", "default": "-1",
//         "about": "The partition id." },
//       { "name": "TopicId", "type": "uuid", "versions": "0+",
//         "about": "The unique ID of this topic." },
//       { "name": "Replicas", "type":  "[]int32", "versions":  "0+", "entityType": "brokerId",
//         "about": "The replicas of this partition, sorted by preferred order." },
//       { "name": "Isr", "type":  "[]int32", "versions":  "0+",
//         "about": "The in-sync replicas of this partition" },
//       { "name": "RemovingReplicas", "type":  "[]int32", "versions":  "0+", "entityType": "brokerId",
//         "about": "The replicas that we are in the process of removing." },
//       { "name": "AddingReplicas", "type":  "[]int32", "versions":  "0+", "entityType": "brokerId",
//         "about": "The replicas that we are in the process of adding." },
//       { "name": "Leader", "type": "int32", "versions": "0+", "default": "-1", "entityType": "brokerId",
//         "about": "The lead replica, or -1 if there is no leader." },
//       { "name": "LeaderRecoveryState", "type": "int8", "default": "0", "versions": "0+", "taggedVersions": "0+", "tag": 0,
//         "about": "1 if the partition is recovering from an unclean leader election; 0 otherwise." },
//       { "name": "LeaderEpoch", "type": "int32", "versions": "0+", "default": "-1",
//         "about": "The epoch of the partition leader." },
//       { "name": "PartitionEpoch", "type": "int32", "versions": "0+", "default": "-1",
//         "about": "An epoch that gets incremented each time we change anything in the partition." },
//       { "name": "Directories", "type": "[]uuid", "versions": "1+",
//         "about": "The log directory hosting each replica, sorted in the same exact order as the Replicas field."},
//       { "name": "EligibleLeaderReplicas", "type": "[]int32", "default": "null", "entityType": "brokerId",
//         "versions": "2+", "nullableVersions": "2+", "taggedVersions": "2+", "tag": 1,
//         "about": "The eligible leader replicas of this partition." },
//       { "name": "LastKnownElr", "type": "[]int32", "default": "null", "entityType": "brokerId",
//         "versions": "2+", "nullableVersions": "2+", "taggedVersions": "2+", "tag": 2,
//         "about": "The last known eligible leader replicas of this partition." }
//     ]
// }
pub struct PartitionRecord {
    pub value_metadata: RecordValueMetadata,
    pub partition_id: i32,
    pub topic_id: uuid::Uuid,
    pub replica_array: CompactArray<i32>,
    pub isr_array: CompactArray<i32>,
    pub removing_replicas_array: CompactArray<i32>,
    pub adding_replicas_array: CompactArray<i32>,
    pub leader: i32,
    pub leader_epoch: i32,
    pub partition_epoch: i32,
    pub directories: CompactArray<uuid::Uuid>,
    pub tagged_fields: TaggedFields,
}

impl Encodable for PartitionRecord {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend(&self.value_metadata.encode());
        buf.extend(&self.partition_id.to_be_bytes());
        buf.extend(self.topic_id.as_bytes());
        buf.extend(&self.replica_array.encode());
        buf.extend(&self.isr_array.encode());
        buf.extend(&self.removing_replicas_array.encode());
        buf.extend(&self.adding_replicas_array.encode());
        buf.extend(&self.leader.to_be_bytes());
        buf.extend(&self.leader_epoch.to_be_bytes());
        buf.extend(&self.partition_epoch.to_be_bytes());
        buf.extend(&self.directories.encode());
        buf.extend(&self.tagged_fields.encode());

        buf
    }
}

impl Decodable for PartitionRecord {
    fn decode(buf: &[u8], _: &RequestContext) -> Result<(PartitionRecord, usize), KafkaError> {
        println!("      Decoding partition record...");
        let mut offset = 0;

        macro_rules! read_bytes {
            ($size:expr) => {{
            if buf.len() < offset + $size {
                println!("Insufficient data to decode PartitionRecord");
                return Err(KafkaError::DecodeError);
            }
            let bytes = &buf[offset..offset + $size];
            offset += $size;
            bytes
            }};
        }

        let empty_request_context = &RequestContext::None;

        let (value_metadata, vm_byte_len) = RecordValueMetadata::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += vm_byte_len;

        let partition_id = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);

        let topic_id = uuid::Uuid::from_slice(&buf[offset..offset+16]).map_err(|_| KafkaError::DecodeError)?;
        offset += 16;

        let (replica_array, ra_byte_len) = CompactArray::<i32>::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += ra_byte_len;

        let (isr_array, isa_byte_len) = CompactArray::<i32>::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += isa_byte_len;

        let (removing_replicas_array, rra_byte_len) = CompactArray::<i32>::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += rra_byte_len;

        let (adding_replicas_array, ara_byte_len) = CompactArray::<i32>::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += ara_byte_len;

        let leader = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        let leader_epoch = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        let partition_epoch = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);

        let (directories, dir_byte_len) = CompactArray::<uuid::Uuid>::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += dir_byte_len;

        let (tagged_fields, tf_byte_len) = TaggedFields::decode(&buf[offset..], empty_request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += tf_byte_len;

        Ok((PartitionRecord {
            value_metadata,
            partition_id,
            topic_id,
            replica_array,
            isr_array,
            removing_replicas_array,
            adding_replicas_array,
            leader,
            leader_epoch,
            partition_epoch,
            directories,
            tagged_fields,
        }, offset))

    }
}

// {
//     // Note: New metadata logs and snapshots begin with a FeatureLevelRecord which specifies the
//     // metadata level that is required to read them. The version of that record cannot advance
//     // beyond 0, for backwards compatibility reasons.
//     "apiKey": 12,
//     "type": "metadata",
//     "name": "FeatureLevelRecord",
//     "validVersions": "0",
//     "flexibleVersions": "0+",
//     "fields": [
//       { "name": "Name", "type": "string", "versions": "0+",
//         "about": "The feature name." },
//       { "name": "FeatureLevel", "type": "int16", "versions": "0+",
//         "about": "The current finalized feature level of this feature for the cluster, a value of 0 means feature not supported." }
//     ]
// }
pub struct FeatureLevelRecord {
    pub value_metadata: RecordValueMetadata,
    pub name: CompactString,
    pub feature_level: i16,
    pub tagged_fields: TaggedFields,
}

impl Encodable for FeatureLevelRecord {
    fn encode(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        buf.extend(&self.value_metadata.encode());
        buf.extend(&self.name.encode());
        buf.extend(&self.feature_level.to_be_bytes());
        buf.extend(&self.tagged_fields.encode());

        buf
    }
}

impl Decodable for FeatureLevelRecord {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(FeatureLevelRecord, usize), KafkaError> {
        println!("      Decoding feature level record...");
        let mut offset = 0;

        macro_rules! read_bytes {
            ($size:expr) => {{
            if buf.len() < offset + $size {
                println!("Insufficient data to decode FeatureLevelRecord");
                return Err(KafkaError::DecodeError);
            }
            let bytes = &buf[offset..offset + $size];
            offset += $size;
            bytes
            }};
        }

        let (value_metadata, vm_byte_len) = RecordValueMetadata::decode(&buf[offset..], request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += vm_byte_len;

        println!("      Offset: {} | Buffer remaining: {}", offset, buf[offset..].len());
        let (name, name_size) = CompactString::decode(&buf[offset..], request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += name_size;
        println!("      Name: {}", name.data);

        let feature_level = i16::from_be_bytes(read_bytes!(2).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("      Feature level: {}", feature_level);

        // let (num_tagged_fields, tf_byte_len) = UnsignedVarInt::decode(&buf[offset..]).map_err(|_| KafkaError::DecodeError)?;
        // offset += tf_byte_len;

        let (tagged_fields, tf_byte_len) = TaggedFields::decode(&buf[offset..], request_context).map_err(|_| KafkaError::DecodeError)?;
        offset += tf_byte_len;

        Ok((FeatureLevelRecord {
            value_metadata,
            name,
            feature_level,
            tagged_fields,
        }, offset))
    }
}


// RawBytesRecord
pub struct RawBytesRecord {
    pub data: Vec<u8>,
}

impl Encodable for RawBytesRecord {
    fn encode(&self) -> Vec<u8> {
        self.data.to_vec()
    }
}

impl Decodable for RawBytesRecord {
    fn decode(buf: &[u8], _: &RequestContext) -> Result<(RawBytesRecord, usize), KafkaError> {

        Ok((RawBytesRecord {
            data: buf.to_vec(),
        }, buf.len()))
    }
}

// baseOffset: int64
// batchLength: int32
// partitionLeaderEpoch: int32
// magic: int8 (current magic value is 2)
// crc: uint32
// attributes: int16
//     bit 0~2:
//         0: no compression
//         1: gzip
//         2: snappy
//         3: lz4
//         4: zstd
//     bit 3: timestampType
//     bit 4: isTransactional (0 means not transactional)
//     bit 5: isControlBatch (0 means not a control batch)
//     bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
//     bit 7~15: unused
// lastOffsetDelta: int32
// baseTimestamp: int64
// maxTimestamp: int64
// producerId: int64
// producerEpoch: int16
// baseSequence: int32
// records: [Record]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl Encodable for RecordBatch {
    fn encode(&self) -> Vec<u8> {
        // collect all the fields into a temporary buffer
        let mut after_crc_buf: Vec<u8> = Vec::new();
        after_crc_buf.extend(&self.attributes.to_be_bytes());
        after_crc_buf.extend(&self.last_offset_delta.to_be_bytes());
        after_crc_buf.extend(&self.base_timestamp.to_be_bytes());
        after_crc_buf.extend(&self.max_timestamp.to_be_bytes());
        after_crc_buf.extend(&self.producer_id.to_be_bytes());
        after_crc_buf.extend(&self.producer_epoch.to_be_bytes());
        after_crc_buf.extend(&self.base_sequence.to_be_bytes());

        // encode records
        after_crc_buf.extend(&(self.records.len() as i32).to_be_bytes());
        for record in &self.records {
            after_crc_buf.extend(record.encode());
        }

        // compute CRC
        let crc = crc32c(&after_crc_buf);

        // collect all the fields into a temporary buffer
        let mut temp_buf: Vec<u8> = Vec::new();
        temp_buf.extend(&self.partition_leader_epoch.to_be_bytes());
        temp_buf.extend(&self.magic.to_be_bytes());
        temp_buf.extend(&crc.to_be_bytes());
        temp_buf.extend(&after_crc_buf);

        // finally, encode the batch length and append the temporary buffer
        let mut buf = Vec::new();
        buf.extend(&self.base_offset.to_be_bytes());
        buf.extend(&(temp_buf.len() as i32).to_be_bytes());
        buf.extend(&temp_buf);
        
        buf
    }
}

impl Decodable for RecordBatch {
    fn decode(buf: &[u8], request_context: &RequestContext) -> Result<(RecordBatch, usize), KafkaError> {
        let mut offset = 0;

        macro_rules! read_bytes {
            ($size:expr) => {{
            if buf.len() < offset + $size {
                println!("Insufficient data to decode RecordBatch");
                return Err(KafkaError::DecodeError);
            }
            let bytes = &buf[offset..offset + $size];
            offset += $size;
            bytes
            }};
        }

        let base_offset = i64::from_be_bytes(read_bytes!(8).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("\nBase offset: {}", base_offset);

        let batch_length = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Batch length: {}", batch_length);

        let partition_leader_epoch = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Partition leader epoch: {}", partition_leader_epoch);
        
        let magic = i8::from_be_bytes(read_bytes!(1).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Magic: {}", magic);

        let crc = u32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("CRC: {}", crc);

        let attributes = i16::from_be_bytes(read_bytes!(2).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Attributes: {}", attributes);

        let last_offset_delta = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Last offset delta: {}", last_offset_delta);

        let base_timestamp = i64::from_be_bytes(read_bytes!(8).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Base timestamp: {}", base_timestamp);

        let max_timestamp = i64::from_be_bytes(read_bytes!(8).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Max timestamp: {}", max_timestamp);

        let producer_id = i64::from_be_bytes(read_bytes!(8).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Producer ID: {}", producer_id);

        let producer_epoch = i16::from_be_bytes(read_bytes!(2).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Producer epoch: {}", producer_epoch);

        let base_sequence = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Base sequence: {}", base_sequence);

        let num_records = i32::from_be_bytes(read_bytes!(4).try_into().map_err(|_| KafkaError::DecodeError)?);
        println!("Number of records: {}", num_records);

        // TODO: Use this to read multiple record batches simultaneously from the file
        // batch_length -= 49; // adjust batch length by subtracting the length of all the fields above
        println!("Decoding {} records...", num_records);
        let mut records: Vec<Record> = Vec::new();
        for _ in 0..num_records {
            let (record, record_size) = Record::decode(&buf[offset..], request_context).map_err(|_| KafkaError::DecodeError)?;
            offset += record_size;
            records.push(record);
        }

        println!("Decoded {} records", records.len());

        Ok((RecordBatch {
            base_offset,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        }, offset))
    }
}


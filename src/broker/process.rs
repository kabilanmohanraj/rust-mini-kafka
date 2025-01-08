use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use crate::common::traits::Decodable;
use crate::common::primitive_types::{CompactArray, CompactNullableString, CompactString};
use crate::common::kafka_record::{PartitionRecord, RecordBatch, RecordValue};
use crate::common::kafka_protocol::{ApiKey, ApiVersionsRequest, ApiVersionsResponse, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, FetchRequest, FetchResponse, FetchResponsePartition, FetchResponseTopic, KafkaBody, PartitionMetadata, RequestContext, ResponseTopic, TaggedFields};

use crate::broker::traits::RequestProcess;
use crate::errors::BrokerError;
use crate::api_versions::get_all_apis;

use uuid::Uuid;

impl RequestProcess for KafkaBody {
    fn process(&self) -> Result<KafkaBody, BrokerError> {
        match self {
            KafkaBody::Request(request) => {
                request.process()
            },
            KafkaBody::Response(_) => {
                Err(BrokerError::UnknownError)
            }
        }
    }
}

impl RequestProcess for DescribeTopicPartitionsRequest {
    fn process(&self) -> Result<KafkaBody, BrokerError> {
        println!("Processing DescribeTopicPartitionsRequest...");

        // open cluster metadata file
        let metadata_file_path = Path::new("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
        let mut metadata_file = File::open(&metadata_file_path).map_err(|_| BrokerError::UnknownError)?;

        // decode cluster metadata
        let mut buf: Vec<u8> = Vec::new();
        metadata_file.read_to_end(&mut buf).map_err(|_| BrokerError::UnknownError)?;

        println!("Initial Buffer length: {:?}", buf.len());
        println!("Decoding record batch...");

        let mut record_batches: Vec<RecordBatch> = Vec::new();
        let mut offset = 0;

        let mut context_map: HashMap<String, String> = HashMap::new();
        context_map.insert("is_metadata_request".to_string(), "true".to_string());
        let request_context = &RequestContext::Some(context_map);

        while offset < buf.len() {
            let (record_batch, batch_byte_len) = match RecordBatch::decode(&buf[offset..], request_context) {
                Ok( (record_batch, batch_byte_len) ) => {
                    (record_batch, batch_byte_len)
                }
                Err(_) => {
                    return Err(BrokerError::UnknownError);
                }
            };

            record_batches.push(record_batch);
            offset += batch_byte_len;
        }

        // iterate over record values from record batches
        let topic_name_to_uuid: HashMap<String, Uuid> = record_batches
            .iter()
            .flat_map(|record_batch| {
                record_batch.records.iter().filter_map(|metadata_record| {
                    match &metadata_record.value {
                        RecordValue::TopicRecord(topic_record) => {
                            Some((topic_record.topic_name.data.clone(), topic_record.topic_id))
                        }
                        _ => None,
                    }
                })
            })
            .collect();

        for (topic_name, uuid) in &topic_name_to_uuid {
            println!("Topic Name: {}, UUID: {}", topic_name, uuid);
        }

        let mut topic_uuid_to_partitions: HashMap<Uuid, Vec<&PartitionRecord>> = HashMap::new();
        for record_batch in &record_batches {
            for metadata_record in &record_batch.records {
                if let RecordValue::PartitionRecord(partition_record) = &metadata_record.value {
                    topic_uuid_to_partitions
                        .entry(partition_record.topic_id)
                        .or_insert_with(Vec::new)
                        .push(partition_record);
                }
            }
        }

        for (uuid, temp_partitions) in &topic_uuid_to_partitions {
            println!("Topic UUID: {}, #Partitions: {}", uuid, temp_partitions.len());
        }

        // create response
        let mut response_topics: Vec<ResponseTopic> = Vec::new();

        for request_topic in &self.topics.data {
            let request_topic_name = request_topic.name.data.clone();

            // create response placeholders
            let request_topic_uuid: Uuid = "00000000-0000-0000-0000-000000000000".parse::<Uuid>().unwrap();
            let mut response_topic = ResponseTopic {
                error_code: 3,
                name: CompactNullableString {
                    data: Some( CompactString { data: request_topic_name.clone() } )
                },
                topic_id: request_topic_uuid,
                is_internal: false,
                partitions: CompactArray { data: vec![] },
                topic_authorized_operations: 0,
                tagged_fields: TaggedFields(None)
            };
            
            match topic_name_to_uuid.get(&request_topic_name) {
                Some(topic_uuid) => {
                    response_topic.error_code = 0;
                    response_topic.topic_id = topic_uuid.clone();
                    
                    match topic_uuid_to_partitions.get(topic_uuid) {
                        Some(partitions) => {
                            let mut response_partitions: Vec<PartitionMetadata> = Vec::new();
                            for partition in partitions {
                                let response_partition = PartitionMetadata {
                                    error_code: 0,
                                    partition_index: partition.partition_id,
                                    leader_id: partition.leader,
                                    leader_epoch: partition.leader_epoch,
                                    replica_nodes: partition.replica_array.data.clone(),
                                    isr_nodes: partition.isr_array.data.clone(),
                                    eligible_leader_replicas: vec![],
                                    last_known_elr: vec![],
                                    offline_replicas: vec![],
                                    tagged_fields: TaggedFields(None),
                                };

                                response_partitions.push(response_partition);
                            }

                            response_topic.partitions = CompactArray { data: response_partitions };
                        }
                        None => {}
                    };
                },
                None => {
                    response_topic.error_code = 3;
                },
            };

            response_topics.push(response_topic);
        }

        // craft final response
        let response_body = KafkaBody::Response(Box::new(
            DescribeTopicPartitionsResponse {
                throttle_time_ms: 0,
                topics: CompactArray { data: response_topics },
                next_cursor: None,
                tagged_fields: TaggedFields(None),
            }
        ));

        Ok( response_body )
    }
}

impl RequestProcess for ApiVersionsRequest {
    fn process(&self) -> Result<KafkaBody, BrokerError> {
        println!("Processing ApiVersionsRequest...");

        // create response
        let response_body = KafkaBody::Response(Box::new(
            ApiVersionsResponse {
                error_code: 0,
                api_versions: get_all_apis().iter()
                                .map(|&(api_key, (min_version, max_version))| ApiKey {
                                    api_key,
                                    min_version,
                                    max_version,
                                    tagged_fields: TaggedFields(None),
                                }).collect(),
                throttle_time_ms: 0,
                tagged_fields: TaggedFields(None),
        }));

        Ok(response_body)
    }
}

impl RequestProcess for FetchRequest {
    fn process(&self) -> Result<KafkaBody, BrokerError> {
        println!("Processing FetchRequest...");

        if self.topics.data.len() == 0 {
            return Ok( KafkaBody::Response(Box::new(FetchResponse::empty())) )
        }

        // FIXME: Implement OOP for record batch reading
        // read record batches from log files
        // open cluster metadata file
        let metadata_file_path = Path::new("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
        let mut metadata_file = File::open(&metadata_file_path).map_err(|_| BrokerError::UnknownError)?;

        // decode cluster metadata
        let mut buf: Vec<u8> = Vec::new();
        metadata_file.read_to_end(&mut buf).map_err(|_| BrokerError::UnknownError)?;

        println!("Initial Buffer length: {:?}", buf.len());
        println!("Decoding record batch...");

        let mut metadata_record_batches: Vec<RecordBatch> = Vec::new();
        let mut offset = 0;

        let mut context_map: HashMap<String, String> = HashMap::new();
        context_map.insert("is_metadata_request".to_string(), "true".to_string());
        let request_context = &RequestContext::Some(context_map);

        while offset < buf.len() {
            let (record_batch, batch_byte_len) = match RecordBatch::decode(&buf[offset..], request_context) {
                Ok( (record_batch, batch_byte_len) ) => {
                    (record_batch, batch_byte_len)
                }
                Err(_) => {
                    return Err(BrokerError::UnknownError);
                }
            };

            metadata_record_batches.push(record_batch);
            offset += batch_byte_len;
        }

        // iterate over record values from record batches
        let topic_name_to_uuid: HashMap<String, Uuid> = metadata_record_batches
            .iter()
            .flat_map(|record_batch| {
                record_batch.records.iter().filter_map(|metadata_record| {
                    match &metadata_record.value {
                        RecordValue::TopicRecord(topic_record) => {
                            Some((topic_record.topic_name.data.clone(), topic_record.topic_id))
                        }
                        _ => None,
                    }
                })
            })
            .collect();

        for (topic_name, uuid) in &topic_name_to_uuid {
            println!("Topic Name: {}, UUID: {}", topic_name, uuid);
        }

        let mut topic_uuid_to_partitions: HashMap<Uuid, Vec<&PartitionRecord>> = HashMap::new();
        for record_batch in &metadata_record_batches {
            for metadata_record in &record_batch.records {
                if let RecordValue::PartitionRecord(partition_record) = &metadata_record.value {
                    topic_uuid_to_partitions
                        .entry(partition_record.topic_id)
                        .or_insert_with(Vec::new)
                        .push(partition_record);
                }
            }
        }

        for (uuid, temp_partitions) in &topic_uuid_to_partitions {
            println!("Topic UUID: {}, #Partitions: {}", uuid, temp_partitions.len());
        }

        let mut response = FetchResponse::empty();

        // check topic existence
        for topic in &self.topics.data {
            let topic_id = topic.topic_id;
            let mut response_topic = FetchResponseTopic {
                topic_id: topic_id,
                partitions: CompactArray { data: vec![] },
                tagged_fields: TaggedFields(None),
            };

            if !topic_uuid_to_partitions.contains_key(&topic_id) {
                let partition = FetchResponsePartition {
                    partition_index: 0,
                    error_code: 100, 
                    high_watermark: 0,
                    last_stable_offset: 0,
                    log_start_offset: 0,
                    aborted_transactions: CompactArray { data: vec![] },
                    preferred_read_replica: -1,
                    records: CompactArray { data: vec![] },
                    tagged_fields: TaggedFields(None),
                };

                response_topic.partitions.data.push(partition);
            } else {
                // topic exists
                // go to the topic partition folder
                // find the log file and read the records
                // construct the response

                // find topic name from UUID
                let topic_name = topic_name_to_uuid.iter()
                    .find(|(_, &uuid)| uuid == topic_id)
                    .map(|(name, _)| name)
                    .unwrap();

                // find number of available partitions
                let num_partitions = topic_uuid_to_partitions.get(&topic_id).unwrap().len();

                let mut partitions_to_fetch: Vec<i32> = Vec::new();
                for p in &topic.partitions.data {
                    partitions_to_fetch.push(p.partition);
                }

                for i in 0..num_partitions {

                    if !partitions_to_fetch.contains(&(i as i32)) {
                        continue;
                    }

                    let log_file_path_str = format!("/tmp/kraft-combined-logs/{}/00000000000000000000.log", topic_name.to_owned()+"-"+&i.to_string());
                    let log_file_path = Path::new(&log_file_path_str);
                    let mut log_file = File::open(log_file_path).map_err(|_| BrokerError::UnknownError)?;

                    // decode log file
                    let mut log_buf: Vec<u8> = Vec::new();
                    log_file.read_to_end(&mut log_buf).map_err(|_| BrokerError::UnknownError)?;

                    println!("Initial Log Buffer length: {:?}", log_buf.len());
                    println!("Decoding record batch...");

                    let mut topic_record_batches: Vec<RecordBatch> = Vec::new();
                    let mut offset = 0;

                    let mut context_map: HashMap<String, String> = HashMap::new();
                    context_map.insert("is_metadata_request".to_string(), "false".to_string());
                    let request_context = &RequestContext::Some(context_map);

                    while offset < log_buf.len() {
                        let (record_batch, batch_byte_len) = match RecordBatch::decode(&log_buf[offset..], request_context) {
                            Ok( (record_batch, batch_byte_len) ) => {
                                (record_batch, batch_byte_len)
                            }
                            Err(_) => {
                                return Err(BrokerError::UnknownError);
                            }
                        };

                        topic_record_batches.push(record_batch);
                        offset += batch_byte_len;
                    }

                    let mut partition = FetchResponsePartition {
                        partition_index: i as i32,
                        error_code: 0, 
                        high_watermark: 0,
                        last_stable_offset: 0,
                        log_start_offset: 0,
                        aborted_transactions: CompactArray { data: vec![] },
                        preferred_read_replica: -1,
                        records: CompactArray { data: vec![] },
                        tagged_fields: TaggedFields(None),
                    };

                    println!("\n\n\n\n\n\n\n Number of record batches: {:?} \n\n\n\n\n\n", topic_record_batches.len());
                    if topic_record_batches.len() > 0 {
                        for record_batch in topic_record_batches {
                            partition.records.data.push(record_batch);
                        }
                    }

                    response_topic.partitions.data.push(partition);

                }

            }

            response.responses.data.push(response_topic);
            
        }

        Ok( KafkaBody::Response(Box::new(response)) )

        // Err(BrokerError::UnknownError)
    }
}
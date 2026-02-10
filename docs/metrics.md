## Weaviate 指标

本文档是 Weaviate 暴露的 Prometheus 指标的唯一权威来源。它解释了我们测量的内容和原因、如何使用这些指标，以及我们如何保持指标集的精简和成本效益。

### 目的

- 提供指标的规范列表、其含义和预期用途
- 标准化团队如何解释和构建仪表板/告警
- 通过将运营需求与分析需求分离来控制成本和标签基数

### 权威来源

- 此文件（`docs/metrics.md`）具有权威性。任何指标变更（添加/修改/废弃）都必须在此处的正确部分反映出来
- 此处的类别和使用状态定义了指标应该存在的位置以及应该如何使用

### 使用类别

- 🎯 活跃（仪表板）：适合仪表板的核心指标；使用稳定、有界的标签
- ⚙️ 活跃（运营）：健康/运行状态和后台进程；尽可能采样
- 🚨 告警：最小化、基于症状的告警，标签基数低
- 📊 分析（可能移出 Prometheus）：调试/分析；避免在 Prometheus 中长期保留/高基数
- ‼️ 可废弃：废弃候选；使用者应迁移出去
- 🗑️ 已废弃：已从代码库中移除；记录一个发布周期以帮助迁移；从仪表板/告警中移除并删除记录规则

### 成本和基数指导原则

- 优先使用带有少量有界标签集的计数器/仪表
- 避免每租户/每类/每路由的标签爆炸，除非对运营至关重要
- 将探索性或宽标签分析移到日志、追踪或外部存储中

### 变更管理

- 添加：包括类型、标签、类别和标签理由
- 更改标签：指出基数影响和迁移步骤
- 废弃：移至 ‼️ 可废弃，在一个小版本中保持，然后移除
- 告警：在仪表板中记录阈值和运行手册链接，不在此处

---

### 🎯 Active (dashboard)

#### Batch Operations
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `batch_durations_ms` | Duration in ms of a single batch | `Histogram` | `class_name, operation, shard_name` | ❌ High 

#### Object Operations
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `object_count` | Number of currently ongoing async operations | `Gauge` | `class_name, shard_name` | ❌ High 

#### Query Operations
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `concurrent_queries_count` | Number of concurrently running query operations | `Gauge` | `class_name, query_type` | ❌ High 
| `requests_total` | Number of all requests made | `Gauge` | `api, class_name, query_type, status` | ❌ High 
| `queries_durations_ms` | Duration of queries in milliseconds | `Histogram` | `class_name, query_type` | ❌ High 
| `queries_filtered_vector_durations_ms` | Duration of queries in milliseconds | `Summary` | `class_name, operation, shard_name` | ❌ High 
| `query_dimensions_total` | Vector dimensions used by read-queries involving vectors | `Counter` | `class_name, operation, query_type` | ❌ High 

#### LSM Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `lsm_active_segments` | Number of currently present segments per shard | `Gauge` | `class_name, path, shard_name, strategy` | ❌ High 
| `lsm_memtable_size` | Size of memtable by path | `Gauge` | `class_name, path, shard_name, strategy` | ❌ High 

#### System Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `async_operations_running` | Number of currently ongoing async operations | `Gauge` | `class_name, operation, path, shard_name` | ❌ High 

#### Queue Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `queue_size` | Number of records in the queue | `Gauge` | `class_name, shard_name` | ❌ High 

#### Vector Index Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `vector_index_tombstones` | Number of active vector index tombstones | `Gauge` | `class_name, shard_name` | ❌ High 
| `vector_index_tombstone_cleaned` | Total number of deleted objects that have been cleaned up | `Counter` | `class_name, shard_name` | ❌ High 
| `vector_index_tombstone_unexpected_total` | Total number of unexpected tombstones found | `Counter` | `class_name, operation, shard_name` | ❌ High 
| `vector_index_operations` | Total number of mutating operations on the vector index | `Gauge` | `class_name, operation, shard_name` | ❌ High 
| `vector_index_size` | The size of the vector index | `Gauge` | `class_name, shard_name` | ❌ High 
| `vector_segments_sum` | Total segments in a shard if quantization enabled | `Gauge` | `class_name, shard_name` | ❌ High 
| `vector_dimensions_sum` | Total dimensions in a shard | `Gauge` | `class_name, shard_name` | ❌ High 
| `vector_index_durations_ms` | Duration of typical vector index operations (insert, delete) | `Summary` | `class_name, operation, shard_name, step` | ❌ High 

#### Startup Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `startup_progress` | Ratio (percentage) of startup progress for a particular component in a shard | `Gauge` | `class_name, operation, shard_name` | ❌ High 
| `startup_diskio_throughput` | Disk I/O throughput in bytes per second | `Summary` | `class_name, operation, shard_name` | ❌ High 

#### Tombstone Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `tombstone_find_local_entrypoint` | Total number of tombstone delete local entrypoint calls | `Counter` | `class_name, shard_name` | ❌ High 
| `tombstone_find_global_entrypoint` | Total number of tombstone delete global entrypoint calls | `Counter` | `class_name, shard_name` | ❌ High 

#### Text-to-Vector (T2V) Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `t2v_concurrent_batches` | Number of batches currently running | `Gauge` | `vectorizer` | - Low 
| `t2v_batch_queue_duration_seconds` | Time of a batch spent in specific portions of the queue | `Histogram` | `operation, vectorizer` | - Low 
| `t2v_request_duration_seconds` | Duration of an individual request to the vectorizer | `Histogram` | `vectorizer` | - Low 
| `t2v_tokens_in_batch` | Number of tokens in a user-defined batch | `Histogram` | `vectorizer` | - Low 
| `t2v_tokens_in_request` | Number of tokens in an individual request sent to the vectorizer | `Histogram` | `vectorizer` | - Low 
| `t2v_rate_limit_stats` | Rate limit stats for the vectorizer | `Gauge` | `stat, vectorizer` | - Low 
| `t2v_repeat_stats` | Why batch scheduling is repeated | `Gauge` | `stat, vectorizer` | - Low 
| `t2v_requests_per_batch` | Number of requests required to process an entire (user) batch | `Histogram` | `vectorizer` | - Low 

#### Index Shard Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `weaviate_index_shards_total` | Total number of shards per index status | `Gauge` | `status` | - Low 
| `weaviate_index_shard_status_update_duration_seconds` | Time taken to update shard status in seconds | `Histogram` | `status` | - Low 

#### Auto Schema Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `weaviate_auto_tenant_total` | Total number of tenants processed | `Counter` | `-` | - Low 
| `weaviate_auto_tenant_duration_seconds` | Time spent in auto tenant operations | `Histogram` | `operation` | - Low 

---

### ⚙️ Active (operational)

#### Vector Index Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `vector_index_tombstone_cycle_end_timestamp_seconds` | Unix epoch timestamp of the end of the last tombstone cleanup cycle | `Gauge` | `class_name, shard_name` | ❌ High 
| `vector_index_tombstone_cycle_progress` | Ratio (percentage) of the progress of the current tombstone cleanup cycle | `Gauge` | `class_name, shard_name` | ❌ High 

#### Tenant Offload Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `tenant_offload_operation_duration_seconds` | Duration of tenant offload operations | `Histogram` | `operation, status` | ❌ High 

#### Module Usage Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `weaviate_<module>_operation_latency_seconds` | Latency of usage operations in seconds | `Histogram` | `operation` | - Low 
| `weaviate_<module>_uploaded_file_size_bytes` | Size of the last uploaded usage file in bytes | `Gauge` | `-` | - Low 

#### Shard Load Limiter Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `database_shards_loading` | Number of shards currently loading | `Gauge` | `-` | - Low 
| `database_shards_waiting_for_permit_to_load` | Number of shards waiting for permit to load | `Gauge` | `-` | - Low 

#### Replication Engine Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `weaviate_replication_pending_operations` | Number of replication operations pending processing | `Gauge` | `node` | - Low 
| `weaviate_replication_ongoing_operations` | Number of replication operations currently in progress | `Gauge` | `node` | - Low 
| `weaviate_replication_complete_operations` | Number of successfully completed replication operations | `Counter` | `node` | - Low 
| `weaviate_replication_failed_operations` | Number of failed replication operations | `Counter` | `node` | - Low 
| `weaviate_replication_cancelled_operations` | Number of cancelled replication operations | `Counter` | `node` | - Low 
| `weaviate_replication_engine_running_status` | Replication engine running status (0:not running, 1:running) | `Gauge` | `node` | - Low 
| `weaviate_replication_engine_producer_running_status` | Replication engine producer running status (0:not running, 1:running) | `Gauge` | `node` | - Low 
| `weaviate_replication_engine_consumer_running_status` | Replication engine consumer running status (0:not running, 1:running) | `Gauge` | `node` | - Low 

#### Distributed Task Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `weaviate_distributed_tasks_running` | Number of active distributed tasks running per namespace | `Gauge` | `namespace` | ❌ High 

#### HTTP Server Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `http_request_duration_seconds` | Time (in seconds) spent serving requests | `Histogram` | `method, route, status_code` | ❌ High 
| `http_request_size_bytes` | Size (in bytes) of the request received | `Histogram` | `method, route` | ❌ High 
| `http_response_size_bytes` | Size (in bytes) of the response sent | `Histogram` | `method, route` | ❌ High 
| `http_requests_inflight` | Current number of inflight requests | `Gauge` | `method, route` | ❌ High 

#### gRPC Server Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `grpc_server_request_duration_seconds` | Time (in seconds) spent serving requests | `Histogram` | `grpc_service, method, status` | ❌ High 
| `grpc_server_request_size_bytes` | Size (in bytes) of the request received | `Histogram` | `grpc_service, method` | ❌ High 
| `grpc_server_response_size_bytes` | Size (in bytes) of the response sent | `Histogram` | `grpc_service, method` | ❌ High 
| `grpc_server_requests_inflight` | Current number of inflight requests | `Gauge` | `grpc_service, method` | ❌ High 

#### Cluster Store Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `weaviate_cluster_store_fsm_apply_duration_seconds` | Time to apply cluster store FSM state in local node | `Histogram` | `nodeID` | - Low 
| `weaviate_cluster_store_fsm_apply_failures_total` | Total failure count of cluster store FSM state apply in local node | `Counter` | `nodeID` | - Low 
| `weaviate_cluster_store_raft_last_applied_index` | Current applied index of a raft cluster in local node | `Gauge` | `nodeID` | - Low 
| `weaviate_cluster_store_fsm_last_applied_index` | Current applied index of cluster store FSM in local node | `Gauge` | `nodeID` | - Low 
| `weaviate_cluster_store_fsm_startup_applied_index` | Previous applied index of the cluster store FSM in local node | `Gauge` | `nodeID` | - Low 

#### Schema Management Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `weaviate_schema_collections` | Number of collections per node | `Gauge` | `nodeID` | - Low 
| `weaviate_schema_shards` | Number of shards per node with corresponding status | `Gauge` | `nodeID, status` | - Low 

#### Runtime Config Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `weaviate_runtime_config_last_load_success` | Whether the last loading attempt of runtime config was success | `Gauge` | `-` | - Low 
| `weaviate_runtime_config_hash` | Hash value of the currently active runtime configuration | `Gauge` | `sha256` | - Low 

---

### 🚨 Alerting

#### Query Operations
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `queries_durations_ms` | Duration of queries in milliseconds | `Histogram` | `class_name, query_type` | ❌ High 

---

### 📊 Analytical (could be moved out of Prometheus)

#### Vector Index Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `vector_index_maintenance_durations_ms` | Duration of a sync or async vector index maintenance operation | `Summary` | `class_name, operation, shard_name` | ❌ High 

#### Module Usage Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `weaviate_<module>_operations_total` | Total number of module operations | `Counter` | `operation, status` | - Low 
| `weaviate_<module>_resource_count` | Number of resources tracked by module | `Gauge` | `resource_type` | - Low 

---

### 🐛 Active (debugging)

#### Batch Operations
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `batch_size_bytes` | Size of a raw batch request batch in bytes | `Summary` | `api` | - Low 
| `batch_size_objects` | Number of objects in a batch | `Summary` | `-` | - Low 
| `batch_size_tenants` | Number of unique tenants referenced in a batch | `Summary` | `-` | - Low 
| `batch_delete_durations_ms` | Duration in ms of a single delete batch | `Summary` | `class_name, operation, shard_name` | ❌ High 
| `batch_objects_processed_total` | Number of objects processed in a batch | `Counter` | `class_name, shard_name` | ❌ High 
| `batch_objects_processed_bytes` | Number of bytes processed in a batch | `Counter` | `class_name, shard_name` | ❌ High 

#### LSM Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `lsm_bitmap_buffers_usage` | Number of bitmap buffers used by size | `Counter` | `operation, size` | - Low 

#### File I/O Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `file_io_writes_total_bytes` | Total number of bytes written to disk | `Summary` | `operation, strategy` | - Low 
| `file_io_reads_total_bytes` | Total number of bytes read from disk | `Summary` | `operation` | - Low 
| `mmap_operations_total` | Total number of mmap operations | `Counter` | `operation, strategy` | - Low 
| `mmap_proc_maps` | Number of entries in /proc/self/maps | `Gauge` | `-` | - Low 

#### Schema Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `schema_writes_seconds` | Duration of schema writes (which always involve the leader) | `Summary` | `type` | - Low 
| `schema_reads_local_seconds` | Duration of local schema reads that do not involve the leader | `Summary` | `type` | - Low 
| `schema_reads_leader_seconds` | Duration of schema reads that are passed to the leader | `Summary` | `type` | - Low 
| `schema_wait_for_version_seconds` | Duration of waiting for a schema version to be reached | `Summary` | `type` | - Low 

---

### ‼️ Can be deprecated

#### Object Operations
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `objects_durations_ms` | Duration of an individual object operation | `Summary` | `class_name, operation, shard_name, step` | ❌ High 

#### Query Operations
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `query_dimensions_combined_total` | Vector dimensions used by read-queries, aggregated across all classes and shards | `Counter` | `-` | - Low 

#### System Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `concurrent_goroutines` | Number of concurrently running goroutines | `Gauge` | `class_name, query_type` | ❌ High 

#### LSM Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `lsm_objects_bucket_segment_count` | Number of segments per shard in the objects bucket | `Gauge` | `class_name, path, shard_name, strategy` | ❌ High 
| `lsm_compressed_vecs_bucket_segment_count` | Number of segments per shard in the vectors_compressed bucket | `Gauge` | `class_name, path, shard_name, strategy` | ❌ High 
| `lsm_segment_objects` | Number of objects/entries of segment by level | `Gauge` | `class_name, level, path, shard_name, strategy` | ❌ High 
| `lsm_segment_size` | Size of segment by level and unit | `Gauge` | `class_name, level, path, shard_name, strategy, unit` | ❌ High 
| `lsm_segment_count` | Number of segments by level | `Gauge` | `class_name, level, path, shard_name, strategy` | ❌ High 
| `lsm_segment_unloaded` | Number of unloaded segments | `Gauge` | `class_name, path, shard_name, strategy` | ❌ High 
| `lsm_memtable_durations_ms` | Time in ms for a bucket operation to complete | `Summary` | `class_name, operation, path, shard_name, strategy` | ❌ High 

#### Queue Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `queue_disk_usage` | Disk usage of the queue | `Gauge` | `class_name, shard_name` | ❌ High 
| `queue_paused` | Whether the queue is paused | `Gauge` | `class_name, shard_name` | ❌ High 
| `queue_count` | Number of queues | `Gauge` | `class_name, shard_name` | ❌ High 
| `queue_partition_processing_duration_ms` | Duration in ms of a single partition processing | `Histogram` | `class_name, shard_name` | ❌ High 

#### Vector Index Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `vector_index_queue_insert_count` | Number of insert operations added to the vector index queue | `Counter` | `class_name, shard_name, target_vector` | ❌ High 
| `vector_index_queue_delete_count` | Number of delete operations added to the vector index queue | `Counter` | `class_name, shard_name, target_vector` | ❌ High 
| `vector_index_tombstone_cleanup_threads` | Number of threads in use to clean up tombstones | `Gauge` | `class_name, shard_name` | ❌ High 
| `vector_index_tombstone_cycle_start_timestamp_seconds` | Unix epoch timestamp of the start of the current tombstone cleanup cycle | `Gauge` | `class_name, shard_name` | ❌ High 

#### Startup Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `startup_durations_ms` | Duration of individual startup operations in ms | `Summary` | `class_name, operation, shard_name` | ❌ High 

#### Backup/Restore Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `backup_restore_ms` | Duration of a backup restore | `Summary` | `backend_name, class_name` | ❌ High 
| `backup_restore_class_ms` | Duration restoring class | `Summary` | `class_name` | ❌ High 
| `backup_restore_init_ms` | Startup phase of a backup restore | `Summary` | `backend_name, class_name` | ❌ High 
| `backup_restore_from_backend_ms` | File transfer stage of a backup restore | `Summary` | `backend_name, class_name` | ❌ High 
| `backup_store_to_backend_ms` | File transfer stage of a backup store | `Summary` | `backend_name, class_name` | ❌ High 
| `bucket_pause_durations_ms` | Bucket pause durations | `Summary` | `bucket_dir` | - Low 
| `backup_restore_data_transferred` | Total number of bytes transferred during a backup restore | `Counter` | `backend_name, class_name` | ❌ High 
| `backup_store_data_transferred` | Total number of bytes transferred during a backup store | `Counter` | `backend_name, class_name` | ❌ High 

#### Shard Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `shards_loaded` | Number of shards loaded | `Gauge` | `-` | - Low 
| `shards_unloaded` | Number of shards not loaded | `Gauge` | `-` | - Low 
| `shards_loading` | Number of shards in process of loading | `Gauge` | `-` | - Low 
| `shards_unloading` | Number of shards in process of unloading | `Gauge` | `-` | - Low 

#### Schema Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `schema_tx_opened_total` | Total number of opened schema transactions | `Counter` | `ownership` | - Low 
| `schema_tx_closed_total` | Total number of closed schema transactions | `Counter` | `ownership, status` | - Low 
| `schema_tx_duration_seconds` | Mean duration of a tx by status | `Summary` | `ownership, status` | - Low 

#### Tombstone Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `tombstone_reassign_neighbors` | Total number of tombstone reassign neighbor calls | `Counter` | `class_name, shard_name` | ❌ High 
| `tombstone_delete_list_size` | Delete list size of tombstones | `Gauge` | `class_name, shard_name` | ❌ High 

#### Tokenizer Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `tokenizer_duration_seconds` | Duration of a tokenizer operation | `Histogram` | `tokenizer` | - Low 
| `tokenizer_requests_total` | Number of tokenizer requests | `Counter` | `tokenizer` | - Low 
| `tokenizer_initialize_duration_seconds` | Duration of a tokenizer initialization operation | `Histogram` | `tokenizer` | - Low 
| `token_count_total` | Number of tokens processed | `Counter` | `tokenizer` | - Low 
| `token_count_per_request` | Number of tokens processed per request | `Histogram` | `tokenizer` | - Low 

#### Module/External API Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `weaviate_module_requests_total` | Number of module requests to external APIs | `Counter` | `api, op` | ❌ High 
| `weaviate_module_request_duration_seconds` | Duration of an individual request to a module external API | `Histogram` | `api, op` | ❌ High 
| `weaviate_module_requests_per_batch` | Number of items in a batch | `Histogram` | `api, op` | ❌ High 
| `weaviate_module_request_size_bytes` | Size (in bytes) of the request sent to an external API | `Histogram` | `api, op` | ❌ High 
| `weaviate_module_response_size_bytes` | Size (in bytes) of the response received from an external API | `Histogram` | `api, op` | ❌ High 
| `weaviate_vectorizer_request_tokens` | Number of tokens in the request sent to an external vectorizer | `Histogram` | `api, inout` | ❌ High 
| `weaviate_module_request_single_count` | Number of single-item external API requests | `Counter` | `api, op` | ❌ High 
| `weaviate_module_request_batch_count` | Number of batched module requests | `Counter` | `api, op` | ❌ High 
| `weaviate_module_error_total` | Number of OpenAI errors | `Counter` | `endpoint, module, op, status_code` | ❌ High 
| `weaviate_module_call_error_total` | Number of module errors (related to external calls) | `Counter` | `endpoint, module, status_code` | ❌ High 
| `weaviate_module_response_status_total` | Number of API response statuses | `Counter` | `endpoint, op, status` | ❌ High 
| `weaviate_module_batch_error_total` | Number of batch errors | `Counter` | `class_name, operation` | ❌ High 

#### Tenant Offload Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `tenant_offload_fetched_bytes_total` | Total bytes fetched during tenant offload operations | `Counter` | `-` | - Low 
| `tenant_offload_transferred_bytes_total` | Total bytes transferred during tenant offload operations | `Counter` | `-` | - Low 

#### Checksum Metrics
| Name | Description | Type | Labels | High Cardinality |
|---|---|---|---|---|
| `checksum_validation_duration_seconds` | Duration of checksum validation | `Summary` | `-` | - Low 
| `checksum_bytes_read` | Number of bytes read during checksum validation | `Summary` | `-` | - Low 



---

### 🗑️ Deprecated

| Name | Description | Type | Labels | Reason | Removed In |
|---|---|---|---|---|---|
| `lsm_bloom_filters_duration_ms` | Duration of bloom filter operations | `Summary` | `class_name, operation, shard_name, strategy` | Removed due to high CPU cost and synchronization on hot path during segment reads; no demonstrated value | v1.31 ([PR #9057](https://github.com/weaviate/weaviate/pull/9057)) |

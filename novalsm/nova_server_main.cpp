
//
// Created by Haoyu Huang on 2/20/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
// NovaLSM main class.


#include "rdma/rdma_ctrl.hpp"
#include "common/nova_common.h"
#include "common/nova_config.h"
#include "nic_server.h"
#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "ltc/storage_selector.h"
#include "ltc/db_migration.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <gflags/gflags.h>
#include "db/version_set.h"

using namespace std;
using namespace rdmaio;
using namespace nova;

DEFINE_string(db_path, "/tmp/db", "level db path");// leveldb的路径??? 之前是/db/nova-db-recordcount-valuesize 现在是~/db_files/nova-db-recordcount-valuesize
DEFINE_string(stoc_files_path, "/tmp/stoc", "StoC files path");//stoc文件的路径 之前是/db/stoc_files 现在是~/db_files/stoc_files pm不会根据这个改 因为这个最后毫无用处
DEFINE_string(pm_path, "/tmp/pm", "pm files path");//用于存放pm文件的路径
DEFINE_int64(levels_in_pm, 0, "this and lower levels should be held in pm");// 这层及以以下的level应该存放于pm之中

DEFINE_string(all_servers, "localhost:11211", "A list of servers");//server的list
DEFINE_int64(server_id, -1, "Server id.");//当前这个server的id
DEFINE_int64(number_of_ltcs, 0, "The first n are LTCs and the rest are StoCs.");//ltc的数量，剩下的都是stoc 这个没有意义

DEFINE_uint64(mem_pool_size_gb, 0, "Memory pool size in GB."); //内存池大小，GB为单位 一般申请30个g 那pm也申请30个G算了
DEFINE_uint64(pm_pool_size_gb, 0, "Persistent Memory pool size in GB"); //pm池子大小，GB为单位 似乎不需要用平常的模式pm send

DEFINE_uint64(use_fixed_value_size, 0, "Fixed value size.");//使用的value的固定大小

DEFINE_uint64(rdma_port, 0, "The port used by RDMA.");//rdma用的端口
DEFINE_uint64(rdma_max_msg_size, 0, "The maximum message size used by RDMA.");//最大的rdma message的大小 一般是256 * 1024 256KB
DEFINE_uint64(rdma_max_num_sends, 0,
              "The maximum number of pending RDMA sends. This includes READ/WRITE/SEND. We also post the same number of RECV events. ");//最大的rdma pending请求的数量 32 和读一起的话就是64
DEFINE_uint64(rdma_doorbell_batch_size, 0, "The doorbell batch size.");//rdma的batch size
DEFINE_bool(enable_rdma, false, "Enable RDMA.");//是否开启rdma
DEFINE_bool(enable_load_data, false, "Enable loading data.");//是否开启传输数据

DEFINE_string(ltc_config_path, "/tmp/uniform-3-32-10000000-frags.txt",
              "The path that stores the configuration.");//ltc分布的config
DEFINE_uint64(ltc_num_client_workers, 0, "Number of client worker threads.");//ltc的线程数量
DEFINE_uint32(num_rdma_fg_workers, 0,//rdma的前台线程数量
              "Number of RDMA foreground worker threads.");
DEFINE_uint32(num_compaction_workers, 0,//compaction的线程数量
              "Number of compaction worker threads.");
DEFINE_uint32(num_rdma_bg_workers, 0,//rdma后台线程数量
              "Number of RDMA background worker threads.");

DEFINE_uint32(num_storage_workers, 0,//存储线程数量
              "Number of storage worker threads.");
DEFINE_uint32(ltc_num_stocs_scatter_data_blocks, 0,//一个sstable的内容要散落到多少stoc里面
              "Number of StoCs to scatter data blocks of an SSTable.");

DEFINE_uint64(block_cache_mb, 0, "block cache size in mb");//block的cache大小 MB为单位
DEFINE_uint64(row_cache_mb, 0, "row cache size in mb. Not supported");//?

DEFINE_uint32(num_memtables, 0, "Number of memtables.");//memtable数量
DEFINE_uint32(num_memtable_partitions, 0,//??????
              "Number of memtable partitions. One active memtable per partition.");
DEFINE_bool(enable_lookup_index, false, "Enable lookup index.");//是否开启lookup_index
DEFINE_bool(enable_range_index, false, "Enable range index.");//是否开启range index

DEFINE_uint32(l0_start_compaction_mb, 0,
              "Level-0 size to start compaction in MB.");//level0开启压缩的大小 MB
DEFINE_uint32(l0_stop_write_mb, 0, "Level-0 size to stall writes in MB.");//level0需要引起写暂停的大小 MB
DEFINE_int32(level, 2, "Number of levels.");//level的数量

DEFINE_uint64(memtable_size_mb, 0, "memtable size in mb");//memtable的大小 MB
DEFINE_uint64(sstable_size_mb, 0, "sstable size in mb");//sstable的大小 MB
DEFINE_uint32(cc_log_buf_size, 0,
              "log buffer size. Not supported. Same as memtable size.");//??
DEFINE_uint32(max_stoc_file_size_mb, 0, "Max StoC file size in MB");//stoc文件的最大的大小 MB为单位
DEFINE_bool(use_local_disk, false,
            "Enable LTC to write data to its local disk.");//ltc是否可以使用本地的硬盘
DEFINE_string(scatter_policy, "random",
              "Policy to scatter an SSTable, i.e., random/power_of_two");//打散一个sstable的策略
DEFINE_string(log_record_mode, "none",
              "Policy for LogC to replicate log records, i.e., none/rdma");//log的记录策略
DEFINE_string(log_type, "dram",
              "Where to put log file, dram or pm or ssd");//log的记录策略
DEFINE_int64(batch_size, 1,
              "number of log records to process at a time");//每次发送的记录数量 预设为1
DEFINE_uint32(num_log_replicas, 0, "Number of replicas for a log record.");//一条log记录的replica个数
DEFINE_string(memtable_type, "", "Memtable type, i.e., pool/static_partition");//memtable的种类

DEFINE_bool(recover_dbs, false, "Enable recovery");//是否开启recovery
DEFINE_uint32(num_recovery_threads, 32, "Number of recovery threads");//recovery线程数量

DEFINE_bool(enable_subrange, false, "Enable subranges");//是否开启subrange
DEFINE_bool(enable_subrange_reorg, false, "Enable subrange reorganization.");//是否开启subrange的重新组织
DEFINE_double(sampling_ratio, 1,
              "Sampling ratio on memtables for subrange reorg. A value between 0 and 1.");//为了重新组织subrange，要使用的采样率
DEFINE_string(zipfian_dist_ref_counts, "/tmp/zipfian",
              "Zipfian ref count file used to report load imbalance across subranges.");//zipfian的引用计数文件??用来算subrange之间的负载均衡
DEFINE_string(client_access_pattern, "uniform",
              "Client access pattern used to report load imbalance across subranges.");//用户访问模式??
DEFINE_uint32(num_tinyranges_per_subrange, 10,
              "Number of tiny ranges per subrange.");//一个subrange中tinyrange的数量

DEFINE_bool(enable_detailed_db_stats, false,
            "Enable detailed stats. It will report stats such as number of overlapping SSTables between Level-0 and Level-1.");//是否开启详细的stats数据
DEFINE_bool(enable_flush_multiple_memtables, false,
            "Enable a compaction thread to compact mulitple memtables at the same time.");//一个compaction线程是否可以同时压缩多个memtable
DEFINE_uint32(subrange_no_flush_num_keys, 100,
              "A subrange merges memtables into new a memtable if its contained number of unique keys is less than this threshold.");//如果一个subrange中的key数量少于这个数量，那就把他们捏成一个memtable
DEFINE_string(major_compaction_type, "no",
              "Major compaction type: i.e., no/lc/sc");//major compaction的类型
DEFINE_uint32(major_compaction_max_parallism, 1,
              "The maximum compaction parallelism.");//最大的compaction 并行数量??
DEFINE_uint32(major_compaction_max_tables_in_a_set, 15,
              "The maximum number of SSTables in a compaction job.");//一次compaction中最多的sstable数量

              // replica!!!!!
DEFINE_uint32(num_sstable_replicas, 1, "Number of replicas for SSTables.");//一个sstable的replica数量
DEFINE_uint32(num_sstable_metadata_replicas, 1, "Number of replicas for meta blocks of SSTables.");//sstable的meta block的replica数量
DEFINE_bool(use_parity_for_sstable_data_blocks, false, "");//是否开启对于sstable的data block的校验
DEFINE_uint32(num_manifest_replicas, 1, "Number of replicas for manifest file.");//manifest文件的replica数量

DEFINE_int32(fail_stoc_id, -1, "The StoC to fail.");
DEFINE_int32(exp_seconds_to_fail_stoc, -1,
             "Number of seconds elapsed to fail the stoc.");//在失效一个stoc之前需要等待的秒数
DEFINE_int32(failure_duration, -1, "Failure duration");
DEFINE_int32(num_migration_threads, 1, "Number of migration threads");//负责迁移的线程的数量
DEFINE_string(ltc_migration_policy, "base", "immediate/base");//迁移的策略
DEFINE_bool(use_ordered_flush, false, "use ordered flush");//是否采用顺序 flush? 默认nfalse 意义也许是序号在前的memtable/sstable先进行冲刷??

NovaConfig *NovaConfig::config; //配置
std::atomic_int_fast32_t leveldb::EnvBGThread::bg_flush_memtable_thread_id_seq;
std::atomic_int_fast32_t nova::StorageWorker::storage_file_number_seq;
// Sequence id to assign tasks to a thread in a round-robin manner.
std::atomic_int_fast32_t nova::RDMAServerImpl::compaction_storage_worker_seq_id_;
std::atomic_int_fast32_t leveldb::EnvBGThread::bg_compaction_thread_id_seq;
std::atomic_int_fast32_t nova::RDMAServerImpl::fg_storage_worker_seq_id_;
std::atomic_int_fast32_t nova::RDMAServerImpl::bg_storage_worker_seq_id_;
std::atomic_int_fast32_t leveldb::StoCBlockClient::rdma_worker_seq_id_;
std::atomic_int_fast32_t nova::DBMigration::migration_seq_id_;
std::atomic_int_fast32_t leveldb::StorageSelector::stoc_for_compaction_seq_id;

std::unordered_map<uint64_t, leveldb::FileMetaData *> leveldb::Version::last_fnfile;
std::atomic<nova::Servers *> leveldb::StorageSelector::available_stoc_servers;//当前的stoc server
NovaGlobalVariables NovaGlobalVariables::global;//全局统计量

//开启服务器
void StartServer() {
//rdma的管理器
    RdmaCtrl *rdma_ctrl = new RdmaCtrl(NovaConfig::config->my_server_id, // 这里就等着连接
                                       NovaConfig::config->rdma_port);
//    if (NovaConfig::config->my_server_id < FLAGS_number_of_ltcs) {
//        NovaConfig::config->mem_pool_size_gb = 10;
//    }
    int port = NovaConfig::config->servers[NovaConfig::config->my_server_id].port;
    uint64_t nrdmatotal = nrdma_buf_server();
    uint64_t ntotal = nrdmatotal;
    ntotal += NovaConfig::config->mem_pool_size_gb * 1024 * 1024 * 1024;
    uint64_t ndram = ntotal; // dram的长度
    ntotal += NovaConfig::config->pm_pool_size_gb * 1024 * 1024 * 1024; // 加上 pm pool的大小 rdma + 30G + 30G
    NOVA_LOG(INFO) << "Allocated buffer size in bytes: " << ndram;

    auto *buf = (char *) malloc(ndram); // (char *)aligned_alloc(4096, ndram);// 改为对齐分配
    memset(buf, 0, ndram); //pm的区域不用清理
    NovaConfig::config->nova_buf = buf; // 所有空间的开始 这里所有空间rdma都可以接收发送
//nnovabuf是结尾
    NovaConfig::config->nnovabuf = ndram; // ntotal; // 所有空间的大小(包括了) dram的空间
    NOVA_ASSERT(buf != NULL) << "Not enough memory";

//如果不恢复数据库，直接删除文件
    if (!FLAGS_recover_dbs) {
        int ret = system(fmt::format("exec rm -rf {}/*",
                           NovaConfig::config->db_path).data());
        ret = system(fmt::format("exec rm -rf {}/*",
                           NovaConfig::config->stoc_files_path).data());
        ret = system(fmt::format("exec rm -rf {}/*",
                           NovaConfig::config->pm_path).data());
    }
    
    mkdirs(NovaConfig::config->db_path.data());
    mkdirs(NovaConfig::config->stoc_files_path.data());
    mkdirs(NovaConfig::config->pm_path.data());

//这里就是一个服务器
    auto *mem_server = new NICServer(rdma_ctrl, buf, port);
    mem_server->Start();
}

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    int i;
    const char **methods = event_get_supported_methods();
    printf("Starting Libevent %s.  Available methods are:\n",
           event_get_version());
    for (i = 0; methods[i] != NULL; ++i) {
        printf("    %s\n", methods[i]);
    }
    if (FLAGS_server_id == -1) {
        exit(0);
    }
    std::vector<gflags::CommandLineFlagInfo> flags;
    gflags::GetAllFlags(&flags);
    for (const auto &flag : flags) {
        printf("%s=%s\n", flag.name.c_str(),
               flag.current_value.c_str());
    }

    NovaConfig::config = new NovaConfig;
    NovaConfig::config->stoc_files_path = FLAGS_stoc_files_path;

    NovaConfig::config->mem_pool_size_gb = FLAGS_mem_pool_size_gb;
    NovaConfig::config->pm_pool_size_gb = FLAGS_pm_pool_size_gb;
    NovaConfig::config->load_default_value_size = FLAGS_use_fixed_value_size;
    // RDMA
    NovaConfig::config->rdma_port = FLAGS_rdma_port;
    NovaConfig::config->max_msg_size = FLAGS_rdma_max_msg_size;
    NovaConfig::config->rdma_max_num_sends = FLAGS_rdma_max_num_sends;
    NovaConfig::config->rdma_doorbell_batch_size = FLAGS_rdma_doorbell_batch_size;

    NovaConfig::config->block_cache_mb = FLAGS_block_cache_mb;
    NovaConfig::config->memtable_size_mb = FLAGS_memtable_size_mb;

    NovaConfig::config->db_path = FLAGS_db_path; //done
    NovaConfig::config->pm_path = FLAGS_pm_path;
    NovaConfig::config->levels_in_pm = FLAGS_levels_in_pm;
    NovaConfig::config->enable_rdma = FLAGS_enable_rdma;
    NovaConfig::config->enable_load_data = FLAGS_enable_load_data;
    NovaConfig::config->major_compaction_type = FLAGS_major_compaction_type;
    NovaConfig::config->enable_flush_multiple_memtables = FLAGS_enable_flush_multiple_memtables;
    NovaConfig::config->major_compaction_max_parallism = FLAGS_major_compaction_max_parallism;
    NovaConfig::config->major_compaction_max_tables_in_a_set = FLAGS_major_compaction_max_tables_in_a_set;

    NovaConfig::config->number_of_recovery_threads = FLAGS_num_recovery_threads;
    NovaConfig::config->recover_dbs = FLAGS_recover_dbs;
    NovaConfig::config->number_of_sstable_data_replicas = FLAGS_num_sstable_replicas;
    NovaConfig::config->number_of_sstable_metadata_replicas = FLAGS_num_sstable_metadata_replicas;
    NovaConfig::config->number_of_manifest_replicas = FLAGS_num_manifest_replicas;
    NovaConfig::config->use_parity_for_sstable_data_blocks = FLAGS_use_parity_for_sstable_data_blocks;

    NovaConfig::config->servers = convert_hosts(FLAGS_all_servers);
    NovaConfig::config->my_server_id = FLAGS_server_id;
    NovaConfig::config->num_conn_workers = FLAGS_ltc_num_client_workers;
    NovaConfig::config->num_fg_rdma_workers = FLAGS_num_rdma_fg_workers;
    NovaConfig::config->num_storage_workers = FLAGS_num_storage_workers;
    NovaConfig::config->num_compaction_workers = FLAGS_num_compaction_workers;
    NovaConfig::config->num_bg_rdma_workers = FLAGS_num_rdma_bg_workers;
    NovaConfig::config->num_memtables = FLAGS_num_memtables;
    NovaConfig::config->num_memtable_partitions = FLAGS_num_memtable_partitions;
    NovaConfig::config->enable_subrange = FLAGS_enable_subrange;
    NovaConfig::config->memtable_type = FLAGS_memtable_type;

    NovaConfig::config->num_stocs_scatter_data_blocks = FLAGS_ltc_num_stocs_scatter_data_blocks;
//stoc文件 最大的大小 KB为单位了 这里到底是什么单位
    NovaConfig::config->max_stoc_file_size = FLAGS_max_stoc_file_size_mb * 1024;
//manifest文件大小 KB为单位?
    NovaConfig::config->manifest_file_size = NovaConfig::config->max_stoc_file_size * 4;//manifest单位应该是B?
//sstable_size 字节为单位的大小
    NovaConfig::config->sstable_size = FLAGS_sstable_size_mb * 1024 * 1024;
    NovaConfig::config->use_local_disk = FLAGS_use_local_disk;
    NovaConfig::config->num_tinyranges_per_subrange = FLAGS_num_tinyranges_per_subrange;

//不同的scatter策略
    if (FLAGS_scatter_policy == "random") {
        NovaConfig::config->scatter_policy = ScatterPolicy::RANDOM;
    } else if (FLAGS_scatter_policy == "power_of_two") { // power of 2 多一点
        NovaConfig::config->scatter_policy = ScatterPolicy::POWER_OF_TWO;
    } else if (FLAGS_scatter_policy == "power_of_three") {
        NovaConfig::config->scatter_policy = ScatterPolicy::POWER_OF_THREE;
    } else if (FLAGS_scatter_policy == "local") {
        NovaConfig::config->scatter_policy = ScatterPolicy::LOCAL;
        NOVA_ASSERT(NovaConfig::config->num_stocs_scatter_data_blocks == 1);
    } else {
        NovaConfig::config->scatter_policy = ScatterPolicy::SCATTER_DC_STATS;
    }

//不同的log策略 none和rdma都有
    if (FLAGS_log_record_mode == "none") {
        NovaConfig::config->log_record_mode = NovaLogRecordMode::LOG_NONE;
    } else if (FLAGS_log_record_mode == "rdma") {
        NovaConfig::config->log_record_mode = NovaLogRecordMode::LOG_RDMA;
    }

    if(FLAGS_log_type == "dram"){
        NovaConfig::config->log_type = NovaLogType::LOG_DRAM;
    }else if(FLAGS_log_type == "pm"){
        NovaConfig::config->log_type = NovaLogType::LOG_PM;
    }else if(FLAGS_log_type == "disk"){
        NovaConfig::config->log_type == NovaLogType::LOG_DISK;
    }

    NovaConfig::config->batch_size = FLAGS_batch_size;

    NovaConfig::config->enable_lookup_index = FLAGS_enable_lookup_index;
    NovaConfig::config->enable_range_index = FLAGS_enable_range_index;
    NovaConfig::config->subrange_sampling_ratio = FLAGS_sampling_ratio;
    NovaConfig::config->zipfian_dist_file_path = FLAGS_zipfian_dist_ref_counts;
    NovaConfig::config->ReadZipfianDist(); // zipfian的负载等 这个还不清楚
    NovaConfig::config->client_access_pattern = FLAGS_client_access_pattern;
    NovaConfig::config->enable_detailed_db_stats = FLAGS_enable_detailed_db_stats;
    NovaConfig::config->subrange_num_keys_no_flush = FLAGS_subrange_no_flush_num_keys;
    NovaConfig::config->l0_stop_write_mb = FLAGS_l0_stop_write_mb;
    NovaConfig::config->l0_start_compaction_mb = FLAGS_l0_start_compaction_mb;
    NovaConfig::config->level = FLAGS_level;
    NovaConfig::config->enable_subrange_reorg = FLAGS_enable_subrange_reorg;
    NovaConfig::config->num_migration_threads = FLAGS_num_migration_threads;
    NovaConfig::config->use_ordered_flush = FLAGS_use_ordered_flush;

// ltc的迁移策略 immediate多一些
    if (FLAGS_ltc_migration_policy == "immediate") {
        NovaConfig::config->ltc_migration_policy = LTCMigrationPolicy::IMMEDIATE;
    } else {
        NovaConfig::config->ltc_migration_policy = LTCMigrationPolicy::PROCESS_UNTIL_MIGRATION_COMPLETE;
    }

//读关于data partition的东西
    NovaConfig::ReadFragments(FLAGS_ltc_config_path);
//如果一个log有一些replica的话
    if (FLAGS_num_log_replicas > 0) {
//在每一个config里面必须要保证replica个数小于总体stoc server的个数，这里默认设置是没有replica
        for (int i = 0; i < NovaConfig::config->cfgs.size(); i++) {
            NOVA_ASSERT(FLAGS_num_log_replicas <= NovaConfig::config->cfgs[i]->stoc_servers.size());
        }
        NovaConfig::ComputeLogReplicaLocations(FLAGS_num_log_replicas);
    }
    NOVA_LOG(INFO) << fmt::format("{} configurations", NovaConfig::config->cfgs.size());
    for (auto c : NovaConfig::config->cfgs) {
        NOVA_LOG(INFO) << c->DebugString();
    }

    leveldb::EnvBGThread::bg_flush_memtable_thread_id_seq = 0;
    leveldb::EnvBGThread::bg_compaction_thread_id_seq = 0;
    nova::RDMAServerImpl::bg_storage_worker_seq_id_ = 0;
    leveldb::StoCBlockClient::rdma_worker_seq_id_ = 0;
    nova::StorageWorker::storage_file_number_seq = 0;
    nova::RDMAServerImpl::compaction_storage_worker_seq_id_ = 0;
    nova::DBMigration::migration_seq_id_ = 0;
    leveldb::StorageSelector::stoc_for_compaction_seq_id = nova::NovaConfig::config->my_server_id;
    nova::NovaGlobalVariables::global.Initialize(); // 为了其他server进行读取的全局统计 用于负载均衡等
    auto available_stoc_servers = new Servers;
    available_stoc_servers->servers = NovaConfig::config->cfgs[0]->stoc_servers;
    for (int i = 0; i < available_stoc_servers->servers.size(); i++) {
        available_stoc_servers->server_ids.insert(available_stoc_servers->servers[i]);
    }
    leveldb::StorageSelector::available_stoc_servers.store(available_stoc_servers);

//各种检查
    // Sanity checks.
    if (NovaConfig::config->number_of_sstable_data_replicas > 1) {
        NOVA_ASSERT(NovaConfig::config->number_of_sstable_data_replicas ==
                    NovaConfig::config->number_of_sstable_metadata_replicas);
        NOVA_ASSERT(NovaConfig::config->num_stocs_scatter_data_blocks == 1);
        NOVA_ASSERT(!NovaConfig::config->use_parity_for_sstable_data_blocks);
    }

//各种检查
    if (NovaConfig::config->use_parity_for_sstable_data_blocks) {
        NOVA_ASSERT(NovaConfig::config->number_of_sstable_data_replicas == 1);
        NOVA_ASSERT(NovaConfig::config->num_stocs_scatter_data_blocks > 1);
        NOVA_ASSERT(NovaConfig::config->num_stocs_scatter_data_blocks +
                    NovaConfig::config->number_of_sstable_metadata_replicas + 1 <=
                    NovaConfig::config->cfgs[0]->stoc_servers.size());
    }
//检查一下其他cfg的问题
    for (int i = 0; i < NovaConfig::config->cfgs.size(); i++) {
        auto cfg = NovaConfig::config->cfgs[i];
        NOVA_ASSERT(FLAGS_ltc_num_stocs_scatter_data_blocks <= cfg->stoc_servers.size()) << fmt::format(
                    "Not enough stoc to scatter. Scatter width: {} Num StoCs: {}",
                    FLAGS_ltc_num_stocs_scatter_data_blocks, cfg->stoc_servers.size());
        NOVA_ASSERT(FLAGS_num_sstable_replicas <= cfg->stoc_servers.size()) << fmt::format(
                    "Not enough stoc to replicate sstables. Replication factor: {} Num StoCs: {}",
                    FLAGS_num_sstable_replicas, cfg->stoc_servers.size());
    }
//开启服务器
    StartServer();
    return 0;
}

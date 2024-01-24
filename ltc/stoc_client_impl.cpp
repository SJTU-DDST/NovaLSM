
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "stoc_client_impl.h"
#include "common/nova_config.h"
#include "common/nova_common.h"

#include <fmt/core.h>
#include "db/filename.h"

namespace leveldb {
    using namespace rdmaio;

// 这里说明这个类没有什么并发
    void StoCBlockClient::IncrementReqId() {
        req_id_++;
        if (req_id_ == 0) {
            req_id_ = 1;
        }
    }

//也是初始化一些东西
    StoCBlockClient::StoCBlockClient(uint32_t client_id,
                                     StocPersistentFileManager *stoc_file_manager)
            : stoc_file_manager_(stoc_file_manager) {
        sem_init(&sem_, 0, 0);
        current_rdma_msg_handler_id_ = client_id;
    }

// 每次调用都保证了是同一层的
    uint32_t
    StoCBlockClient::InitiateReplicateSSTables(uint32_t stoc_server_id,
                                               const std::string &dbname,
                                               const std::string &pmname,
                                               int level,
                                               int levels_in_pm,
                                               const std::vector<leveldb::ReplicationPair> &pairs) {
        NOVA_ASSERT(stoc_server_id != nova::NovaConfig::config->my_server_id);
        RDMARequestTask task = {};
        task.type = RDMA_CLIENT_RECONSTRUCT_MISSING_REPLICA;
        task.server_id = stoc_server_id;
        task.missing_replicas = pairs;
        task.dbname = dbname;
        task.pmname = pmname;
        task.level = level;
        task.levels_in_pm = levels_in_pm;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        StoCResponse *response = new StoCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);
        IncrementReqId();
        return reqid;
    }

    uint32_t
    StoCBlockClient::InitiateRDMAWRITE(uint32_t remote_server_id, char *data,
                                       uint32_t size) {
        NOVA_ASSERT(remote_server_id != nova::NovaConfig::config->my_server_id);
        RDMARequestTask task = {};
        task.type = RDMA_CLIENT_RDMA_WRITE_REQUEST;
        task.server_id = remote_server_id;
        task.write_buf = data;
        task.size = size;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        AddAsyncTask(task);
        IncrementReqId();
        return reqid;
    }

//查看remote_server_id是否准备好了??
    uint32_t StoCBlockClient::InitiateIsReadyForProcessingRequests(
            uint32_t remote_server_id) {
        NOVA_ASSERT(remote_server_id != nova::NovaConfig::config->my_server_id);
        RDMARequestTask task = {};
        task.type = RDMA_CLIENT_IS_READY_FOR_REQUESTS;
        task.server_id = remote_server_id;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        StoCResponse *response = new StoCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);
        IncrementReqId();
        return reqid;
    }

// 向远端发送一个compaction请求
    uint32_t StoCBlockClient::InitiateCompaction(uint32_t remote_server_id,
                                                 leveldb::CompactionRequest *compaction_request) {
        NOVA_ASSERT(remote_server_id != nova::NovaConfig::config->my_server_id);
        RDMARequestTask task = {};
        task.type = RDMA_CLIENT_COMPACTION;
        task.server_id = remote_server_id;
        task.compaction_request = compaction_request;
        task.sem = compaction_request->completion_signal;
        NOVA_ASSERT(task.sem);

        uint32_t reqid = req_id_;
        StoCResponse *response = new StoCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);
        IncrementReqId();
        return reqid;
    }

// 用于向远程stoc指定文件写一段数据 done
// dbname 以下是initiateappendblock相关
    uint32_t StoCBlockClient::InitiateAppendBlock(
            uint32_t stoc_id, uint32_t thread_id, uint32_t *stoc_file_id,
            char *buf, const std::string &dbname, const std::string &pmname, int level, int levels_in_pm, uint64_t file_number,
            uint32_t replica_id,
            uint32_t size, FileInternalType internal_type) {
        if (stoc_id == nova::NovaConfig::config->my_server_id) { // 本地写修改好了
            std::string filename;
            if (file_number == 0) {
                filename = leveldb::DescriptorFileName(dbname, 0, replica_id);
            } else {
                filename = leveldb::TableFileName(dbname, pmname, 
                                                  file_number,
                                                  level, levels_in_pm,
                                                  internal_type, replica_id);
            }
            leveldb::StoCPersistentFile *stoc_file = stoc_file_manager_->OpenStoCFile(
                    thread_id, filename);
            NOVA_ASSERT(stoc_file);
            uint64_t stoc_file_off = stoc_file->AllocateBuf(filename, size, internal_type);
            NOVA_ASSERT(stoc_file_off != UINT64_MAX)
                << fmt::format("{} {}", filename, size);
            char *stoc_file_buf = (char *) stoc_file_off;
            memcpy(stoc_file_buf, buf, size);
            NOVA_ASSERT(stoc_file->MarkOffsetAsWritten(stoc_file->file_id(),
                                                       stoc_file_off))
                << stoc_id;
            uint64_t persisted_bytes = stoc_file->Persist(stoc_file->file_id());
            NOVA_ASSERT(persisted_bytes == size)
                << fmt::format("persisted bytes:{} written bytes:{}",
                               persisted_bytes, size);
            leveldb::BlockHandle h = stoc_file->Handle(filename, internal_type);
            leveldb::StoCBlockHandle rh = {};
            rh.server_id = nova::NovaConfig::config->my_server_id;
            rh.stoc_file_id = stoc_file->file_id();
            rh.offset = h.offset();
            rh.size = h.size();
            if (file_number != 0) {
                NOVA_ASSERT(h.offset() == 0 && h.size() == size);
                stoc_file->ForceSeal();
            }
            uint32_t reqid = req_id_;
            StoCResponse *response = new StoCResponse;
            req_response[reqid] = response;
            IncrementReqId();
            response->is_complete = true;
            response->stoc_block_handles.push_back(rh);
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("Wake up local write");
            sem_post(&sem_);
            return reqid;
        }

        NOVA_ASSERT(stoc_id != nova::NovaConfig::config->my_server_id);
        RDMARequestTask task = {};
        task.type = RDMAClientRequestType::RDMA_CLIENT_REQ_WRITE_DATA_BLOCKS;
        task.server_id = stoc_id;
        task.thread_id = thread_id;
        task.write_buf = buf;
        task.dbname = dbname;
        task.pmname = pmname;
        task.level = level;
        task.levels_in_pm = levels_in_pm;
        task.file_number = file_number;
        task.replica_id = replica_id;
        task.write_size = size;
        task.sem = &sem_;
        task.internal_type = internal_type;
        task.local_which = 0; // 目前假设所有发送都来源于dram
        task.remote_which = (level <= levels_in_pm ? 1 : 0); // 如果level小 那么就是直接写入pm, 如果level大, 那就是写入dram缓冲区

        uint32_t reqid = req_id_;
        StoCResponse *response = new StoCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);
        IncrementReqId();
        return reqid;
    }

    uint32_t StoCBlockClient::InitiateReadStoCStats(uint32_t stoc_id) {
        if (stoc_id == nova::NovaConfig::config->my_server_id) {
            uint32_t reqid = req_id_;
            StoCResponse *response = new StoCResponse;
            req_response[reqid] = response;
            response->is_complete = true;
            response->stoc_queue_depth = nova::NovaGlobalVariables::global.stoc_queue_depth;
            response->stoc_pending_write_bytes = nova::NovaGlobalVariables::global.stoc_pending_disk_writes;
            response->stoc_pending_read_bytes = nova::NovaGlobalVariables::global.stoc_pending_disk_reads;
            IncrementReqId();
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("Wake up local read stats");
            sem_post(&sem_);
            return reqid;
        }

        RDMARequestTask task = {};
        task.type = RDMAClientRequestType::RDMA_CLIENT_READ_STOC_STATS;
        task.server_id = stoc_id;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        StoCResponse *response = new StoCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);
        IncrementReqId();
        return reqid;
    }

    uint32_t StoCBlockClient::InitiateReadInMemoryLogFile(char *local_buf,
                                                          uint32_t stoc_id,
                                                          uint64_t remote_offset,
                                                          uint64_t size) {
        RDMARequestTask task = {};
        task.type = RDMAClientRequestType::RDMA_CLIENT_READ_LOG_FILE;
        task.rdma_log_record_backing_mem = local_buf;
        task.server_id = stoc_id;
        task.remote_stoc_offset = remote_offset;
        task.size = size;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        StoCResponse *response = new StoCResponse;
        req_response[reqid] = response;
        task.response = response;
        AddAsyncTask(task);
        IncrementReqId();
        return reqid;
    }

    uint32_t StoCRDMAClient::InitiateReadInMemoryLogFile(char *local_buf,
                                                         uint32_t stoc_id,
                                                         uint64_t remote_offset,
                                                         uint64_t size) {
        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.req_type = StoCRequestType::STOC_READ_IN_MEMORY_LOG_FILE;
        context.done = false;
        char *sendbuf = rdma_broker_->GetSendBuf(stoc_id);
        leveldb::EncodeFixed32(sendbuf, req_id);
        context.wr_id = rdma_broker_->PostRead(local_buf, size, stoc_id, 0, remote_offset, false);
        request_context_[req_id] = context;
        IncrementReqId();
        NOVA_LOG(DEBUG)
            << fmt::format(
                    "stoc-client[{}]: Read log file: stoc:{} off:{} size:{} req:{}",
                    stoc_client_id_, stoc_id, remote_offset, size,
                    req_id);
        return req_id;
    }

    uint32_t StoCBlockClient::InitiateQueryLogFile(uint32_t stoc_id,
                                                   uint32_t server_id,
                                                   uint32_t dbid,
                                                   std::unordered_map<std::string, uint64_t> *logfile_offset) {
        NOVA_ASSERT(
                stoc_id != nova::NovaConfig::config->my_server_id);
        RDMARequestTask task = {};
        task.type = RDMAClientRequestType::RDMA_CLIENT_REQ_QUERY_LOG_FILES;
        task.server_id = stoc_id;
        task.dbid = dbid;
        task.logfile_offset = logfile_offset;
        task.sem = &sem_;

        uint32_t reqid = req_id_;
        AddAsyncTask(task);
        IncrementReqId();
        return reqid;
    }

// 查看response
    bool StoCBlockClient::IsDone(uint32_t req_id,
                                 leveldb::StoCResponse *response,
                                 uint64_t *timeout) {
        auto it = req_response.find(req_id); // 这个结构用来储存发过来的response 取出来找到自己的response就好 如果需要response的话会在这里登记
        if (it == req_response.end()) {
            return true;
        }
        NOVA_ASSERT(response);
        auto stored_response = it->second;
        if (!stored_response->is_complete) {
            return false;
        }
        response->is_complete = true;
        response->stoc_file_id = stored_response->stoc_file_id;
        response->stoc_block_handles = stored_response->stoc_block_handles;
        response->replication_results = stored_response->replication_results;
        response->stoc_queue_depth = stored_response->stoc_queue_depth;
        response->stoc_pending_read_bytes = stored_response->stoc_pending_read_bytes;
        response->stoc_pending_write_bytes = stored_response->stoc_pending_write_bytes;
        response->is_ready_to_process_requests = stored_response->is_ready_to_process_requests;
        delete it->second;
        req_response.erase(req_id);
        return true;
    }

    uint32_t StoCBlockClient::InitiateDeleteTables(uint32_t server_id,
                                                   const std::vector<leveldb::SSTableStoCFilePair> &stoc_file_ids) {
        if (server_id == nova::NovaConfig::config->my_server_id) {
            for (int i = 0; i < stoc_file_ids.size(); i++) {
                leveldb::StoCPersistentFile *stoc_file = stoc_file_manager_->FindStoCFile(
                        stoc_file_ids[i].stoc_file_id);
                stoc_file->DeleteSSTable(stoc_file_ids[i].stoc_file_id,
                                         stoc_file_ids[i].sstable_name);
                leveldb::FileType type;
                NOVA_ASSERT(
                        leveldb::ParseFileName(stoc_file_ids[i].sstable_name,
                                               &type));
                if (type == leveldb::FileType::kTableFile) {
                    stoc_file_manager_->DeleteSSTable(
                            stoc_file_ids[i].sstable_name + "-meta");
                }
            }
            return 0;
        }

        RDMARequestTask task = {};
        task.type = RDMAClientRequestType::RDMA_CLIENT_REQ_DELETE_TABLES;
        task.server_id = server_id;
        task.stoc_file_ids = stoc_file_ids;
        AddAsyncTask(task);
        return 0;
    }

// recoever的时候建立filename到fileid的映射 总的来说 就是打开文件并且建立映射
    uint32_t
    StoCBlockClient::InitiateInstallFileNameStoCFileMapping(uint32_t stoc_id,
                                                            const std::unordered_map<std::string, uint32_t> &fn_stocfnid) {
        if (stoc_id == nova::NovaConfig::config->my_server_id) {
            stoc_file_manager_->OpenStoCFiles(fn_stocfnid);
            sem_post(&sem_);
            return 0;
        }
        RDMARequestTask task = {};
        task.type = RDMAClientRequestType::RDMA_CLIENT_FILENAME_STOC_FILE_MAPPING;
        task.server_id = stoc_id;
        task.fn_stoc_file_id = fn_stocfnid;
        task.sem = &sem_;
        AddAsyncTask(task);
        uint32_t reqid = req_id_;
        IncrementReqId();
        return reqid;
    }


// dbname 以上是initiateappendblock相关 以下是replicate相关
    uint32_t StoCRDMAClient::InitiateReplicateSSTables(uint32_t stoc_server_id,
                                                       const std::string &dbname,
                                                       const std::string &pmname,
                                                       int level,
                                                       int levels_in_pm,
                                                       const std::vector<leveldb::ReplicationPair> &pairs) {
        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.done = false;
        context.req_type = StoCRequestType::STOC_REPLICATE_SSTABLES;

        char *send_buf = rdma_broker_->GetSendBuf(stoc_server_id);
        uint32_t msg_size = 1;
        send_buf[0] = StoCRequestType::STOC_REPLICATE_SSTABLES;
        msg_size += EncodeStr(send_buf + msg_size, dbname); // pmname level levels_in_pm
        msg_size += EncodeStr(send_buf + msg_size, pmname);
        msg_size += EncodeFixed32(send_buf + msg_size, (uint32_t)level);
        msg_size += EncodeFixed32(send_buf + msg_size, (uint32_t)levels_in_pm);
        msg_size += EncodeFixed32(send_buf + msg_size, pairs.size());

        for (const auto &it : pairs) {
            msg_size += it.Encode(send_buf + msg_size);
        }
        rdma_broker_->PostSend(send_buf, msg_size, stoc_server_id, req_id);
        request_context_[req_id] = context;
        IncrementReqId();
        NOVA_LOG(DEBUG)
            << fmt::format(
                    "stocclient[{}]: Reconstruct replicas stoc:{} size:{} req:{}",
                    stoc_client_id_, stoc_server_id, pairs.size(), req_id);
        return req_id;
    }

    uint32_t
    StoCRDMAClient::InitiateInstallFileNameStoCFileMapping(uint32_t stoc_id,
                                                           const std::unordered_map<std::string, uint32_t> &fn_stocid) {
        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.done = false;
        context.req_type = StoCRequestType::STOC_FILENAME_STOCFILEID;

        char *send_buf = rdma_broker_->GetSendBuf(stoc_id);
        uint32_t msg_size = 1;
        send_buf[0] = StoCRequestType::STOC_FILENAME_STOCFILEID;
        msg_size += EncodeFixed32(send_buf + msg_size, fn_stocid.size());
        for (const auto &it : fn_stocid) {
            msg_size += EncodeStr(send_buf + msg_size, it.first);
            msg_size += EncodeFixed32(send_buf + msg_size, it.second);
            NOVA_LOG(DEBUG)
                << fmt::format("Install {} {} at StoC-{}", it.first, it.second,
                               stoc_id);
        }

        rdma_broker_->PostSend(send_buf, msg_size, stoc_id, req_id);
        request_context_[req_id] = context;
        IncrementReqId();
        NOVA_LOG(DEBUG)
            << fmt::format(
                    "stocclient[{}]: Inform Filename StoC file ID stoc:{} size:{} req:{}",
                    stoc_client_id_, stoc_id, fn_stocid.size(), req_id);
        return req_id;
    }

// 将填好的任务下发到rdma线程中 这里都是加入到后台rdma msg handler了
    void StoCBlockClient::AddAsyncTask(
            const leveldb::RDMARequestTask &task) {
        if (task.type == RDMAClientRequestType::RDMA_CLIENT_REQ_LOG_RECORD) { // 这里保证了相同的memtable的任务会传到相同的rdmahandler!!!!
            uint64_t id = task.memtable_id;
            rdma_msg_handlers_[id % rdma_msg_handlers_.size()]->AddTask(task);
            return;
        }
        uint32_t seq = StoCBlockClient::rdma_worker_seq_id_.fetch_add(1, std::memory_order_relaxed) %
                       rdma_msg_handlers_.size();
        rdma_msg_handlers_[seq]->AddTask(task);
    }

// 下发一个读block的rpc请求 handle是读的一些信息 
    uint32_t StoCBlockClient::InitiateReadDataBlock(
            const leveldb::StoCBlockHandle &block_handle, uint64_t offset, uint32_t size, char *result,
            uint32_t result_size, std::string filename, bool is_foreground_reads) {
        NOVA_ASSERT(size <= result_size)
            << fmt::format("{} {} {} {}", block_handle.DebugString(), filename,
                           size, result_size);
        // 如果发现这个请求是对自己这个server的 就不用发rpc了                   
        if (block_handle.server_id == nova::NovaConfig::config->my_server_id) { // 本地化已经改好了 或许本地的文件如果在pm里面可以省去变成memfile? 之后再说
            StoCBlockHandle converted_handle = {};
            uint32_t stoc_file_id = block_handle.stoc_file_id;
            if (!filename.empty()) {
                stoc_file_id = stoc_file_manager_->OpenStoCFile(0, filename)->file_id(); // 之后需要把这里改为对持久文件直接读写
            }
            Slice output;
            converted_handle.server_id = block_handle.server_id;
            converted_handle.stoc_file_id = stoc_file_id;
            converted_handle.offset = offset;
            converted_handle.size = size;
            stoc_file_manager_->ReadDataBlock(converted_handle,
                                              converted_handle.offset,
                                              converted_handle.size,
                                              result, &output);
//            RDMA_ASSERT(output.size() == converted_handle.size);
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("Wake up local read");
            sem_post(&sem_);
            uint32_t reqid = req_id_;
            IncrementReqId();
            return reqid;
        }

        RDMARequestTask task = {};
        task.type = RDMAClientRequestType::RDMA_CLIENT_REQ_READ;
        task.server_id = block_handle.server_id;
        task.stoc_block_handle = block_handle;
        task.offset = offset;
        task.size = size;
        task.result = result;
        task.write_size = result_size;
        task.filename = filename;
        task.sem = &sem_;
        task.is_foreground_reads = is_foreground_reads;
        task.local_which = 0;
        task.remote_which = IsPMfile(filename) ? 1 : 0;// 是pmfile从pm读
        AddAsyncTask(task);

        uint32_t reqid = req_id_;
        IncrementReqId();
        return reqid;
    }

// 写日志
// 写日志会落到这里
    uint32_t StoCBlockClient::InitiateReplicateLogRecords(
            const std::string &log_file_name, uint64_t thread_id,
            uint32_t db_id, uint32_t memtable_id,
            char *rdma_backing_mem,
            const std::vector<LevelDBLogRecord> &log_records,
            StoCReplicateLogRecordState *replicate_log_record_states,
            StoCLogType log_type) {
        RDMARequestTask task = {};
        task.type = RDMAClientRequestType::RDMA_CLIENT_REQ_LOG_RECORD;
        task.log_file_name = log_file_name;
        task.thread_id = thread_id;
        task.dbid = db_id;
        task.memtable_id = memtable_id;
        task.log_records = log_records;
        task.write_buf = rdma_backing_mem;
        task.replicate_log_record_states = replicate_log_record_states;
        task.sem = &sem_;
        task.log_type = log_type;
        AddAsyncTask(task);
        return 0;
    }

// 关闭log相关文件
    uint32_t StoCBlockClient::InitiateCloseLogFiles(
            const std::vector<std::string> &log_file_name, uint32_t dbid) {
        RDMARequestTask task = {};
        task.type = RDMAClientRequestType::RDMA_CLIENT_REQ_CLOSE_LOG;
        task.log_files = log_file_name;
        task.dbid = dbid;
        AddAsyncTask(task);
        NOVA_LOG(DEBUG) << fmt::format("Close {}", dbid);
        return 0;
    }


    void StoCRDMAClient::IncrementReqId() {
        current_req_id_++;
        if (current_req_id_ == upper_req_id_) {
            current_req_id_ = lower_req_id_;
        }
    }

    uint32_t StoCRDMAClient::GetCurrentReqId() {
        return current_req_id_;
    }

    uint32_t StoCRDMAClient::InitiateDeleteTables(uint32_t stoc_id,
                                                  const std::vector<SSTableStoCFilePair> &stoc_fileids) {
        NOVA_ASSERT(stoc_id != nova::NovaConfig::config->my_server_id);
        uint32_t req_id = current_req_id_;
        NOVA_LOG(DEBUG)
            << fmt::format("stocclient[{}]: Delete SSTables server:{} n:{}",
                           stoc_client_id_, stoc_id, stoc_fileids.size());

        char *send_buf = rdma_broker_->GetSendBuf(stoc_id);
        uint32_t msg_size = 1;
        send_buf[0] = StoCRequestType::STOC_DELETE_TABLES;
        EncodeFixed32(send_buf + msg_size, stoc_fileids.size());
        msg_size += 4;
        for (auto &pair : stoc_fileids) {
            msg_size += EncodeStr(send_buf + msg_size, pair.sstable_name);
            EncodeFixed32(send_buf + msg_size, pair.stoc_file_id);
            msg_size += 4;
        }
        rdma_broker_->PostSend(send_buf, msg_size, stoc_id, req_id);
        IncrementReqId();
        // Does not need to send immediately.
        return 0;
    }

    uint32_t StoCRDMAClient::InitiateQueryLogFile(uint32_t stoc_id,
                                                  uint32_t server_id,
                                                  uint32_t dbid,
                                                  std::unordered_map<std::string, uint64_t> *logfile_offset) {
        NOVA_ASSERT(server_id !=
                    nova::NovaConfig::config->my_server_id);

        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.req_type = StoCRequestType::STOC_QUERY_LOG_FILES;
        context.logfile_offset = logfile_offset;
        context.done = false;

        char *send_buf = rdma_broker_->GetSendBuf(stoc_id);
        uint32_t msg_size = 1;
        send_buf[0] = StoCRequestType::STOC_QUERY_LOG_FILES;
        EncodeFixed32(send_buf + msg_size, server_id);
        msg_size += 4;
        EncodeFixed32(send_buf + msg_size, dbid);
        msg_size += 4;

        rdma_broker_->PostSend(send_buf, msg_size, stoc_id,
                               req_id);
        request_context_[req_id] = context;
        IncrementReqId();

        NOVA_LOG(DEBUG)
            << fmt::format(
                    "stocclient[{}]: Query StoC file server:{} db:{} req:{}",
                    stoc_client_id_, stoc_id,
                    dbid, req_id);
        return req_id;
    }

// compaction request发送到对面
    uint32_t StoCRDMAClient::InitiateCompaction(uint32_t stoc_id,
                                                leveldb::CompactionRequest *compaction_request) {
        NOVA_ASSERT(stoc_id !=
                    nova::NovaConfig::config->my_server_id);
        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.req_type = StoCRequestType::STOC_COMPACTION;
        context.compaction = compaction_request;
        context.done = false;

        char *send_buf = rdma_broker_->GetSendBuf(stoc_id);
        uint32_t msg_size = 1;
        send_buf[0] = StoCRequestType::STOC_COMPACTION;
        msg_size += compaction_request->EncodeRequest(send_buf + 1);

        rdma_broker_->PostSend(send_buf, msg_size, stoc_id, req_id);
        request_context_[req_id] = context;
        IncrementReqId();

        NOVA_LOG(DEBUG)
            << fmt::format(
                    "stocclient[{}]: Initiate compaction server:{}  req:{}",
                    stoc_client_id_, stoc_id, req_id);
        return req_id;
    }

    uint32_t StoCRDMAClient::InitiateReadDataBlock(
            const leveldb::StoCBlockHandle &block_handle, uint64_t offset,
            uint32_t size, char *result, uint32_t result_size,
            std::string filename, bool is_foreground_reads) {
        NOVA_ASSERT(block_handle.server_id !=
                    nova::NovaConfig::config->my_server_id);
        NOVA_ASSERT(size <= result_size);
        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.req_type = StoCRequestType::STOC_READ_BLOCKS;
        context.backing_mem = result;
        context.size = size;
        context.done = false;
        context.log_file_name = filename;
        context.local_which = 0;
        context.remote_which = IsPMfile(filename) ? 1 : 0; // 如果是pm文件 那就pm读 不是的话还是dram缓冲区

        char *send_buf = rdma_broker_->GetSendBuf(block_handle.server_id);
        uint32_t msg_size = 1;
        send_buf[0] = StoCRequestType::STOC_READ_BLOCKS;
        msg_size += EncodeBool(send_buf + msg_size, is_foreground_reads);
        msg_size += EncodeFixed32(send_buf + msg_size,
                                  block_handle.stoc_file_id);
        msg_size += EncodeFixed64(send_buf + msg_size, offset);
        msg_size += EncodeFixed32(send_buf + msg_size, size);
        msg_size += EncodeFixed64(send_buf + msg_size, (uint64_t) result);
        msg_size += EncodeStr(send_buf + msg_size, filename);
        rdma_broker_->PostSend(send_buf, msg_size, block_handle.server_id,
                               req_id);
        request_context_[req_id] = context;
        IncrementReqId();
        NOVA_LOG(DEBUG)
            << fmt::format(
                    "stocclient[{}]: Read StoC file server:{} StoC file:{} offset:{} size:{} off:{} size:{} fn:{} backing_mem:{} req:{}",
                    stoc_client_id_, block_handle.server_id,
                    block_handle.stoc_file_id, block_handle.offset,
                    block_handle.size, offset, size, filename,
                    (uint64_t) result,
                    req_id);
        return req_id;
    }

    uint32_t
    StoCRDMAClient::InitiateIsReadyForProcessingRequests(uint32_t stoc_id) {
        NOVA_ASSERT(stoc_id != nova::NovaConfig::config->my_server_id);
        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.done = false;
        context.req_type = StoCRequestType::STOC_IS_READY_FOR_REQUESTS;

        char *send_buf = rdma_broker_->GetSendBuf(stoc_id);
        uint32_t msg_size = 1;
        send_buf[0] = StoCRequestType::STOC_IS_READY_FOR_REQUESTS;
        rdma_broker_->PostSend(send_buf, msg_size, stoc_id, req_id);
        request_context_[req_id] = context;
        IncrementReqId();
        return req_id;
    }

    uint32_t StoCRDMAClient::InitiateReadStoCStats(uint32_t server_id) {
        NOVA_ASSERT(server_id != nova::NovaConfig::config->my_server_id);
        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.done = false;
        context.req_type = StoCRequestType::STOC_READ_STATS;

        char *send_buf = rdma_broker_->GetSendBuf(server_id);
        uint32_t msg_size = 1;
        send_buf[0] = StoCRequestType::STOC_READ_STATS;
        rdma_broker_->PostSend(send_buf, msg_size, server_id, req_id);
        request_context_[req_id] = context;
        IncrementReqId();
        return req_id;
    }

    uint32_t
    StoCRDMAClient::InitiateRDMAWRITE(uint32_t remote_server_id, char *data,
                                      uint32_t size) {
        NOVA_ASSERT(remote_server_id != nova::NovaConfig::config->my_server_id);
        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.done = false;
        context.req_type = StoCRequestType::RDMA_WRITE_REQUEST;

        char *send_buf = rdma_broker_->GetSendBuf(remote_server_id);
        uint32_t msg_size = 1;
        send_buf[0] = StoCRequestType::RDMA_WRITE_REQUEST;
        EncodeFixed32(send_buf + msg_size, size);
        msg_size += 4;
        rdma_broker_->PostSend(send_buf, msg_size, remote_server_id, req_id);
        context.backing_mem = data;
        context.size = size;
        request_context_[req_id] = context;
        IncrementReqId();
        NOVA_LOG(DEBUG)
            << fmt::format(
                    "stocclient[{}]: RDMA WRITE server:{} size:{} req:{}",
                    stoc_client_id_, remote_server_id, size, req_id);
        return req_id;
    }

// dbname 以下是intiateblock相关
// 用于向远程文件写
    uint32_t StoCRDMAClient::InitiateAppendBlock(uint32_t stoc_id,
                                                 uint32_t thread_id,
                                                 uint32_t *stoc_file_id,
                                                 char *buf,
                                                 const std::string &dbname,
                                                 const std::string &pmname,
                                                 int level,
                                                 int levels_in_pm,
                                                 uint64_t file_number,
                                                 uint32_t replica_id,
                                                 uint32_t size,
                                                 FileInternalType internal_type) {
        NOVA_ASSERT(stoc_id != nova::NovaConfig::config->my_server_id);
        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.done = false;
        context.req_type = StoCRequestType::STOC_WRITE_SSTABLE;
        context.local_which = 0; // 当前设置全是dram写入
        context.remote_which = (level <= levels_in_pm ? 1 : 0); // 如果level小说明直接写入pm 否则写入dram缓冲区

        char *send_buf = rdma_broker_->GetSendBuf(stoc_id);
        uint32_t msg_size = 2;
        send_buf[0] = StoCRequestType::STOC_WRITE_SSTABLE;
        send_buf[1] = internal_type;
        msg_size += EncodeStr(send_buf + msg_size, dbname);
        msg_size += EncodeStr(send_buf + msg_size, pmname); // 改了格式!!!!!!! pmname level levels_in_pm
        EncodeFixed32(send_buf + msg_size, (uint32_t)level);
        msg_size += 4;
        EncodeFixed32(send_buf + msg_size, (uint32_t)levels_in_pm);
        msg_size += 4;
        EncodeFixed64(send_buf + msg_size, file_number);
        msg_size += 8;
        EncodeFixed32(send_buf + msg_size, replica_id);
        msg_size += 4;
        EncodeFixed32(send_buf + msg_size, size);
        msg_size += 4;
        rdma_broker_->PostSend(send_buf, msg_size, stoc_id, req_id);
        context.backing_mem = buf;
        context.size = size;
        request_context_[req_id] = context;
        IncrementReqId();
        NOVA_LOG(DEBUG)
            << fmt::format(
                    "stocclient[{}]: Write StoC file server:{} t:{} db:{} fn:{} size:{} req:{}",
                    stoc_client_id_, stoc_id, thread_id, dbname, file_number,
                    size, req_id);
        return req_id;
    }

// rdmaclient是每个rdma线程1个
// 保证这里处理1个memtable内所有的log
    uint32_t StoCRDMAClient::InitiateReplicateLogRecords(
            const std::string &log_file_name, uint64_t thread_id,
            uint32_t db_id, uint32_t memtable_id,
            char *rdma_backing_mem,
            const std::vector<LevelDBLogRecord> &log_records,
            StoCReplicateLogRecordState *replicate_log_record_states,
            StoCLogType log_type) {
        uint32_t req_id = current_req_id_;
        StoCRequestContext context = {};
        context.done = false;
        context.req_type = StoCRequestType::STOC_REPLICATE_LOG_RECORDS;
        context.log_file_name = log_file_name;
        context.thread_id = thread_id;
        context.db_id = db_id;
        context.memtable_id = memtable_id;
        context.replicate_log_record_states = replicate_log_record_states;
        context.log_record_mem = rdma_backing_mem;
        context.log_record_size = nova::LogRecordsSize(log_records);
        context.log_type = log_type;
        request_context_[req_id] = context;
        bool success = rdma_log_writer_->AddRecord(log_file_name, // 每个rdma handler1个的log writer 不过里面的log manager是共享的
                                                   thread_id, db_id, // 1个memtable内所有log record会到同一个log writer
                                                   memtable_id,
                                                   rdma_backing_mem,
                                                   log_records,
                                                   req_id,
                                                   replicate_log_record_states,
                                                   log_type);
        IncrementReqId();
        if (!success) {
            request_context_.erase(req_id);
            return 0;
        }
        NOVA_LOG(DEBUG)
            << fmt::format(
                    "stocclient[{}]: Replicate log record req:{}",
                    stoc_client_id_, req_id);
        return req_id;
    }

    uint32_t
    StoCRDMAClient::InitiateCloseLogFiles(
            const std::vector<std::string> &log_file_name,
            uint32_t dbid) {
        uint32_t req_id = current_req_id_;
        rdma_log_writer_->CloseLogFiles(log_file_name, dbid, req_id, true);
        IncrementReqId();
        return 0;
    }

// RDMA handler中查看isdone
    bool StoCRDMAClient::IsDone(uint32_t req_id, StoCResponse *response,
                                uint64_t *timeout) {
        if (req_id == 0) { // req_id 什么时候会等于0??
            // local bypass.
            if (response) {
                response->is_complete = true;
            }
            return true;
        }

// 如果没有context了(当初根本没有加入?)或者有context 而且done了 那就说明这个req已经做好了

        auto context_it = request_context_.find(req_id);
        if (context_it == request_context_.end()) {
            return true;
        }

        if (context_it->second.done) {
            if (response) { // response 是否为 null 代表 是否需要回复??
                response->is_complete = true;
                response->stoc_file_id = context_it->second.stoc_file_id;
                response->stoc_block_handles = context_it->second.stoc_block_handles;
                response->replication_results = context_it->second.replication_results;
                response->stoc_queue_depth = context_it->second.stoc_queue_depth;
                response->stoc_pending_read_bytes = context_it->second.stoc_pending_read_bytes;
                response->stoc_pending_write_bytes = context_it->second.stoc_pending_write_bytes;
                response->is_ready_to_process_requests = context_it->second.is_ready_for_requests;
            }
            request_context_.erase(req_id);
            return true;
        }

        if (timeout) {
            *timeout = 0;
            if (context_it->second.req_type ==
                StoCRequestType::STOC_READ_BLOCKS) {
                *timeout = context_it->second.size / 100;
            } else if (context_it->second.req_type ==
                       StoCRequestType::STOC_WRITE_SSTABLE) {
                *timeout = context_it->second.size / 7000;
            }
        }
        return false;
    }

//stoc处理cqe。大部分都没懂，需要根据发送和协议等运转机制来看 client端? 需要根据工作类型去看。。
    bool
    StoCRDMAClient::OnRecv(ibv_wc_opcode type, uint64_t wr_id,
                           int remote_server_id,
                           char *buf,
                           uint32_t imm_data, bool *) {
        bool processed = false;
        uint32_t req_id = imm_data;
        switch (type) {
//完成的工作是rdma read类型的
            case IBV_WC_RDMA_READ: {
                uint32_t req_id = leveldb::DecodeFixed32(buf);
                auto context_it = request_context_.find(req_id);
                NOVA_ASSERT(context_it != request_context_.end());
                NOVA_ASSERT(context_it->second.wr_id == wr_id);
                context_it->second.done = true;
                NOVA_LOG(DEBUG) << fmt::format(
                            "stocclient[{}]: Read Log file complete req:{} wr_id:{}",
                            stoc_client_id_, req_id, wr_id);
                processed = true;
            }
                break;
//是rdma send类型的
            case IBV_WC_SEND:
                break;
//是rdma write类型的
            case IBV_WC_RDMA_WRITE: {
//如果是replicate log record
                if (buf[0] == leveldb::StoCRequestType::STOC_REPLICATE_LOG_RECORDS) { // ltc第二次给stoc发了消息 发送了之后
                    req_id = leveldb::DecodeFixed32(buf + 1);
                    auto context_it = request_context_.find(req_id);
                    NOVA_ASSERT(context_it != request_context_.end())
                        << fmt::format(
                                "stocclient[{}]: BUG req:{} wr_id:{} first:{}",
                                stoc_client_id_, req_id, wr_id, buf[0]);
                    auto &context = context_it->second;
                    NOVA_ASSERT(rdma_log_writer_->AckWriteSuccess(
                            context.log_file_name,
                            remote_server_id,
                            wr_id,
                            context.replicate_log_record_states))
                        << fmt::format(
                                "stocclient[{}]: BUG req:{} wr_id:{} first:{}",
                                stoc_client_id_, req_id, wr_id, buf[0]);
                    NOVA_LOG(DEBUG) << fmt::format(
                                "stocclient[{}]: Log record replicated req:{} wr_id:{} first:{}",
                                stoc_client_id_, req_id, wr_id, buf[0]);
                    bool complete = rdma_log_writer_->CheckCompletion( // 每次回来的时候都检查一下是否完成
                            context.log_file_name, context.db_id,
                            context.replicate_log_record_states);
                    if (complete) {
                        context.done = true;
                    }
                    processed = true;
//如果是写远程buf
                } else if (buf[0] == leveldb::RDMA_WRITE_REMOTE_BUF_ALLOCATED) {
                    req_id = leveldb::DecodeFixed32(buf + 1);
                    auto context_it = request_context_.find(req_id);
                    NOVA_ASSERT(context_it != request_context_.end())
                        << fmt::format(
                                "stocclient[{}]: BUG req:{} wr_id:{} first:{}",
                                stoc_client_id_, req_id, wr_id, buf[0]);
                    auto &context = context_it->second;
                    NOVA_LOG(DEBUG) << fmt::format(
                                "stocclient[{}]: req:{} wr_id:{} first:{}",
                                stoc_client_id_, req_id, wr_id, buf[0]);
                    context.done = true;
                    processed = true;
                }
            }
                break;
//如果是rdma recv类型的工作完成
            case IBV_WC_RECV:
            case IBV_WC_RECV_RDMA_WITH_IMM:
                auto context_it = request_context_.find(req_id);
                if (context_it != request_context_.end()) {
                    // I sent this request a while ago and now it is complete.
                    auto &context = context_it->second;
//如果对面发送的send是stoc write sstable response类型的
                    if (buf[0] == STOC_WRITE_SSTABLE_RESPONSE) {
                        NOVA_ASSERT(context.req_type ==
                                    StoCRequestType::STOC_WRITE_SSTABLE);
                        // StoC file handle.
                        uint32_t stoc_file_id = DecodeFixed32(buf + 1);
                        uint64_t stoc_file_offset = leveldb::DecodeFixed64(
                                buf + 5);
                        RDMARequestTask task = {};
                        task.type = RDMA_CLIENT_WRITE_SSTABLE_RESPONSE;
                        task.write_buf = context.backing_mem;
                        task.size = context.size;
                        task.server_id = remote_server_id;
                        task.offset = stoc_file_offset;
                        task.thread_id = req_id;
                        task.local_which = context.local_which;
                        task.remote_which = context.remote_which;
                        rdma_msg_handler_->private_queue_.push_back(task);

                        context.done = false;
                        context.stoc_file_id = stoc_file_id;
                        NOVA_LOG(DEBUG) << fmt::format(
                                    "stocclient[{}]: Write StoC file received off id:{} offset:{} req:{}",
                                    stoc_client_id_, stoc_file_id,
                                    stoc_file_offset,
                                    req_id);
                        processed = true;
//如果对应的本地的任务是stoc read block
                    } else if (context.req_type == StoCRequestType::STOC_READ_BLOCKS) { // 默认写成功了?????!!!!!! ltc收到了来自stoc的带imm的write
//                        if (context.log_file_name.empty()) {
//                            NOVA_ASSERT(
//                                    context.backing_mem[context.size - 1] != 0)
//                                << context.log_file_name;
//                        }

                        // Waiting for WRITEs.
                        if (nova::IsRDMAWRITEComplete(context.backing_mem, context.size)) {
                            NOVA_LOG(DEBUG) << fmt::format(
                                        "stocclient[{}]: Read StoC file blocks complete size:{} req:{}",
                                        stoc_client_id_, context.size, req_id);

                            context.done = true;
                            processed = true;
                        } else {
                            context.done = false;
                        }
//如果对面发送的是stoc persist response类型的
                    } else if (buf[0] ==
                               StoCRequestType::STOC_PERSIST_RESPONSE) {
                        NOVA_ASSERT(context.req_type ==
                                    StoCRequestType::STOC_WRITE_SSTABLE);
                        uint32_t msg_size = 1;
                        uint32_t stoc_block_handles = DecodeFixed32(
                                buf + msg_size);
                        msg_size += 4;
                        std::string rids;
                        for (int i = 0; i < stoc_block_handles; i++) {
                            StoCBlockHandle rh = {};
                            rh.DecodeHandle(buf + msg_size);
                            context.stoc_block_handles.push_back(rh);
                            msg_size += StoCBlockHandle::HandleSize();
                            rids += fmt::format("{},", rh.stoc_file_id);
                        }
                        NOVA_ASSERT(stoc_block_handles == 1);
                        context.done = true;
                        NOVA_LOG(DEBUG) << fmt::format(
                                    "stocclient[{}]: Persist StoC file received handles:{} rids:{} req:{}",
                                    stoc_client_id_, stoc_block_handles, rids,
                                    req_id);
                        processed = true;
//如果对面发送的是stoc read stats response类型的
                    } else if (buf[0] ==
                               StoCRequestType::STOC_READ_STATS_RESPONSE) {
                        context.stoc_queue_depth = leveldb::DecodeFixed64(
                                buf + 1);
                        context.stoc_pending_read_bytes = leveldb::DecodeFixed64(
                                buf + 9);
                        context.stoc_pending_write_bytes = leveldb::DecodeFixed64(
                                buf + 17);
                        context.done = true;
                        processed = true;
//如果对面发送的是stoc alloc log buffer success类型的
                    } else if (buf[0] ==
                               StoCRequestType::STOC_ALLOCATE_LOG_BUFFER_SUCC) { // ltc第一次发送log给stoc 第一个rtt结束 stoc发送地址和大小
                        StoCLogType log_type = StoCLogType::STOC_LOG_DRAM; //= static_cast<StoCLogType>(buf[1])
                        if(buf[1] == StoCLogType::STOC_LOG_DRAM){
                            log_type = StoCLogType::STOC_LOG_DRAM;
                        }else if(buf[1] == StoCLogType::STOC_LOG_PM){
                            log_type = StoCLogType::STOC_LOG_PM;
                        }else if(buf[1] == StoCLogType::STOC_LOG_DISK){
                            log_type = StoCLogType::STOC_LOG_DISK;// tbd
                        }

                        uint64_t base = leveldb::DecodeFixed64(buf + 2);
                        uint64_t size = leveldb::DecodeFixed64(buf + 10);

                        RDMARequestTask task = {};
                        task.type = RDMA_CLIENT_ALLOCATE_LOG_BUFFER_SUCC;
                        task.server_id = remote_server_id;
                        task.log_type = log_type;
                        task.log_file_name = context.log_file_name;
                        task.offset = base;
                        task.size = size;
                        task.rdma_log_record_backing_mem = context.log_record_mem;
                        task.write_size = context.log_record_size;
                        task.thread_id = req_id;
                        task.replicate_log_record_states = context.replicate_log_record_states;
                        rdma_msg_handler_->private_queue_.push_back(task);
                        NOVA_LOG(DEBUG) << fmt::format(
                                    "stocclient[{}]: Allocate log buffer success req:{}",
                                    stoc_client_id_, req_id);
                        processed = true;
//如果对面发送的是rdma write remote buf allocated类型的
                    } else if (buf[0] ==
                               StoCRequestType::RDMA_WRITE_REMOTE_BUF_ALLOCATED) {
                        uint64_t remote_buf = leveldb::DecodeFixed64(buf + 1);
                        uint64_t size = leveldb::DecodeFixed64(buf + 9);
                        NOVA_ASSERT(size == context.size);

                        RDMARequestTask task = {};
                        task.type = RDMA_CLIENT_RDMA_WRITE_REMOTE_BUF_ALLOCATED;
                        task.server_id = remote_server_id;
                        task.offset = remote_buf;
                        task.size = size;
                        task.write_buf = context.backing_mem;
                        task.write_size = context.size;
                        task.thread_id = req_id;
                        rdma_msg_handler_->private_queue_.push_back(task);
                        NOVA_LOG(DEBUG) << fmt::format(
                                    "stocclient[{}]: Allocate log buffer success req:{}",
                                    stoc_client_id_, req_id);
                        processed = true;
//对面发送的是stoc query log files response类型的
                    } else if (buf[0] ==
                               StoCRequestType::STOC_QUERY_LOG_FILES_RESPONSE) {
                        uint32_t read_size = 1;
                        uint32_t size = leveldb::DecodeFixed32(buf + read_size);
                        read_size += 4;
                        for (int i = 0; i < size; i++) {
                            std::string log;
                            read_size += leveldb::DecodeStr(buf + read_size,
                                                            &log);
                            uint64_t offset = leveldb::DecodeFixed64(
                                    buf + read_size);
                            read_size += 8;
                            (*context.logfile_offset)[log] = offset;
                        }
                        context.done = true;
                        processed = true;
//对面发送的是stoc filename stocfileid response类型的
                    } else if (buf[0] ==
                               StoCRequestType::STOC_FILENAME_STOCFILEID_RESPONSE) {
                        context.done = true;
                        processed = true;
//对面发送的是stoc compaction response类型的
                    } else if (buf[0] ==
                               StoCRequestType::STOC_COMPACTION_RESPONSE) { // 收到了stoc发送的compaction的回复
                        uint32_t num_outputs = leveldb::DecodeFixed32(buf + 1);
                        Slice outputs(buf + 5,
                                      nova::NovaConfig::config->max_msg_size);
                        for (int i = 0; i < num_outputs; i++) { // 得到压缩产生的文件名等数据
                            FileMetaData *meta = new FileMetaData;
                            NOVA_ASSERT(meta->Decode(&outputs, false));
                            context.compaction->outputs.push_back(meta);
                        }
                        context.done = true;
                        processed = true;
                    } else if (buf[0] ==
                               StoCRequestType::STOC_IS_READY_FOR_REQUESTS_RESPONSE) {
                        bool is_ready = leveldb::DecodeBool(buf + 1);
                        context.is_ready_for_requests = is_ready;
                        context.done = true;
                        processed = true;
                    } else if (buf[0] == StoCRequestType::STOC_REPLICATE_SSTABLES_RESPONSE) {
                        Slice tmp(buf + 1, nova::NovaConfig::config->max_msg_size);
                        uint32_t size;
                        NOVA_ASSERT(DecodeFixed32(&tmp, &size));
                        for (int i = 0; i < size; i++) {
                            ReplicationPair pair = {};
                            NOVA_ASSERT(pair.Decode(&tmp));
                            context.replication_results.push_back(pair);
                        }
                        context.done = true;
                        processed = true;
                    }
                }
                break;
        }
        return processed;
    }

}
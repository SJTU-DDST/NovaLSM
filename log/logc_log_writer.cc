
//
// Created by Haoyu Huang on 12/23/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include "common/nova_config.h"
#include "logc_log_writer.h"


namespace leveldb {


// 每个worker一个的logc，用于向stoc中写入wal
    // Create a writer that will append data to "*dest".
// "*dest" must be initially empty.
// "*dest" must remain live while this Writer is in use.
    LogCLogWriter::LogCLogWriter(nova::NovaRDMABroker *rdma_broker,
                                 MemManager *mem_manager,
                                 nova::StoCInMemoryLogFileManager *log_manager)
            : rdma_broker_(rdma_broker), mem_manager_(mem_manager),
              log_manager_(log_manager) {
    }

// 建立log_file_name到meta的映射 并且把log record填进去 填好要发送的buf中的信息
    void LogCLogWriter::Init(const std::string &log_file_name,
                             uint64_t thread_id,
                             const std::vector<LevelDBLogRecord> &log_records,
                             char *backing_buf) {
        // 先找这个logfile
        auto it = logfile_last_buf_.find(log_file_name);
        // 没有找到的话就建立一些结构
        if (it == logfile_last_buf_.end()) {
            LogFileMetadata meta = {};
            meta.stoc_bufs = new LogFileBuf[nova::NovaConfig::config->servers.size()]; // 建立这个logFileMetadata元数据的作用是什么???
            for (int i = 0;
                 i <
                 nova::NovaConfig::config->servers.size(); i++) {
                meta.stoc_bufs[i].base = 0;
                meta.stoc_bufs[i].offset = 0;
                meta.stoc_bufs[i].size = 0;
            }
            logfile_last_buf_[log_file_name] = meta;
        }
        uint32_t size = 0;
        for (const auto &record : log_records) {
            size += nova::EncodeLogRecord(backing_buf + size, record);
        }
    }

// 接收到对应的stoc发来的 已经分配log buffer的信息
    void LogCLogWriter::AckAllocLogBuf(const std::string &log_file_name,
                                       int stoc_server_id, uint64_t offset,
                                       uint64_t size,
                                       char *backing_mem,
                                       uint32_t log_record_size,
                                       uint32_t client_req_id,
                                       StoCReplicateLogRecordState *replicate_log_record_states) {
        log_manager_->AddRemoteBuf(log_file_name, stoc_server_id, offset);
        replicate_log_record_states[stoc_server_id].result = StoCReplicateLogRecordResult::ALLOC_SUCCESS;
        auto meta = &logfile_last_buf_[log_file_name];
        meta->stoc_bufs[stoc_server_id].base = offset;
        meta->stoc_bufs[stoc_server_id].size = size;
        meta->stoc_bufs[stoc_server_id].offset = 0;
        meta->stoc_bufs[stoc_server_id].is_initializing = false;
        char *sendbuf = rdma_broker_->GetSendBuf(stoc_server_id);
        sendbuf[0] = leveldb::StoCRequestType::STOC_REPLICATE_LOG_RECORDS;
        leveldb::EncodeFixed32(sendbuf + 1, client_req_id);
        replicate_log_record_states[stoc_server_id].rdma_wr_id = rdma_broker_->PostWrite(
                backing_mem, log_record_size,
                stoc_server_id,
                meta->stoc_bufs[stoc_server_id].base +
                meta->stoc_bufs[stoc_server_id].offset, /*is_remote_offset=*/
                false, 0); //无imm 所以直接写入 stoc端没有反应
        meta->stoc_bufs[stoc_server_id].offset += log_record_size;
        replicate_log_record_states[stoc_server_id].result = StoCReplicateLogRecordResult::WAIT_FOR_WRITE;
    }

// 本地server read write replicate log record后，更改本地任务的状态
    bool LogCLogWriter::AckWriteSuccess(const std::string &log_file_name,
                                        int remote_sid, uint64_t wr_id,
                                        StoCReplicateLogRecordState *replicate_log_record_states) {
        StoCReplicateLogRecordState &state = replicate_log_record_states[remote_sid];
        if (state.rdma_wr_id == wr_id &&
            state.result == StoCReplicateLogRecordResult::WAIT_FOR_WRITE) {
            state.result = StoCReplicateLogRecordResult::WRITE_SUCCESS;
            return true;
        }
        return false;
    }

// rdmaclient调用写wal日志 
    bool
    LogCLogWriter::AddRecord(const std::string &log_file_name,
                             uint64_t thread_id,
                             uint32_t dbid,
                             uint32_t memtableid,
                             char *rdma_backing_buf,
                             const std::vector<LevelDBLogRecord> &log_records,
                             uint32_t client_req_id,
                             StoCReplicateLogRecordState *replicate_log_record_states) {
        uint32_t cfgid = replicate_log_record_states[0].cfgid;
        auto cfg = nova::NovaConfig::config->cfgs[cfgid];
        nova::LTCFragment *frag = cfg->fragments[dbid];
        if (frag->log_replica_stoc_ids.empty()) { // 找到对应的dbwal日志的位置
            return true;
        }
        // If one of the log buf is intializing, return false. ??????? isinitializing是什么? 这个buffer如果是现在分配的 那就要先初始化
        Init(log_file_name, thread_id, log_records, rdma_backing_buf);
        for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
            uint32_t stoc_server_id = cfg->stoc_servers[frag->log_replica_stoc_ids[i]];
            auto &it = logfile_last_buf_[log_file_name];
            if (it.stoc_bufs[stoc_server_id].is_initializing) { // 因为后面会做初始化 这里保证第一次进来的线程(会做初始化)会通过 但是再后面的如果检测到正在初始化就会停住
                return false;
            }
        }
        uint32_t log_record_size = nova::LogRecordsSize(log_records);
        for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
            uint32_t stoc_server_id = cfg->stoc_servers[frag->log_replica_stoc_ids[i]];
            auto &it = logfile_last_buf_[log_file_name]; // it代表要发送到的stoc对应的那个结构
            if (it.stoc_bufs[stoc_server_id].base == 0) { // base = 0 标识暂时什么都没做过?
                it.stoc_bufs[stoc_server_id].is_initializing = true; // 所以先要initialize 
                // Allocate a new buf.
                char *send_buf = rdma_broker_->GetSendBuf(stoc_server_id);
                char *buf = send_buf;
                buf[0] = StoCRequestType::STOC_ALLOCATE_LOG_BUFFER; // 要求先分配 ltc向stoc发送要求先分配log 
                buf++;
                leveldb::EncodeFixed32(buf, log_file_name.size());
                buf += 4;
                memcpy(buf, log_file_name.data(), log_file_name.size());
                replicate_log_record_states[stoc_server_id].result = StoCReplicateLogRecordResult::WAIT_FOR_ALLOC; // 更新状态
                rdma_broker_->PostSend(
                        send_buf, 1 + 4 + log_file_name.size(),
                        stoc_server_id, client_req_id);
            } else {
                NOVA_ASSERT(!it.stoc_bufs[stoc_server_id].is_initializing);
                NOVA_ASSERT(
                        it.stoc_bufs[stoc_server_id].offset + log_record_size <=
                        it.stoc_bufs[stoc_server_id].size);
                // WRITE.
                char *sendbuf = rdma_broker_->GetSendBuf(stoc_server_id);
                sendbuf[0] = leveldb::StoCRequestType::STOC_REPLICATE_LOG_RECORDS;
                leveldb::EncodeFixed32(sendbuf + 1, client_req_id);
                replicate_log_record_states[stoc_server_id].rdma_wr_id = rdma_broker_->PostWrite(
                        rdma_backing_buf, log_record_size, stoc_server_id,
                        it.stoc_bufs[stoc_server_id].base +
                        it.stoc_bufs[stoc_server_id].offset,
                        false, 0);
                it.stoc_bufs[stoc_server_id].offset += log_record_size;
                replicate_log_record_states[stoc_server_id].result = StoCReplicateLogRecordResult::WAIT_FOR_WRITE;
            }
        }
        return true;
    }

//检查本地server的replicate log是否都已经成功完成
    bool LogCLogWriter::CheckCompletion(const std::string &log_file_name,
                                        uint32_t dbid,
                                        StoCReplicateLogRecordState *replicate_log_record_states) {
        uint32_t cfg_id = replicate_log_record_states[0].cfgid;
        auto cfg = nova::NovaConfig::config->cfgs[cfg_id];
        nova::LTCFragment *frag = cfg->fragments[dbid];
        // Pull all pending writes.
        int acks = 0;
        int total_states = 0;
        for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
            uint32_t stoc_server_id = cfg->stoc_servers[frag->log_replica_stoc_ids[i]];
            switch (replicate_log_record_states[stoc_server_id].result) {
                case StoCReplicateLogRecordResult::REPLICATE_LOG_RECORD_NONE:
                    break;
                case StoCReplicateLogRecordResult::WAIT_FOR_ALLOC:
                    total_states += 1;
                    break;
                case StoCReplicateLogRecordResult::WAIT_FOR_WRITE:
                    total_states += 1;
                    break;
                case StoCReplicateLogRecordResult::ALLOC_SUCCESS:
                    total_states += 1;
                    break;
                case StoCReplicateLogRecordResult::WRITE_SUCCESS:
                    total_states += 1;
                    acks++;
                    break;
            }
        }
        NOVA_ASSERT(total_states == frag->log_replica_stoc_ids.size());
        return acks == frag->log_replica_stoc_ids.size();
    }

    Status
    LogCLogWriter::CloseLogFiles(const std::vector<std::string> &log_file_name,
                                 uint32_t dbid, uint32_t client_req_id) {
        auto cfgid = nova::NovaConfig::config->current_cfg_id.load();
        auto cfg = nova::NovaConfig::config->cfgs[cfgid];
        nova::LTCFragment *frag = nova::NovaConfig::config->cfgs[cfgid]->fragments[dbid];
        for (const auto &logfile : log_file_name) {
            LogFileMetadata *meta = &logfile_last_buf_[logfile];
            delete meta->stoc_bufs;
            logfile_last_buf_.erase(logfile);
        }
        log_manager_->DeleteLogBuf(log_file_name);
        for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
            uint32_t stoc_server_id = cfg->stoc_servers[frag->log_replica_stoc_ids[i]];
            char *send_buf = rdma_broker_->GetSendBuf(stoc_server_id);
            int size = 0;
            send_buf[size] = StoCRequestType::STOC_DELETE_LOG_FILE;
            size++;
            size += leveldb::EncodeFixed32(send_buf + size, log_file_name.size());
            for (auto &name : log_file_name) {
                size += leveldb::EncodeStr(send_buf + size, name);
            }
            rdma_broker_->PostSend(send_buf, size, stoc_server_id, client_req_id);
        }
        return Status::OK();
    }
}
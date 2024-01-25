
//
// Created by Haoyu Huang on 1/8/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include <fmt/core.h>
#include <semaphore.h>
#include <db/compaction.h>
#include <db/table_cache.h>

#include "db/filename.h"
#include "rdma_server.h"
#include "ltc/stoc_client_impl.h"
#include "common/nova_config.h"
#include "common/nova_common.h"

namespace nova {
    namespace {
        uint64_t to_req_id(uint32_t remote_sid, uint32_t stoc_req_id) {
            uint64_t req_id = 0;
            req_id = ((uint64_t) remote_sid) << 32;
            return req_id + stoc_req_id;
        }

        uint64_t to_stoc_req_id(uint64_t req_id) {
            return (uint32_t) (req_id);
        }
    }

//处理当前的这个worker的rdma的客户端请求
    RDMAServerImpl::RDMAServerImpl(rdmaio::RdmaCtrl *rdma_ctrl,
                                   NovaMemManager *mem_manager,
                                   NovaPMManager *pm_manager,
                                   leveldb::StocPersistentFileManager *stoc_file_manager,
                                   StoCInMemoryLogFileManager *log_manager,
                                   uint32_t thread_id,
                                   bool is_compaction_thread,
                                   RDMAAdmissionCtrl *admission_control)
            : rdma_ctrl_(rdma_ctrl),
              mem_manager_(mem_manager),
              pm_manager_(pm_manager),
              stoc_file_manager_(stoc_file_manager),
              log_manager_(log_manager), thread_id_(thread_id),
              is_compaction_thread_(is_compaction_thread),
              admission_control_(admission_control) {
        current_worker_id_ = thread_id;
    }

// 添加storage worker完成的任务 交给stoc的任务完成了把任务推进去
    void RDMAServerImpl::AddCompleteTasks(
            const std::vector<nova::ServerCompleteTask> &tasks) {
        mutex_.lock();
        for (auto &task : tasks) {
            public_cq_.push_back(task);
        }
        mutex_.unlock();
    }

    void RDMAServerImpl::AddCompleteTask(
            const nova::ServerCompleteTask &task) {
        mutex_.lock();
        public_cq_.push_back(task);
        mutex_.unlock();
    }

//选一个前台
    void RDMAServerImpl::AddFGStorageTask(const nova::StorageTask &task) {
        uint32_t id =
                fg_storage_worker_seq_id_.fetch_add(1,
                                                    std::memory_order_relaxed) %
                fg_storage_workers_.size();
        fg_storage_workers_[id]->AddTask(task);
    }

//选一个后台storage worker，把任务加进去
    void RDMAServerImpl::AddBGStorageTask(const nova::StorageTask &task) {
        uint32_t id =
                bg_storage_worker_seq_id_.fetch_add(1,
                                                    std::memory_order_relaxed) %
                bg_storage_workers_.size();
        bg_storage_workers_[id]->AddTask(task);
    }

    void
    RDMAServerImpl::AddCompactionStorageTask(const nova::StorageTask &task) {
        uint32_t id =
                compaction_storage_worker_seq_id_.fetch_add(1,
                                                            std::memory_order_relaxed) %
                compaction_storage_workers_.size();
        compaction_storage_workers_[id]->AddTask(task);
    }

    int RDMAServerImpl::ProcessCompletionQueue() {
        int nworks = 0;
        mutex_.lock();

        // 取出public中的任务
        while (!public_cq_.empty()) {
            auto &task = public_cq_.front();
            private_cq_.push_back(task);
            public_cq_.pop_front();
        }
        mutex_.unlock();
        nworks = private_cq_.size();

        auto it = private_cq_.begin();
        while (it != private_cq_.end()) {
            const auto &task = *it;
            NOVA_ASSERT(task.remote_server_id != -1);
            if (!admission_control_->CanIssueRequest(task.remote_server_id)) {
                it++;
                continue;
            }

            if (task.request_type == leveldb::STOC_FILENAME_STOCFILEID) {
                char *send_buf = rdma_broker_->GetSendBuf(
                        task.remote_server_id);
                send_buf[0] = leveldb::StoCRequestType::STOC_FILENAME_STOCFILEID_RESPONSE;
                rdma_broker_->PostSend(send_buf, 1, task.remote_server_id,
                                       task.stoc_req_id);
            } else if (task.request_type == leveldb::STOC_ALLOCATE_LOG_BUFFER) { // ltc第一次发log的申请 stoc端完成了log文件的创建 
                char *send_buf = rdma_broker_->GetSendBuf( // 地址和大小
                        task.remote_server_id);
                send_buf[0] = leveldb::StoCRequestType::STOC_ALLOCATE_LOG_BUFFER_SUCC;
                send_buf[1] = task.log_type;
                leveldb::EncodeFixed64(send_buf + 2, (uint64_t) task.rdma_buf);
                leveldb::EncodeFixed64(send_buf + 10,
                                       NovaConfig::config->max_stoc_file_size);
                rdma_broker_->PostSend(send_buf, 1 + 1 + 8 + 8,
                                       task.remote_server_id,
                                       task.stoc_req_id);
            } else if(task.request_type == leveldb::STOC_WRITE_LOG_BUFFER_DISK){ // disk模式的log stoc处理了第一次发送过来的消息 返回
                char *send_buf = rdma_broker_->GetSendBuf(
                        task.remote_server_id);
                send_buf[0] = leveldb::StoCRequestType::STOC_WRITE_LOG_BUFFER_DISK_RESPONSE; // 1
                leveldb::EncodeFixed32(send_buf + 1,
                                       task.stoc_file_id); // 1 + 4
                leveldb::EncodeFixed64(send_buf + 5, task.stoc_file_buf_offset); // 1 + 4 + 8 相当于rdma_buf 不过这里会变化
                leveldb::EncodeFixed64(send_buf + 13,
                                       NovaConfig::config->max_stoc_file_size); // 1 + 4 + 8 + 8
                rdma_broker_->PostSend(send_buf, 1 + 4 + 8 + 8,
                                       task.remote_server_id,
                                       task.stoc_req_id);
            }else if (task.request_type == leveldb::RDMA_WRITE_REQUEST) {
                char *sendbuf = rdma_broker_->GetSendBuf(task.remote_server_id);
                sendbuf[0] =
                        leveldb::StoCRequestType::RDMA_WRITE_REMOTE_BUF_ALLOCATED;
                leveldb::EncodeFixed64(sendbuf + 1, (uint64_t) task.rdma_buf);
                leveldb::EncodeFixed64(sendbuf + 9, (uint64_t) task.size);
                rdma_broker_->PostSend(sendbuf, 1 + 8 + 8,
                                       task.remote_server_id,
                                       task.stoc_req_id);
            } else if (task.request_type == leveldb::STOC_WRITE_SSTABLE) {
                char *sendbuf = rdma_broker_->GetSendBuf(task.remote_server_id);
                sendbuf[0] =
                        leveldb::StoCRequestType::STOC_WRITE_SSTABLE_RESPONSE;
                leveldb::EncodeFixed32(sendbuf + 1,
                                       task.stoc_file_id);
                leveldb::EncodeFixed64(sendbuf + 5, task.stoc_file_buf_offset);
                rdma_broker_->PostSend(sendbuf, 13, task.remote_server_id,
                                       task.stoc_req_id);
            } else if (task.request_type == leveldb::STOC_READ_STATS) {
                char *sendbuf = rdma_broker_->GetSendBuf(task.remote_server_id);
                sendbuf[0] =
                        leveldb::StoCRequestType::STOC_READ_STATS_RESPONSE;
                leveldb::EncodeFixed64(sendbuf + 1,
                                       NovaGlobalVariables::global.stoc_queue_depth);
                leveldb::EncodeFixed64(sendbuf + 9,
                                       NovaGlobalVariables::global.stoc_pending_disk_reads);
                leveldb::EncodeFixed64(sendbuf + 17,
                                       NovaGlobalVariables::global.stoc_pending_disk_writes);
                rdma_broker_->PostSend(sendbuf, 13, task.remote_server_id,
                                       task.stoc_req_id);
            } else if (task.request_type == // stoc端已经将ltc要求的东西读出来了 发回去
                       leveldb::StoCRequestType::STOC_READ_BLOCKS) { // 用write原语发过去
                char *sendbuf = rdma_broker_->GetSendBuf(task.remote_server_id); // rdma的大buff中的对应的东西 这个是为了回收资源
                uint64_t wr_id = rdma_broker_->PostWrite(task.rdma_buf, // rdma_buf 是本地申请的 装了读出来的数据的buf 这里也需要区分本地是什么 这里应该是stoc向ltc发送
                                                         task.size,
                                                         task.remote_server_id,
                                                         task.ltc_mr_offset, // 远程的内存
                                                         false,
                                                         task.stoc_req_id, task.ispmfile ? 1: 0 , 0); //暂时先不改全部照旧 之后会去掉read的buf 如果是pmfile 那就用1 如果是缓冲区那就0
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("Read {} s:{} req:{} mr:{} size:{} off:{}", task.stoc_block_handle.DebugString(),
                                   task.remote_server_id, task.stoc_req_id, task.ltc_mr_offset, task.size, (uint64_t) (task.rdma_buf));
                sendbuf[0] = leveldb::StoCRequestType::STOC_READ_BLOCKS;
                leveldb::EncodeFixed64(sendbuf + 1, wr_id);
                leveldb::EncodeFixed32(sendbuf + 9, task.stoc_block_handle.size);
                leveldb::EncodeFixed64(sendbuf + 13, (uint64_t) (task.rdma_buf));
                leveldb::EncodeBool(sendbuf + 21, task.ispmfile);
            } else if (task.request_type ==
                       leveldb::StoCRequestType::STOC_PERSIST) {
                char *sendbuf = rdma_broker_->GetSendBuf(task.remote_server_id);
                sendbuf[0] = leveldb::StoCRequestType::STOC_PERSIST_RESPONSE;
                uint32_t msg_size = 1;
                leveldb::EncodeFixed32(sendbuf + msg_size,
                                       task.stoc_block_handles.size());
                msg_size += 4;
                for (int i = 0; i < task.stoc_block_handles.size(); i++) {
                    task.stoc_block_handles[i].EncodeHandle(sendbuf + msg_size);
                    msg_size += leveldb::StoCBlockHandle::HandleSize();
                }
                rdma_broker_->PostSend(sendbuf, msg_size, task.remote_server_id,
                                       task.stoc_req_id);
            } else if(task.request_type == 
                      leveldb::StoCRequestType::STOC_LOG_PERSIST){
                char *sendbuf = rdma_broker_->GetSendBuf(task.remote_server_id);
                sendbuf[0] = leveldb::StoCRequestType::STOC_LOG_PERSIST_RESPONSE;
                // 对面用这些信息应该可以找到对应的log什么的 核心是request id
                rdma_broker_->PostSend(sendbuf, 1, task.remote_server_id, // 只需要发送一个在干啥就好
                                        task.stoc_req_id);
            } else if (task.request_type ==
                       leveldb::StoCRequestType::STOC_COMPACTION) {
                char *sendbuf = rdma_broker_->GetSendBuf(task.remote_server_id);
                sendbuf[0] = leveldb::StoCRequestType::STOC_COMPACTION_RESPONSE;
                uint32_t msg_size = 1;
                msg_size += leveldb::EncodeFixed32(sendbuf + msg_size,
                                                   task.compaction_state->outputs.size());
                for (auto &out : task.compaction_state->outputs) {
                    msg_size += out.Encode(sendbuf + msg_size);
                }
                rdma_broker_->PostSend(sendbuf, msg_size, task.remote_server_id,
                                       task.stoc_req_id);
                task.compaction_request->FreeMemoryStoC();
                delete task.compaction_request;
                delete task.compaction_state->compaction;
                delete task.compaction_state;
            } else if (task.request_type ==
                       leveldb::StoCRequestType::STOC_IS_READY_FOR_REQUESTS) {
                char *sendbuf = rdma_broker_->GetSendBuf(task.remote_server_id);
                uint32_t msg_size = 1;
                sendbuf[0] =
                        leveldb::StoCRequestType::STOC_IS_READY_FOR_REQUESTS_RESPONSE;
                msg_size += leveldb::EncodeBool(sendbuf + 1,
                                                NovaGlobalVariables::global.is_ready_to_process_requests);
                rdma_broker_->PostSend(sendbuf, msg_size, task.remote_server_id,
                                       task.stoc_req_id);
            } else if (task.request_type ==
                       leveldb::StoCRequestType::STOC_REPLICATE_SSTABLES) {
                char *sendbuf = rdma_broker_->GetSendBuf(task.remote_server_id);
                uint32_t msg_size = 1;
                sendbuf[0] = leveldb::StoCRequestType::STOC_REPLICATE_SSTABLES_RESPONSE;
                msg_size += leveldb::EncodeFixed32(sendbuf + msg_size, task.replication_results.size());
                for (auto result : task.replication_results) {
                    msg_size += result.Encode(sendbuf + msg_size);
                }
                rdma_broker_->PostSend(sendbuf, msg_size, task.remote_server_id, task.stoc_req_id);
                NOVA_LOG(rdmaio::DEBUG) << "rdma replication complete";
            } else {
                NOVA_ASSERT(false) << task.request_type;
            }
            NOVA_LOG(DEBUG) << fmt::format(
                        "CCServer[{}]: Completed Request ss:{} req:{} type:{}",
                        thread_id_, task.remote_server_id, task.stoc_req_id,
                        task.request_type);
            it = private_cq_.erase(it);
        }
        return nworks;
    }

    RDMAWriteHandler::RDMAWriteHandler(
            const std::vector<DBMigration *> &destination_migration_threads)
            : destination_migration_threads_(destination_migration_threads) {}

    void RDMAWriteHandler::Handle(char *buf, uint32_t size) {
        NOVA_ASSERT(buf[0] == leveldb::StoCRequestType::LTC_MIGRATION);
        int value = DBMigration::migration_seq_id_ %
                    destination_migration_threads_.size();
        DBMigration::migration_seq_id_ += 1;
        destination_migration_threads_[value]->AddDestMigrateDB(buf, size);
    }

//rdmaserver处理cqe server端?? 各种任务的后续工作 需要根据工作类型去看。。
    // No need to flush RDMA requests since Flush will be done after all requests are processed in a receive queue.
    bool
    RDMAServerImpl::ProcessRDMAWC(ibv_wc_opcode type, uint64_t wr_id,
                                  int remote_server_id, char *buf,
                                  uint32_t imm_data,
                                  bool *) {
        bool processed = false;
        switch (type) {
//完成的工作是rdma send类型的
            case IBV_WC_SEND:
                break;
//完成的工作是rdma write类型的
            case IBV_WC_RDMA_WRITE:
//如果是stoc read blocks类型的，释放掉之前的空间
                if (buf[0] == leveldb::StoCRequestType::STOC_READ_BLOCKS) { // recover 的 server端的 write的wc stoc端响应ltc端read block之后 留下的write with imm 的结束 用于清理资源 第一个rtt结束
                    uint64_t written_wr_id = leveldb::DecodeFixed64(buf + 1);
                    if (written_wr_id == wr_id) {
                        uint32_t size = leveldb::DecodeFixed32(buf + 9);
                        uint64_t allocated_buf_int = leveldb::DecodeFixed64(
                                buf + 13);
                        char *allocated_buf = (char *) (allocated_buf_int);
                        bool ispmfile = leveldb::DecodeBool(buf + 21);
                        if(ispmfile){ // 如果是pm的文件的话 不需要释放
                            ;
                        }else{
                            uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                                    size);
                            mem_manager_->FreeItem(thread_id_, allocated_buf, scid);                            
                        }
                        processed = true;
                        NOVA_LOG(DEBUG) << fmt::format(
                                    "rdma-server[{}]: imm:{} type:{} allocated buf:{} size:{} wr:{}.",
                                    thread_id_,
                                    imm_data,
                                    buf[0], allocated_buf_int, size, wr_id);
                    }
                }
                break;
            case IBV_WC_RDMA_READ:
                break;
// 如果完成的工作是rdma recv类型的 什么时候
            case IBV_WC_RECV:
            case IBV_WC_RECV_RDMA_WITH_IMM:
                // imm_data是来自对面的 rdmaclient的 req_id
                uint32_t stoc_req_id = imm_data;
                uint64_t req_id = to_req_id(remote_server_id, stoc_req_id); // req_id 的规律是什么
                auto context_it = request_context_map_.find(req_id);
                if (context_it != request_context_map_.end()) {
                    auto &context = context_it->second;
                    // Waiting for writes.
                    if (context.request_type ==
                        leveldb::StoCRequestType::STOC_WRITE_SSTABLE) {
                        if (IsRDMAWRITEComplete(
                                (char *) context.stoc_file_buf_offset,
                                context.size)) {
                            NOVA_ASSERT(stoc_file_manager_->FindStoCFile(
                                    context.stoc_file_id)->MarkOffsetAsWritten(
                                    context.stoc_file_id,
                                    context.stoc_file_buf_offset))
                                << fmt::format(
                                        "rdma-server[{}]: Write StoC file failed id:{} offset:{} creq_id:{} req_id:{}",
                                        thread_id_, context.stoc_file_id,
                                        context.stoc_file_buf_offset,
                                        stoc_req_id,
                                        req_id);
                            processed = true;

                            NOVA_LOG(DEBUG) << fmt::format(
                                        "rdma-server[{}]: Write StoC file complete id:{} offset:{} creq_id:{} req_id:{}",
                                        thread_id_, context.stoc_file_id,
                                        context.stoc_file_buf_offset,
                                        stoc_req_id,
                                        req_id);

                            StorageTask task = {};
                            task.request_type = leveldb::StoCRequestType::STOC_PERSIST;
                            task.remote_server_id = remote_server_id;
                            task.rdma_server_thread_id = thread_id_;
                            task.stoc_req_id = stoc_req_id;
                            task.internal_type = context.internal_type;
                            leveldb::SSTableStoCFilePair pair = {};
                            pair.stoc_file_id = context.stoc_file_id;
                            pair.sstable_name = context.sstable_name;
                            task.persist_pairs.push_back(pair);
                            AddBGStorageTask(task);
                            request_context_map_.erase(req_id);
                        }
                    } else if(context.request_type == 
                        leveldb::StoCRequestType::STOC_WRITE_LOG_BUFFER_DISK){
                        if (IsRDMAWRITEComplete(
                                (char *) context.stoc_file_buf_offset,
                                context.size)){ // write写好了
                            NOVA_ASSERT(stoc_file_manager_->FindStoCFile(
                                    context.stoc_file_id)->MarkOffsetAsWritten(
                                    context.stoc_file_id,
                                    context.stoc_file_buf_offset))
                                << fmt::format(
                                        "rdma-server[{}]: Write StoC file failed id:{} offset:{} creq_id:{} req_id:{}",
                                        thread_id_, context.stoc_file_id,
                                        context.stoc_file_buf_offset,
                                        stoc_req_id,
                                        req_id);
                            processed = true;

                            NOVA_LOG(DEBUG) << fmt::format(
                                        "rdma-server[{}]: Write StoC file complete id:{} offset:{} creq_id:{} req_id:{}",
                                        thread_id_, context.stoc_file_id,
                                        context.stoc_file_buf_offset,
                                        stoc_req_id,
                                        req_id);
                            
                            StorageTask task = {};
                            task.request_type = leveldb::StoCRequestType::STOC_LOG_PERSIST; // DISK log的persist任务
                            task.remote_server_id = remote_server_id;
                            task.rdma_server_thread_id = thread_id_;
                            task.stoc_req_id = stoc_req_id;
                            task.internal_type = context.internal_type;
                            leveldb::SSTableStoCFilePair pair = {};
                            pair.stoc_file_id = context.stoc_file_id;
                            pair.sstable_name = context.sstable_name;
                            task.persist_pairs.push_back(pair);
                            AddBGStorageTask(task);
                            request_context_map_.erase(req_id);
                        }
                    }else if (context.request_type ==
                               leveldb::StoCRequestType::RDMA_WRITE_REMOTE_BUF_ALLOCATED) {
                        rdma_write_handler_->Handle(context.buf, context.size);
                        request_context_map_.erase(req_id);
                        processed = true;
                    }
                }

                if (buf[0] == leveldb::StoCRequestType::STOC_READ_STATS) {
                    processed = true;
                    ServerCompleteTask ct = {};
                    ct.remote_server_id = remote_server_id;
                    ct.request_type = leveldb::StoCRequestType::STOC_READ_STATS;
                    ct.stoc_req_id = stoc_req_id;
                    private_cq_.push_back(ct);
                } else if (buf[0] ==
                           leveldb::StoCRequestType::STOC_DELETE_TABLES) {
                    uint32_t msg_size = 1;
                    uint32_t nfiles = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;

                    for (int i = 0; i < nfiles; i++) {
                        std::string sstable_id;
                        msg_size += leveldb::DecodeStr(buf + msg_size,
                                                       &sstable_id);
                        uint32_t stoc_file_id = leveldb::DecodeFixed32(
                                buf + msg_size);
                        msg_size += 4;
                        stoc_file_manager_->DeleteSSTable(sstable_id);
                        leveldb::FileType type;
                        NOVA_ASSERT(leveldb::ParseFileName(sstable_id, &type));
                        if (type == leveldb::FileType::kTableFile) {
                            stoc_file_manager_->DeleteSSTable(
                                    sstable_id + "-meta");
                        }
                    }
                    NOVA_LOG(DEBUG) << fmt::format(
                                "rdma-server[{}]: Delete SSTables. nsstables:{}",
                                thread_id_, nfiles);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::StoCRequestType::STOC_READ_BLOCKS) { // stoc第一次接收到ltc的读请求
                    uint32_t msg_size = 1;
                    uint32_t stoc_file_id = 0;
                    uint64_t offset = 0;
                    uint32_t size = 0;
                    uint64_t ltc_mr_offset = 0;
                    bool is_foreground_read = leveldb::DecodeBool(buf + msg_size);
                    msg_size += 1;
                    stoc_file_id = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    offset = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;
                    size = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    ltc_mr_offset = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;
                    std::string filename;
                    leveldb::DecodeStr(buf + msg_size, &filename);
                    NOVA_LOG(DEBUG) << fmt::format(
                                "rdma-server{}: Read blocks of StoC file {} offset:{} size:{} ltc_mr_offset:{} file:{}",
                                thread_id_, stoc_file_id, offset, size, ltc_mr_offset, filename);
                    // 这里似乎不必要 加入storage worker里面 可以直接将ct加入 先看看

                    if (!filename.empty()) {
                        stoc_file_id = stoc_file_manager_->OpenStoCFile(thread_id_, filename)->file_id();
                    }

                    char* rdma_buf = nullptr;
                    StorageTask task = {};

                    if(leveldb::IsPMfile(filename)){ // 如果是pm的文件的话 就不申请空间进行复制
                        task.ispmfile = true;
                    }else{
                        task.ispmfile = false;
                        uint32_t scid = mem_manager_->slabclassid(thread_id_, size);
                        rdma_buf = mem_manager_->ItemAlloc(thread_id_, scid);
                        NOVA_ASSERT(rdma_buf);
                    }

                    task.stoc_req_id = stoc_req_id;
                    task.rdma_server_thread_id = thread_id_;
                    task.remote_server_id = remote_server_id;
                    task.request_type = leveldb::StoCRequestType::STOC_READ_BLOCKS;
                    task.stoc_file_id = stoc_file_id;

                    task.ltc_mr_offset = ltc_mr_offset;
                    task.rdma_buf = rdma_buf;
                    task.stoc_block_handle.server_id = nova::NovaConfig::config->my_server_id;
                    task.stoc_block_handle.stoc_file_id = stoc_file_id;
                    task.stoc_block_handle.offset = offset;
                    task.stoc_block_handle.size = size;

                    if (is_foreground_read) {
                        AddFGStorageTask(task);
                    } else {
                        AddBGStorageTask(task);
                    }
                    processed = true;
                } else if (buf[0] ==
                           leveldb::StoCRequestType::STOC_WRITE_SSTABLE) {
                    uint32_t msg_size = 2;
                    std::string dbname;
                    std::string pmname;
                    int level;
                    int levels_in_pm;
                    uint64_t file_number;
                    uint32_t replica_id;
                    uint32_t size;
                    leveldb::FileInternalType internal_type;
                    if (buf[1] == 'm') {
                        internal_type = leveldb::FileInternalType::kFileMetadata;
                    } else if (buf[1] == 'p') {
                        internal_type = leveldb::FileInternalType::kFileParity;
                    } else {
                        NOVA_ASSERT(buf[1] == 'd');
                        internal_type = leveldb::FileInternalType::kFileData;
                    }

                    msg_size += leveldb::DecodeStr(buf + msg_size, &dbname);
                    msg_size += leveldb::DecodeStr(buf + msg_size, &pmname); // pmname level levels_in_pm
                    level = (int)leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    levels_in_pm = (int)leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    file_number = leveldb::DecodeFixed64(buf + msg_size);
                    msg_size += 8;
                    replica_id = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;
                    size = leveldb::DecodeFixed32(buf + msg_size);
                    msg_size += 4;

                    std::string filename;
                    if (file_number == 0) {
                        filename = leveldb::DescriptorFileName(dbname, 0,
                                                               replica_id);
                    } else {
                        filename = leveldb::TableFileName(dbname, pmname, 
                                                          file_number, level, levels_in_pm,
                                                          internal_type,
                                                          replica_id);
                    }

                    leveldb::StoCPersistentFile *stoc_file = stoc_file_manager_->OpenStoCFile(
                            thread_id_, filename);
                    uint64_t stoc_file_off = stoc_file->AllocateBuf(
                            filename, size, internal_type);
                    NOVA_ASSERT(stoc_file_off != UINT64_MAX)
                        << fmt::format("rdma-server{}: {} {}", thread_id_,
                                       filename,
                                       size);
                    NOVA_ASSERT(stoc_file->stoc_file_name_ == filename)
                        << fmt::format("rdma-server{}: {} {}", thread_id_,
                                       stoc_file->stoc_file_name_, filename);


                    ServerCompleteTask task = {};
                    task.request_type = leveldb::STOC_WRITE_SSTABLE;
                    task.remote_server_id = remote_server_id;
                    task.stoc_req_id = stoc_req_id;
                    task.stoc_file_id = stoc_file->file_id();
                    task.stoc_file_buf_offset = stoc_file_off;
                    private_cq_.push_back(task);

                    RequestContext context = {};
                    context.request_type = leveldb::StoCRequestType::STOC_WRITE_SSTABLE;
                    context.stoc_file_id = stoc_file->file_id();
                    context.stoc_file_buf_offset = stoc_file_off;
                    context.sstable_name = filename;
                    context.internal_type = internal_type;
                    context.size = size;
                    request_context_map_[req_id] = context;

                    NOVA_LOG(DEBUG) << fmt::format(
                                "rdma-server{}: Allocate buf for StoC file Write db:{} fn:{} size:{} file_id:{} file_off:{} fname:{}",
                                thread_id_, dbname, file_number, size, 
                                stoc_file->file_id(), stoc_file_off, filename);
                    processed = true;
                    // dbname 这里上面是initiateappendblock的部分
                } else if (buf[0] ==
                           leveldb::StoCRequestType::STOC_ALLOCATE_LOG_BUFFER) { // stoc端第一次收到ltc的log的申请
                    //leveldb::StoCLogType log_type = buf[1]; // 依次顺延
                    leveldb::StoCLogType log_type = leveldb::StoCLogType::STOC_LOG_DRAM; //= static_cast<StoCLogType>(buf[1])
                    if(buf[1] == leveldb::StoCLogType::STOC_LOG_DRAM){
                        log_type = leveldb::StoCLogType::STOC_LOG_DRAM;
                    }else if(buf[1] == leveldb::StoCLogType::STOC_LOG_PM){
                        log_type = leveldb::StoCLogType::STOC_LOG_PM;
                    }else if(buf[1] == leveldb::StoCLogType::STOC_LOG_DISK){
                        log_type = leveldb::StoCLogType::STOC_LOG_DISK;// tbd
                    }                    
                    uint32_t size = leveldb::DecodeFixed32(buf + 2);
                    std::string log_file(buf + 6, size);
                    uint32_t db_index;
                    nova::ParseDBIndexFromLogFileName(log_file, &db_index);
                    char *rdma_buf = nullptr; // 几种情况的都包括了
                    if(log_type == leveldb::StoCLogType::STOC_LOG_DRAM){
                        uint32_t slabclassid = mem_manager_->slabclassid(thread_id_,
                                                                        nova::NovaConfig::config->max_stoc_file_size);
                        rdma_buf = mem_manager_->ItemAlloc(thread_id_, slabclassid);  
                        NOVA_ASSERT(rdma_buf) << "Running out of memory";                      
                    }else if(log_type == leveldb::StoCLogType::STOC_LOG_PM){
                        rdma_buf = pm_manager_->ItemAlloc(db_index, log_file);
                        NOVA_ASSERT(rdma_buf) << "Running out of memory";
                    }else if(log_type == leveldb::StoCLogType::STOC_LOG_DISK){
                        //这个之后再说
                        //tbd
                    }
                    log_manager_->AddLocalBuf(log_file, rdma_buf, log_type);
                    ServerCompleteTask task = {};
                    task.request_type = leveldb::STOC_ALLOCATE_LOG_BUFFER;
                    task.rdma_buf = rdma_buf;
                    task.log_type = log_type;
                    task.remote_server_id = remote_server_id;
                    task.stoc_req_id = stoc_req_id;
                    private_cq_.push_back(task);

                    NOVA_LOG(DEBUG) << fmt::format(
                                "rdma-server{}]: Allocate log buffer for file {}.",
                                thread_id_, log_file);
                    processed = true;
                } else if(buf[0] == 
                           leveldb::StoCRequestType::STOC_WRITE_LOG_BUFFER_DISK){ // disk的申请log相关的 第一次收到ltc的请求
                    leveldb::StoCLogType log_type = leveldb::StoCLogType::STOC_LOG_DISK; // disk的
                    uint32_t log_record_size = leveldb::DecodeFixed32(buf + 1); // 要分配的空间大小
                    uint32_t name_size = leveldb::DecodeFixed32(buf + 5); // log name相关
                    std::string log_file(buf + 9, name_size);
                    uint32_t db_index;
                    nova::ParseDBIndexFromLogFileName(log_file, &db_index);
                    leveldb::StoCPersistentFile *stoc_file = stoc_file_manager_->OpenStoCFile(
                        thread_id_, log_file); // 开一个wal 文件
                    uint64_t stoc_file_off = stoc_file->AllocateBuf(
                            log_file, log_record_size, leveldb::FileInternalType::kFileData);                    

                    NOVA_ASSERT(stoc_file_off != UINT64_MAX)
                        << fmt::format("rdma-server{}: {} {}", thread_id_,
                                       log_file,
                                       log_record_size);
                    NOVA_ASSERT(stoc_file->stoc_file_name_ == log_file)
                        << fmt::format("rdma-server{}: {} {}", thread_id_,
                                       stoc_file->stoc_file_name_, log_file);
                    // 里面需要加一个判断 如果是第一次添加的话那就加上 不是的话那就不加
                    // true 代表第一次
                    log_manager_->AddLocalBuf(log_file, stoc_file->backing_mem_, log_type); // 里面已经加上了 只有第一次申请会添加
                    ServerCompleteTask task = {};
                    task.request_type = leveldb::StoCRequestType::STOC_WRITE_LOG_BUFFER_DISK;
                    task.rdma_buf = (char*) stoc_file_off; // 每次申请的buf的开头
                    task.log_type = log_type; // log的需求
                    task.remote_server_id = remote_server_id;
                    task.stoc_req_id = stoc_req_id;

                    // 以下是sstable的要求
                    task.stoc_file_id = stoc_file->file_id();
                    task.stoc_file_buf_offset = stoc_file_off;
                    private_cq_.push_back(task);

                    RequestContext context = {}; // writesstable的东西
                    context.request_type = leveldb::StoCRequestType::STOC_WRITE_LOG_BUFFER_DISK;
                    context.stoc_file_id = stoc_file->file_id();
                    context.stoc_file_buf_offset = stoc_file_off; // 每次申请的开头
                    context.sstable_name = log_file;
                    context.internal_type = leveldb::FileInternalType::kFileData;
                    context.size = log_record_size; // 申请的空间大小
                    request_context_map_[req_id] = context;

                    NOVA_LOG(DEBUG) << fmt::format(
                                "rdma-server{}]: Allocate log buffer for file {}.",
                                thread_id_, log_file);
                    processed = true;                    
                }else if (buf[0] ==
                           leveldb::StoCRequestType::RDMA_WRITE_REQUEST) {
                    uint32_t size = leveldb::DecodeFixed32(buf + 1);
                    uint32_t slabclassid = mem_manager_->slabclassid(thread_id_, size);
                    char *rdmabuf = mem_manager_->ItemAlloc(thread_id_, slabclassid);
                    NOVA_ASSERT(rdmabuf) << "Running out of memory";

                    ServerCompleteTask task = {};
                    task.request_type = leveldb::RDMA_WRITE_REQUEST;
                    task.rdma_buf = rdmabuf;
                    task.size = size;
                    task.remote_server_id = remote_server_id;
                    task.stoc_req_id = stoc_req_id;
                    private_cq_.push_back(task);

                    RequestContext context = {};
                    context.request_type = leveldb::StoCRequestType::RDMA_WRITE_REMOTE_BUF_ALLOCATED;
                    context.buf = rdmabuf;
                    context.size = size;
                    request_context_map_[req_id] = context;
                    NOVA_LOG(DEBUG) << fmt::format(
                                "rdma-server{}]: Allocate buffer for RDMA WRITE.",
                                thread_id_);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::StoCRequestType::STOC_QUERY_LOG_FILES) {
                    uint32_t server_id = leveldb::DecodeFixed32(buf + 1);
                    uint32_t dbid = leveldb::DecodeFixed32(buf + 5);
                    std::unordered_map<std::string, uint64_t> logfile_offset;
                    log_manager_->QueryLogFiles(dbid, &logfile_offset);
                    // TODO.
                    uint32_t msg_size = 0;
                    char *send_buf = rdma_broker_->GetSendBuf(remote_server_id);
                    send_buf[msg_size] = leveldb::StoCRequestType::STOC_QUERY_LOG_FILES_RESPONSE;
                    msg_size += 1;
                    msg_size += leveldb::EncodeFixed32(send_buf + msg_size,
                                                       logfile_offset.size());
                    for (const auto &it : logfile_offset) {
                        msg_size += leveldb::EncodeStr(send_buf + msg_size,
                                                       it.first);
                        msg_size += leveldb::EncodeFixed64(send_buf + msg_size,
                                                           it.second);
                    }
                    rdma_broker_->PostSend(send_buf, msg_size, remote_server_id,
                                           stoc_req_id);
                } else if (buf[0] ==
                           leveldb::StoCRequestType::STOC_DELETE_LOG_FILE) {
                    int size = 1;
                    uint32_t nlogs = leveldb::DecodeFixed32(buf + 1);
                    size += 4;
                    std::vector<std::string> logfiles;
                    for (int i = 0; i < nlogs; i++) {
                        std::string log;
                        size += leveldb::DecodeStr(buf + size, &log);
                        logfiles.push_back(log);
                    }
                    log_manager_->DeleteLogBuf(logfiles, false);
                    NOVA_LOG(DEBUG) << fmt::format(
                                "rdma-server{}]: Delete log buffer for file {}.",
                                thread_id_, logfiles.size());
                    processed = true;
                } else if (buf[0] ==
                           leveldb::StoCRequestType::STOC_FILENAME_STOCFILEID) {
                    uint32 read_size = 1;
                    uint32_t nfiles = leveldb::DecodeFixed32(buf + read_size);
                    read_size += 4;
                    std::unordered_map<std::string, uint32_t> fn_stocfile;

                    for (int i = 0; i < nfiles; i++) {
                        std::string fn;
                        read_size += leveldb::DecodeStr(buf + read_size, &fn);
                        uint32_t stoc_file_id = leveldb::DecodeFixed32(
                                buf + read_size);
                        read_size += 4;
                        fn_stocfile[fn] = stoc_file_id;
                    }
                    stoc_file_manager_->OpenStoCFiles(fn_stocfile);
                    ServerCompleteTask task = {};
                    task.remote_server_id = remote_server_id;
                    task.request_type = leveldb::STOC_FILENAME_STOCFILEID;
                    task.stoc_req_id = stoc_req_id;
                    private_cq_.push_back(task);
                    NOVA_LOG(DEBUG) << fmt::format(
                                "rdma-server{}]: Filename stoc file mapping {}.",
                                thread_id_, fn_stocfile.size());
                    processed = true;
                } else if (buf[0] ==
                           leveldb::StoCRequestType::STOC_COMPACTION) {
                    auto req = new leveldb::CompactionRequest;
                    req->DecodeRequest(buf + 1,
                                       nova::NovaConfig::config->max_msg_size);
                    StorageTask task = {};
                    task.stoc_req_id = stoc_req_id;
                    task.rdma_server_thread_id = thread_id_;
                    task.remote_server_id = remote_server_id;
                    task.request_type = leveldb::StoCRequestType::STOC_COMPACTION;
                    task.compaction_request = req;
                    AddCompactionStorageTask(task);
                    processed = true;
                } else if (buf[0] ==
                           leveldb::StoCRequestType::STOC_IS_READY_FOR_REQUESTS) {
                    processed = true;
                    ServerCompleteTask ct = {};
                    ct.remote_server_id = remote_server_id;
                    ct.request_type = leveldb::StoCRequestType::STOC_IS_READY_FOR_REQUESTS;
                    ct.stoc_req_id = stoc_req_id;
                    private_cq_.push_back(ct);
                } else if (buf[0] ==
                           leveldb::StoCRequestType::STOC_REPLICATE_SSTABLES) {
                    StorageTask task = {};
                    task.stoc_req_id = stoc_req_id;
                    task.rdma_server_thread_id = thread_id_;
                    task.remote_server_id = remote_server_id;
                    task.request_type = leveldb::StoCRequestType::STOC_REPLICATE_SSTABLES;

                    uint32_t num_pairs = 0;
                    leveldb::Slice input(buf + 1,
                                         nova::NovaConfig::config->max_msg_size);
                    uint32_t level;
                    uint32_t levels_in_pm;
                    NOVA_ASSERT(leveldb::DecodeStr(&input, &task.dbname));
                    NOVA_ASSERT(leveldb::DecodeStr(&input, &task.pmname));
                    NOVA_ASSERT(leveldb::DecodeFixed32(&input, &level));
                    task.level = (int) level;
                    NOVA_ASSERT(leveldb::DecodeFixed32(&input, &levels_in_pm));
                    task.levels_in_pm = (int) levels_in_pm;
                    NOVA_ASSERT(leveldb::DecodeFixed32(&input, &num_pairs));
                    for (int i = 0; i < num_pairs; i++) {
                        leveldb::ReplicationPair pair;
                        NOVA_ASSERT(pair.Decode(&input));
                        NOVA_LOG(DEBUG) << pair.DebugString();
                        task.replication_pairs.push_back(pair);
                    }
                    AddCompactionStorageTask(task);
                    processed = true;
                }
                break;
        }
        return processed;
    }
}
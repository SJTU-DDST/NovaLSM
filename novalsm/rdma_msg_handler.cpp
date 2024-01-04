
//
// Created by Haoyu Huang on 12/25/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <leveldb/write_batch.h>
#include "rdma_msg_handler.h"
#include "common/nova_config.h"
#include "common/nova_common.h"

namespace nova {

    static void DeleteEntry(const leveldb::Slice &key, void *value) {
        CacheValue *tf = reinterpret_cast<CacheValue *>(value);
        delete tf;
    }

//加一个工作任务
    void RDMAMsgHandler::AddTask(
            const leveldb::RDMARequestTask &task) {
        mutex_.Lock();
        public_queue_.push_back(task);
        mutex_.Unlock();
    }

    int RDMAMsgHandler::size() {
        mutex_.Lock();
        int size = public_queue_.size();
        mutex_.Unlock();
        return size;
    }

// 处理自己这个线程对应的任务
    int RDMAMsgHandler::ProcessRequestQueue() {
        // 就是public queue -> private queue -> pending_reqs 处理这个里面的
        { // 处理之前已经发送过的 留下了 ctx的 请求
            auto it = pending_reqs_.begin();
            while (it != pending_reqs_.end()) {
                if (stoc_client_->IsDone(it->req_id, it->response, nullptr)) { // RDMAClient!!!!!!!!!!!!!!!!!!!!!!!!!!!注意！！！
                    if (it->sem) {
                        NOVA_LOG(rdmaio::DEBUG)
                            << fmt::format("Wake up request: {}", it->req_id);
                        NOVA_ASSERT(sem_post(it->sem) == 0); // 这里的sem一般都是block client的sem
                    }
                    it = pending_reqs_.erase(it);
                } else {
                    it++;
                }
            }
        }

        // 取出公共队列中的任务
        mutex_.Lock();
        while (!public_queue_.empty()) {
            private_queue_.push_back(public_queue_.front());
            public_queue_.pop_front();
        }
        mutex_.Unlock();

        stat_tasks_ += private_queue_.size();
        uint32_t size = private_queue_.size();
        auto it = private_queue_.begin();
        std::vector<int> serverids;

        // 处理当前拿到的任务
        while (it != private_queue_.end()) {
            const auto &task = *it;
            serverids.clear();
            if (task.type == leveldb::RDMA_CLIENT_REQ_CLOSE_LOG ||
                task.type == leveldb::RDMA_CLIENT_REQ_LOG_RECORD) {
                NOVA_ASSERT(task.server_id == -1);
                // A log record request.
                nova::LTCFragment *frag = nova::NovaConfig::config->cfgs[0]->fragments[task.dbid];
                for (int i = 0; i < frag->log_replica_stoc_ids.size(); i++) {
                    uint32_t stoc_server_id = nova::NovaConfig::config->cfgs[0]->stoc_servers[frag->log_replica_stoc_ids[i]];
                    serverids.push_back(stoc_server_id);
                }
                if (!admission_control_->CanIssueRequest(serverids)) { // 这里无锁，有问题。。
                    it++;
                    continue;
                }
            } else {
                NOVA_ASSERT(task.server_id >= 0);
                if (!admission_control_->CanIssueRequest(task.server_id)) {
                    it++;
                    continue;
                }
            }
            RequestCtx ctx = {};
            ctx.sem = task.sem; // 一般这里的sem是client的sem client需要串行工作 通过sem结束等待
            ctx.response = task.response;
            bool failed = false;
            switch (task.type) {
                case leveldb::RDMA_CLIENT_ALLOCATE_LOG_BUFFER_SUCC:
                    rdma_log_writer_->AckAllocLogBuf(task.log_file_name,
                                                     task.server_id,
                                                     task.offset,
                                                     task.size,
                                                     task.rdma_log_record_backing_mem,
                                                     task.write_size,
                                                     task.thread_id,
                                                     task.replicate_log_record_states);
                    ctx.req_id = task.thread_id;
                    break;
                case leveldb::RDMA_CLIENT_RDMA_WRITE_REQUEST:
                    ctx.req_id = stoc_client_->InitiateRDMAWRITE(task.server_id, task.write_buf, task.size);
                    break;
                case leveldb::RDMA_CLIENT_RDMA_WRITE_REMOTE_BUF_ALLOCATED: { // 目前是dram到dram的 这里是第一次所以加了imm? 这里也是log相关的
                    char *sendbuf = rdma_broker_->GetSendBuf(task.server_id);
                    sendbuf[0] = leveldb::StoCRequestType::RDMA_WRITE_REMOTE_BUF_ALLOCATED;
                    leveldb::EncodeFixed32(sendbuf + 1,
                                           (uint64_t) task.thread_id);
                    rdma_broker_->PostWrite(
                            task.write_buf,
                            task.write_size, task.server_id,
                            task.offset, false, task.thread_id, 0, 0);
                    ctx.req_id = task.thread_id;
                }
                    break;
                case leveldb::RDMA_CLIENT_WRITE_SSTABLE_RESPONSE: // 1个rtt之后 ltc收到要写的位置 然后开始写 这个是initiateappendblock相关的
                    rdma_broker_->PostWrite(
                            task.write_buf,
                            task.size, task.server_id,
                            task.offset, false, task.thread_id, task.local_which, task.remote_which);
                    ctx.req_id = task.thread_id;
                    break;
                case leveldb::RDMA_CLIENT_COMPACTION:
                    ctx.req_id = stoc_client_->InitiateCompaction(
                            task.server_id,
                            task.compaction_request);
                    break;
                case leveldb::RDMA_CLIENT_FILENAME_STOC_FILE_MAPPING:
                    ctx.req_id = stoc_client_->InitiateInstallFileNameStoCFileMapping(
                            task.server_id, task.fn_stoc_file_id);
                    break;
                case leveldb::RDMA_CLIENT_READ_LOG_FILE:
                    ctx.req_id = stoc_client_->InitiateReadInMemoryLogFile(
                            task.rdma_log_record_backing_mem, task.server_id, task.remote_stoc_offset, task.size);
                    break;
                case leveldb::RDMA_CLIENT_REQ_READ: // block到rdma的read
                    ctx.req_id = stoc_client_->InitiateReadDataBlock( // 这里的client 是 rdmaclient 不是blockclient！！！
                            task.stoc_block_handle,
                            task.offset,
                            task.size,
                            task.result, task.write_size, task.filename,
                            task.is_foreground_reads);
                    break;
                case leveldb::RDMA_CLIENT_REQ_QUERY_LOG_FILES:
                    ctx.req_id = stoc_client_->InitiateQueryLogFile(
                            task.server_id,
                            nova::NovaConfig::config->my_server_id,
                            task.dbid,
                            task.logfile_offset);
                    break;
                case leveldb::RDMA_CLIENT_REQ_WRITE_DATA_BLOCKS:
                    ctx.req_id = stoc_client_->InitiateAppendBlock(
                            task.server_id, task.thread_id,
                            nullptr, task.write_buf, task.dbname, task.pmname, task.level, task.levels_in_pm, // done
                            task.file_number, task.replica_id, task.write_size,
                            task.internal_type);
// dbname 这里上面是 initiateappendblock等
                    break;
                case leveldb::RDMA_CLIENT_REQ_DELETE_TABLES:
                    ctx.req_id = stoc_client_->InitiateDeleteTables(
                            task.server_id, task.stoc_file_ids);
                    break;
                case leveldb::RDMA_CLIENT_READ_STOC_STATS:
                    ctx.req_id = stoc_client_->InitiateReadStoCStats(
                            task.server_id);
                    break;
                case leveldb::RDMA_CLIENT_REQ_CLOSE_LOG:
                    ctx.req_id = stoc_client_->InitiateCloseLogFiles(
                            task.log_files, task.dbid);
                    break;
                case leveldb::RDMA_CLIENT_IS_READY_FOR_REQUESTS:
                    ctx.req_id = stoc_client_->InitiateIsReadyForProcessingRequests(
                            task.server_id);
                    break;
                case leveldb::RDMA_CLIENT_REQ_LOG_RECORD:
                    ctx.req_id = stoc_client_->InitiateReplicateLogRecords(
                            task.log_file_name,
                            task.thread_id,
                            task.dbid,
                            task.memtable_id,
                            task.write_buf,
                            task.log_records,
                            task.replicate_log_record_states);
                    if (ctx.req_id == 0) {
                        // Failed and must retry.
                        failed = true;
                    }
                    break;
                case leveldb::RDMA_CLIENT_RECONSTRUCT_MISSING_REPLICA:
                    ctx.req_id = stoc_client_->InitiateReplicateSSTables(
                            task.server_id, task.dbname, task.pmname, task.level, task.levels_in_pm, task.missing_replicas);
                    break;
            }
            if (failed) {
                NOVA_ASSERT(serverids.size() > 0);
                for (auto sid : serverids) {
                    admission_control_->RemoveRequests(sid, 1);
                }
                it++;
            } else {
                pending_reqs_.push_back(ctx); // 把这个请求的上下文记录到当前正在处理的请求中
                it = private_queue_.erase(it);
            }
        }
        return size;
    }

    bool RDMAMsgHandler::IsInitialized() {
        mutex_.Lock();
        bool t = is_running_;
        mutex_.Unlock();
        return t;
    }

//rdma前后台进程的函数
    void RDMAMsgHandler::Start() {
        NOVA_LOG(DEBUG) << "Async worker started";
//初始化连接和qp的各种数据
        if (NovaConfig::config->enable_rdma) {
            rdma_broker_->Init(rdma_ctrl_); // 初始化与其他机器的rdma的qp链接等
        }
//添加映射
        nova::NovaConfig::config->add_tid_mapping();
        sleep(10);

        mutex_.Lock();
        is_running_ = true;
        mutex_.Unlock();

        bool should_sleep = true;
        uint32_t timeout = RDMA_POLL_MIN_TIMEOUT_US;
        while (is_running_) {
            while (should_pause) { // 重新初始化 或者关闭stoc文件的时候会停下?
                paused = true;
//等待唤醒
                sem_wait(&sem_);
                paused = false;
            }
//应该sleep且前台rdma线程多余1个
            if (should_sleep &&
                nova::NovaConfig::config->num_fg_rdma_workers > 1) {
                usleep(timeout);
            }
            int n = 0;

            auto endpoints = rdma_broker_->end_points();
            for (const auto &endpoint : endpoints) {
                uint32_t new_requests = 0;
                uint32_t completed_requests = rdma_broker_->PollSQ( // 获取已经完成的send request的信息
                        endpoint.server_id, &new_requests);
                new_requests = 0;
                rdma_broker_->PollRQ(endpoint.server_id, &new_requests); // 获取已经完成的recv request的信息 这两个函数不可能产生new request
                n += completed_requests;
                n += new_requests;
            }
            n += rdma_server_->ProcessCompletionQueue(); // ??? 这个里面的任务全是 storage worker发下来的 总之处理这个里面的任务 一般storage worker发回来的都是 之前完成的工作的回复 rdma server中 之前完成的工作的complete task
            n += ProcessRequestQueue(); // ??? 处理上层应用交给rdma handler的task 这个是工作请求 (也许会包含poll出来的请求?)
            if (n == 0) {
                should_sleep = true; //不忙就睡
                timeout *= 2;
                if (timeout > RDMA_POLL_MAX_TIMEOUT_US) {
                    timeout = RDMA_POLL_MAX_TIMEOUT_US;
                }
            } else {
                should_sleep = false;
                timeout = RDMA_POLL_MIN_TIMEOUT_US;
            }
        }
    }

//用于从cq中读取已经完成的request并且进行处理 处理cqe
    bool
    RDMAMsgHandler::ProcessRDMAWC(ibv_wc_opcode opcode,
                                  uint64_t wr_id,
                                  int remote_server_id,
                                  char *buf, uint32_t imm_data,
                                  bool *generate_a_new_request) {
//如果是发送类型的rdma请求完成了 这里应该只会来自于 cq_ opcode是主动类型的
        if (opcode == IBV_WC_SEND ||
            opcode == IBV_WC_RDMA_WRITE ||
            opcode == IBV_WC_RDMA_READ) {
            // a send request completes.
//先修改一下计数
            admission_control_->RemoveRequests(remote_server_id, 1);
        }
        if (opcode == IBV_WC_SEND) { // 第一次发了read 请求之后 ltc方面不需要做什么
            return true;
        }
        //如果是上面的类型 说明是本服务器发给别人的 下面的类型应该是IBV_WC_ERCV 代表别的服务器发送给我的请求
//如果是其它类型的cqe的话，可能需要stoc客户端和rdmaserver都进行处理??? 下面应该是类似于receiv
// 看起来是两种智能 stoc client是发起请求端处理 rdmaserver是收到请求server端处理
        bool processed_by_client = stoc_client_->OnRecv(opcode, wr_id, remote_server_id, buf, imm_data, nullptr);
        bool processed_by_server = rdma_server_->ProcessRDMAWC(opcode, wr_id, remote_server_id, buf, imm_data, nullptr);
        if (processed_by_client && processed_by_server) { // 应该只被一个处理 什么被什么处理 ??
            NOVA_ASSERT(false)
                << fmt::format("Processed by both client and server");
        }
    }
}
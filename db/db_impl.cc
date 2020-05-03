// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>

#include <algorithm>
#include <atomic>
#include <set>
#include <string>
#include <vector>
#include <list>
#include <cc/nova_cc.h>
#include <fmt/core.h>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "leveldb/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {
    namespace {
        uint64_t time_diff(timeval t1, timeval t2) {
            return (t2.tv_sec - t1.tv_sec) * 1000000 +
                   (t2.tv_usec - t1.tv_usec);
        }
    }

    uint64_t TableLocator::Lookup(const leveldb::Slice &key, uint64_t hash) {
        RDMA_ASSERT(hash >= 0 && hash <= MAX_BUCKETS);
        TableLocation &loc = table_locator_[hash % MAX_BUCKETS];
//        uint64_t memtable_id = 0;
//        loc.mutex.lock();
//        memtable_id = loc.memtable_id;
//        loc.mutex.unlock();
//        return memtable_id;
        return loc.memtable_id.load();
    }

    void TableLocator::Insert(const leveldb::Slice &key, uint64_t hash,
                              uint32_t memtableid) {
        RDMA_ASSERT(hash >= 0 && hash <= MAX_BUCKETS) << hash;
        TableLocation &loc = table_locator_[hash % MAX_BUCKETS];
//        loc.mutex.lock();
//        loc.memtable_id = memtableid;
//        loc.mutex.unlock();
        loc.memtable_id.store(memtableid);
    }


    const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
    struct DBImpl::Writer {
        explicit Writer(port::Mutex *mu)
                : batch(nullptr), sync(false), done(false), cv(mu) {}

        Status status;
        WriteBatch *batch;
        bool sync;
        bool done;
        port::CondVar cv;
    };

    struct DBImpl::CompactionState {
        // Files produced by compaction
        struct Output {
            uint64_t number;
            uint64_t file_size;
            uint64_t converted_file_size;
            InternalKey smallest, largest;
            RTableHandle meta_block_handle;
            std::vector<RTableHandle> data_block_group_handles;
        };

        Output *current_output() { return &outputs[outputs.size() - 1]; }

        explicit CompactionState(Compaction *c)
                : compaction(c),
                  smallest_snapshot(0),
                  outfile(nullptr),
                  builder(nullptr),
                  total_bytes(0) {}

        Compaction *const compaction;

        // Sequence numbers < smallest_snapshot are not significant since we
        // will never have to service a snapshot below smallest_snapshot.
        // Therefore if we have seen a sequence number S <= smallest_snapshot,
        // we can drop all entries for the same key with sequence numbers < S.
        SequenceNumber smallest_snapshot;

        std::vector<Output> outputs;

        // State kept for output being generated
        MemWritableFile *outfile;
        TableBuilder *builder;
        std::vector<MemWritableFile *> output_files;

        uint64_t total_bytes;
    };

// Fix user-supplied options to be reasonable
    template<class T, class V>
    static void ClipToRange(T *ptr, V minvalue, V maxvalue) {
        if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
        if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
    }

    Options SanitizeOptions(const std::string &dbname,
                            const InternalKeyComparator *icmp,
                            const InternalFilterPolicy *ipolicy,
                            const Options &src) {
        Options result = src;
        result.comparator = icmp;
        result.filter_policy = (src.filter_policy != nullptr) ? ipolicy
                                                              : nullptr;
        ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
        ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
        ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
        ClipToRange(&result.block_size, 1 << 10, 4 << 20);
        if (result.info_log == nullptr) {
            // Open a log file in the same directory as the db
            src.env->CreateDir(dbname);  // In case it does not exist
            src.env->RenameFile(InfoLogFileName(dbname),
                                OldInfoLogFileName(dbname));
            Status s = src.env->NewLogger(InfoLogFileName(dbname),
                                          &result.info_log);
            if (!s.ok()) {
                // No place suitable for logging
                result.info_log = nullptr;
            }
        }
        return result;
    }

    static int TableCacheSize(const Options &sanitized_options) {
        // Reserve ten files or so for other uses and give the rest to TableCache.
        return sanitized_options.max_open_files - kNumNonTableCacheFiles;
    }

    DBImpl::DBImpl(const Options &raw_options, const std::string &dbname)
            : env_(raw_options.env),
              internal_comparator_(raw_options.comparator),
              internal_filter_policy_(raw_options.filter_policy),
              options_(SanitizeOptions(dbname, &internal_comparator_,
                                       &internal_filter_policy_, raw_options)),
              owns_info_log_(options_.info_log != raw_options.info_log),
              owns_cache_(options_.block_cache != raw_options.block_cache),
              dbname_(dbname),
              db_profiler_(new DBProfiler(raw_options.enable_tracing,
                                          raw_options.trace_file_path)),
              table_cache_(new TableCache(dbname_, options_,
                                          TableCacheSize(options_),
                                          db_profiler_)),
              db_lock_(nullptr),
              shutting_down_(false),
              seed_(0),
              manual_compaction_(nullptr),
              versions_(new VersionSet(dbname_, &options_, table_cache_,
                                       &internal_comparator_)),
              compaction_threads_(raw_options.bg_threads),
              reorg_thread_(raw_options.reorg_thread),
              memtable_available_signal_(&range_lock_),
              user_comparator_(raw_options.comparator) {
        memtable_id_seq_.store(100);
        if (options_.enable_table_locator) {
            table_locator_ = new TableLocator;
            for (int i = 0; i < MAX_BUCKETS; i++) {
                table_locator_->Insert(Slice(), i, 0);
            }
        }
        uint32_t sid;
        nova::ParseDBIndexFromDBName(dbname_, &sid, &dbid_);
    }

    DBImpl::~DBImpl() {
        // Wait for background work to finish.
        if (db_lock_ != nullptr) {
            env_->UnlockFile(db_lock_);
        }

        delete versions_;
        delete table_cache_;

        if (owns_info_log_) {
            delete options_.info_log;
        }
        if (owns_cache_) {
            delete options_.block_cache;
        }
    }

    Status DBImpl::NewDB() {
        VersionEdit new_db;
        new_db.SetComparatorName(user_comparator()->Name());
        new_db.SetNextFile(2);
        new_db.SetLastSequence(0);

        const std::string manifest = DescriptorFileName(dbname_, 1);
        WritableFile *file;
        Status s = env_->NewWritableFile(manifest, {
                .level = -1
        }, &file);
        if (!s.ok()) {
            return s;
        }
        {
//            log::Writer log(file);
//            std::string record;
//            new_db.EncodeTo(&record);
//            s = log.AddRecord(record);
//            if (s.ok()) {
//                s = file->Close();
//            }
        }
        delete file;
        if (s.ok()) {
            // Make "CURRENT" file that points to the new manifest file.
            s = SetCurrentFile(env_, dbname_, 1);
        } else {
            env_->DeleteFile(manifest);
        }
        return s;
    }

    void DBImpl::EvictFileFromCache(uint64_t file_number) {
        table_cache_->Evict(file_number);
    }

    void DBImpl::MaybeIgnoreError(Status *s) const {
        if (s->ok() || options_.paranoid_checks) {
            // No change needed
        } else {
            Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
            *s = Status::OK();
        }
    }

    void DBImpl::DeleteObsoleteVersions(leveldb::EnvBGThread *bg_thread) {
        mutex_.AssertHeld();
        versions_->DeleteObsoleteVersions();
    }

    void DBImpl::DeleteObsoleteFiles(EnvBGThread *bg_thread) {
        mutex_.AssertHeld();

        if (!bg_error_.ok()) {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }

        // Make a set of all of the live files
        std::set<uint64_t> live = pending_outputs_;
        versions_->AddLiveFiles(&live);

        std::vector<std::string> filenames;
        env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
        uint64_t number;
        FileType type;
        std::vector<std::string> files_to_delete;

        for (std::string &filename : filenames) {
            if (ParseFileName(filename, &number, &type)) {
                bool keep = true;
                switch (type) {
                    case kTableFile:
                        keep = (live.find(number) != live.end());
                        break;
                    case kTempFile:
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs_, which is inserted into "live"
                        keep = (live.find(number) != live.end());
                        break;
                    case kCurrentFile:
                    case kDBLockFile:
                    case kInfoLogFile:
                        keep = true;
                        break;
                }

                if (!keep) {
                    files_to_delete.push_back(std::move(filename));
                    if (type == kTableFile) {
//                        table_cache_->Evict(number);
                    }
                    Log(options_.info_log, "Delete type=%d #%lld\n",
                        static_cast<int>(type),
                        static_cast<unsigned long long>(number));
                }
            }
        }

        std::map<uint32_t, std::vector<SSTableRTablePair>> server_pairs;
        auto it = compacted_tables_.begin();
        while (it != compacted_tables_.end()) {
            uint64_t fn = it->first;
            FileMetaData &meta = it->second;

            if (live.find(fn) != live.end()) {
                // Do not remove if it is still alive.
                it++;
                continue;
            }
            table_cache_->Evict(meta.number);
            auto handles = meta.data_block_group_handles;
            for (int i = 0; i < handles.size(); i++) {
                SSTableRTablePair pair = {};
                pair.sstable_id = TableFileName(dbname_, meta.number);
                pair.rtable_id = handles[i].rtable_id;
                server_pairs[handles[i].server_id].push_back(pair);
            }

            {
                auto &it = server_pairs[meta.meta_block_handle.server_id];
                bool found = false;
                for (auto &rtable : it) {
                    if (rtable.rtable_id == meta.meta_block_handle.rtable_id ||
                        meta.meta_block_handle.rtable_id == 0) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    SSTableRTablePair pair = {};
                    pair.sstable_id = TableFileName(dbname_, meta.number);
                    pair.rtable_id = meta.meta_block_handle.rtable_id;
                    it.push_back(pair);
                }
            }
            it = compacted_tables_.erase(it);
        }
        // While deleting all files unblock other threads. All files being deleted
        // have unique names which will not collide with newly created files and
        // are therefore safe to delete while allowing other threads to proceed.
        mutex_.Unlock();
        for (const std::string &filename : files_to_delete) {
            env_->DeleteFile(dbname_ + "/" + filename);
        }
        for (auto &it : server_pairs) {
            bg_thread->dc_client()->InitiateDeleteTables(it.first,
                                                         it.second);
            for (auto &tble : it.second) {
                RDMA_LOG(rdmaio::INFO)
                    << fmt::format("Delete {} {}", tble.rtable_id,
                                   tble.sstable_id);
            }
        }
        mutex_.Lock();
    }

    DBImpl::NovaCCRecoveryThread::NovaCCRecoveryThread(
            uint32_t client_id,
            std::vector<leveldb::MemTable *> memtables,
            MemManager *mem_manager)
            : client_id_(client_id), memtables_(memtables),
              mem_manager_(mem_manager) {
        sem_init(&sem_, 0, 0);
    }

    void DBImpl::NovaCCRecoveryThread::Recover() {
        sem_wait(&sem_);
        if (log_replicas_.empty()) {
            return;
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("t{}: started memtables:{}", client_id_,
                           log_replicas_.size());
        timeval start{};
        gettimeofday(&start, nullptr);

        timeval new_memtable_end{};
        gettimeofday(&new_memtable_end, nullptr);
        new_memtable_time = time_diff(start, new_memtable_end);

        for (int i = 0; i < log_replicas_.size(); i++) {
            char *buf = log_replicas_[i];
            leveldb::MemTable *memtable = memtables_[i];

            leveldb::LevelDBLogRecord record;
            uint32_t record_size = nova::DecodeLogRecord(buf, &record);
            while (record_size != 0) {
                memtable->Add(record.sequence_number,
                              leveldb::ValueType::kTypeValue, record.key,
                              record.value);
                recovered_log_records += 1;
                buf += record_size;
                record_size = nova::DecodeLogRecord(buf, &record);
                max_sequence_number = std::max(max_sequence_number,
                                               record.sequence_number);
            }
        }

        RDMA_ASSERT(memtables_.size() == log_replicas_.size());

        timeval end{};
        gettimeofday(&end, nullptr);
        recovery_time = time_diff(start, end);
    }

    Status DBImpl::Recover() {
        timeval start = {};
        gettimeofday(&start, nullptr);

        uint32_t stoc_id = options_.manifest_stoc_id;
        std::string manifest = DescriptorFileName(dbname_, 0);
        auto client = reinterpret_cast<NovaBlockCCClient *> (options_.dc_client);
        uint32_t scid = options_.mem_manager->slabclassid(0,
                                                          options_.max_file_size);
        char *buf = options_.mem_manager->ItemAlloc(0, scid);
        RTableHandle handle = {};
        handle.server_id = stoc_id;
        handle.rtable_id = 0;
        handle.offset = 0;
        handle.size = options_.max_file_size;

        RDMA_LOG(rdmaio::INFO) << fmt::format(
                    "Recover the latest verion from manifest file {} at StoC-{}",
                    manifest, stoc_id);

        client->InitiateRTableReadDataBlock(handle, 0, options_.max_file_size,
                                            buf,
                                            manifest);
        client->Wait();

        std::map<std::string, uint64_t> logfile_buf;
        nova::CCFragment *frag = nova::NovaConfig::config->db_fragment[dbid_];
        if (!frag->log_replica_stoc_ids.empty()) {
            uint32_t stoc_server_id = nova::NovaConfig::config->dc_servers[frag->log_replica_stoc_ids[0]].server_id;
            RDMA_LOG(rdmaio::INFO) << fmt::format(
                        "Recover the latest memtables from log files at StoC-{}",
                        stoc_server_id);
            client->InitiateQueryLogFile(stoc_server_id,
                                         nova::NovaConfig::config->my_server_id,
                                         dbid_, &logfile_buf);
            client->Wait();
        }

        for (auto &it : logfile_buf) {
            RDMA_LOG(rdmaio::INFO)
                << fmt::format("log file {}:{}", it.first, it.second);
        }

        // Recover log records.
        std::vector<VersionSubRange> subrange_edits;
        RDMA_ASSERT(versions_->Recover(Slice(buf, options_.max_file_size),
                                       &subrange_edits).ok());

        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Recovered Version: {}",
                           versions_->current()->DebugString());
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Recovered lsn:{} lfn:{}", versions_->last_sequence_,
                           versions_->NextFileNumber());

        // Inform all StoCs of the mapping between a file and rtable id.
        std::vector<FileMetaData *> *files = versions_->current()->files_;
        std::map<uint32_t, std::map<std::string, uint32_t>> stoc_fn_rtableid;
        for (int level = 0; level < config::kNumLevels; level++) {
            for (int i = 0; i < files[level].size(); i++) {
                auto meta = files[level][i];
                std::string metafilename = TableFileName(dbname_, meta->number,
                                                         true);
                std::string filename = TableFileName(dbname_, meta->number,
                                                     false);
                auto meta_handle = meta->meta_block_handle;
                stoc_fn_rtableid[meta_handle.server_id][metafilename] = meta_handle.rtable_id;

                for (auto &data_block : meta->data_block_group_handles) {
                    stoc_fn_rtableid[data_block.server_id][filename] = data_block.rtable_id;
                }
            }
        }

        for (auto &mapping : stoc_fn_rtableid) {
            uint32_t stoc_id = mapping.first;
            std::map<std::string, uint32_t> &fnrtable = mapping.second;
            RDMA_LOG(rdmaio::INFO)
                << fmt::format("Recover Install FileRTable mapping {} size:{}",
                               stoc_id,
                               fnrtable.size());
            client->InitiateFileNameRTableMapping(stoc_id, fnrtable);
            client->Wait();
        }


        // Fetch metadata blocks.
        for (int level = 0; level < config::kNumLevels; level++) {
            for (int i = 0; i < files[level].size(); i++) {
                auto meta = files[level][i];
                std::string filename = TableFileName(dbname_, meta->number,
                                                     true);
                uint32_t backing_scid = options_.mem_manager->slabclassid(0,
                                                                          meta->meta_block_handle.size);
                char *backing_buf = options_.mem_manager->ItemAlloc(0,
                                                                    backing_scid);
                RDMA_LOG(rdmaio::INFO)
                    << fmt::format("Recover metadata blocks {} handle:{}",
                                   filename,
                                   meta->meta_block_handle.DebugString());
                client->InitiateRTableReadDataBlock(meta->meta_block_handle, 0,
                                                    meta->meta_block_handle.size,
                                                    backing_buf, filename);
                client->Wait();

                WritableFile *writable_file;
                EnvFileMetadata env_meta = {};
                filename = TableFileName(dbname_, meta->number);
                Status s = env_->NewWritableFile(filename, env_meta,
                                                 &writable_file);
                RDMA_ASSERT(s.ok());
                Slice sstable_rtable(backing_buf,
                                     meta->meta_block_handle.size);
                s = writable_file->Append(sstable_rtable);
                RDMA_ASSERT(s.ok());
                s = writable_file->Flush();
                RDMA_ASSERT(s.ok());
                s = writable_file->Sync();
                RDMA_ASSERT(s.ok());
                s = writable_file->Close();
                RDMA_ASSERT(s.ok());
                delete writable_file;
                writable_file = nullptr;

                options_.mem_manager->FreeItem(0, backing_buf, backing_scid);
            }
        }

        // TODO: Rebuild table locator.
        ReadOptions ro;
        ro.mem_manager = options_.mem_manager;
        ro.dc_client = options_.dc_client;
        ro.thread_id = 0;
        ro.hash = 0;
        uint32_t memtableid = 0;
        for (int i = 0; i < files[0].size(); i++) {
            auto meta = files[0][i];
            auto it = table_cache_->NewIterator(
                    AccessCaller::kCompaction, ro, *meta, meta->number, 0,
                    meta->converted_file_size);
            it->SeekToFirst();
            while (it->Valid()) {
                ParsedInternalKey ik;
                ParseInternalKey(it->key(), &ik);
                uint64_t hash;
                nova::str_to_int(ik.user_key.data(), &hash, ik.user_key.size());
                table_locator_->Insert(ik.user_key, hash, memtableid);

                AtomicMemTable &mem = versions_->mid_table_mapping_[memtableid];
                mem.l0_file_number_ = meta->number;
                mem.is_immutable_ = true;
                mem.is_flushed_ = true;
                it->Next();
            }
            memtableid++;
            delete it;
        }

        SubRanges *srs = latest_subranges_;
        srs->subranges.resize(subrange_edits.size());
        for (const auto &edit : subrange_edits) {
            srs->subranges[edit.subrange_id].lower = SubRange::Copy(edit.lower);
            srs->subranges[edit.subrange_id].upper = SubRange::Copy(edit.upper);
            srs->subranges[edit.subrange_id].lower_inclusive = edit.lower_inclusive;
            srs->subranges[edit.subrange_id].upper_inclusive = edit.upper_inclusive;
        }
        srs->AssertSubrangeBoundary(user_comparator_);
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Recovered Subranges: {}", srs->DebugString());

        for (const auto &logfile : logfile_buf) {
            uint32_t index = logfile.first.find_last_of('-');
            uint32_t log_file_number = std::stoi(
                    logfile.first.substr(index + 1));
            versions_->MarkFileNumberUsed(log_file_number);
        }
        options_.mem_manager->FreeItem(0, buf, scid);

        uint32_t recovered_log_records = 0;
        timeval rdma_read_complete;
        gettimeofday(&rdma_read_complete, nullptr);

        RDMA_ASSERT(RecoverLogFile(logfile_buf, &recovered_log_records,
                                   &rdma_read_complete).ok());
        timeval end = {};
        gettimeofday(&end, nullptr);

        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Total recovery duration: {},{},{},{},{}",
                           logfile_buf.size(),
                           recovered_log_records,
                           time_diff(start, rdma_read_complete),
                           time_diff(rdma_read_complete, end),
                           time_diff(start, end));
        return Status::OK();
    }

    Status
    DBImpl::RecoverLogFile(const std::map<std::string, uint64_t> &logfile_buf,
                           uint32_t *recovered_log_records,
                           timeval *rdma_read_complete) {
        if (logfile_buf.empty()) {
            return Status::OK();
        }

        auto client = reinterpret_cast<NovaBlockCCClient *> (options_.dc_client);
        std::vector<NovaCCRecoveryThread *> recovery_threads;
        uint32_t memtable_per_thread =
                logfile_buf.size() / options_.num_recovery_thread;
        uint32_t remainder = logfile_buf.size() % options_.num_recovery_thread;
        RDMA_ASSERT(logfile_buf.size() < partitioned_active_memtables_.size());
        uint32_t memtable_index = 0;
        uint32_t thread_id = 0;
        while (memtable_index < logfile_buf.size()) {
            std::vector<MemTable *> memtables;
            for (int j = 0; j < memtable_per_thread; j++) {
                memtables.push_back(
                        partitioned_active_memtables_[memtable_index]->memtable);
                memtable_index++;
            }
            if (remainder > 0) {
                memtable_index++;
                remainder--;
            }
            NovaCCRecoveryThread *thread = new NovaCCRecoveryThread(
                    thread_id, memtables, options_.mem_manager);
            recovery_threads.push_back(thread);
            thread_id++;
        }

        // Pin each recovery thread to a core.
        std::vector<std::thread> threads;
        for (int i = 0; i < recovery_threads.size(); i++) {
            threads.emplace_back(&NovaCCRecoveryThread::Recover,
                                 recovery_threads[i]);
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i, &cpuset);
            int rc = pthread_setaffinity_np(threads[i].native_handle(),
                                            sizeof(cpu_set_t), &cpuset);
            RDMA_ASSERT(rc == 0) << rc;
        }

        std::vector<char *> rdma_bufs;
        std::vector<uint32_t> reqs;
        nova::CCFragment *frag = nova::NovaConfig::config->db_fragment[dbid_];
        uint32_t stoc_server_id = nova::NovaConfig::config->dc_servers[frag->log_replica_stoc_ids[0]].server_id;
        for (auto &replica : logfile_buf) {
            uint32_t scid = options_.mem_manager->slabclassid(0,
                                                              options_.max_log_file_size);
            char *rdma_buf = options_.mem_manager->ItemAlloc(0, scid);
            RDMA_ASSERT(rdma_buf);
            rdma_bufs.push_back(rdma_buf);
            uint32_t reqid = client->InitiateReadInMemoryLogFile(rdma_buf,
                                                                 stoc_server_id,
                                                                 replica.second,
                                                                 options_.max_log_file_size);
            reqs.push_back(reqid);
        }

        // Wait for all RDMA READ to complete.
        for (auto &replica : logfile_buf) {
            client->Wait();
        }

        for (int i = 0; i < reqs.size(); i++) {
            leveldb::CCResponse response;
            RDMA_ASSERT(client->IsDone(reqs[i], &response, nullptr));
        }

        gettimeofday(rdma_read_complete, nullptr);

        // put all rdma foreground to sleep.
        for (int i = 0; i < rdma_threads_.size(); i++) {
            auto *thread = reinterpret_cast<nova::NovaRDMAComputeComponent *>(rdma_threads_[i]);
            thread->should_pause = true;
        }

        // Divide.
        RDMA_LOG(rdmaio::INFO)
            << fmt::format(
                    "Start recovery: memtables:{} memtable_per_thread:{}",
                    logfile_buf.size(),
                    memtable_per_thread);

        std::vector<char *> replicas;
        thread_id = 0;
        for (int i = 0; i < logfile_buf.size(); i++) {
            if (replicas.size() == memtable_per_thread) {
                recovery_threads[thread_id]->log_replicas_ = replicas;
                replicas.clear();
                thread_id += 1;
            }
            replicas.push_back(rdma_bufs[i]);
        }

        if (!replicas.empty()) {
            recovery_threads[thread_id]->log_replicas_ = replicas;
        }

        for (int i = 0;
             i < options_.num_recovery_thread; i++) {
            sem_post(&recovery_threads[i]->sem_);
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Start recovery: recovery threads:{}",
                           recovery_threads.size());
        for (int i = 0; i < recovery_threads.size(); i++) {
            threads[i].join();
        }

        uint32_t log_records = 0;
        for (int i = 0; i < recovery_threads.size(); i++) {
            auto recovery = recovery_threads[i];
            log_records += recovery->recovered_log_records;

            RDMA_LOG(rdmaio::INFO)
                << fmt::format("recovery duration of {}: {},{},{},{}",
                               i,
                               recovery->log_replicas_.size(),
                               recovery->recovered_log_records,
                               recovery->new_memtable_time,
                               recovery->recovery_time);
        }
        *recovered_log_records = log_records;

        uint64_t max_sequence = 0;
        for (auto &recovery : recovery_threads) {
            max_sequence = std::max(max_sequence,
                                    recovery->max_sequence_number);
        }

        if (versions_->LastSequence() < max_sequence) {
            versions_->SetLastSequence(max_sequence);
        }

        for (int i = 0; i < rdma_threads_.size(); i++) {
            auto *thread = reinterpret_cast<nova::NovaRDMAComputeComponent *>(rdma_threads_[i]);
            thread->should_pause = false;
            sem_post(&thread->sem_);
        }
        return Status::OK();
    }

    void DBImpl::TestCompact(leveldb::EnvBGThread *bg_thread,
                             const std::vector<leveldb::EnvBGTask> &tasks) {
        VersionEdit edit;
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            FileMetaData &meta = imm->meta();
            meta.number = versions_->NewFileNumber();
            meta.flush_timestamp = versions_->last_sequence_;
            meta.level = 0;
            Status s;
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
                                              AccessCaller::kCompaction);
            s = TestBuildTable(dbname_, env_, options_, table_cache_, iter,
                               &meta, bg_thread);
            RDMA_ASSERT(s.ok()) << s.ToString();
            delete iter;
            // Note that if file_size is zero, the file has been deleted and
            // should not be added to the manifest.
            int level = 0;
            if (meta.file_size > 0) {
                RDMA_ASSERT(imm->memtableid() != 0);
                edit.AddFile(level, imm->memtableid(),
                             meta.number,
                             meta.file_size,
                             meta.converted_file_size,
                             meta.flush_timestamp,
                             meta.smallest,
                             meta.largest,
                             meta.meta_block_handle,
                             meta.data_block_group_handles);
            }
        }
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
        Version *v = new Version(versions_,
                                 versions_->version_id_seq_.fetch_add(1));
        mutex_.Lock();
        Status s = versions_->LogAndApply(&edit, v);
        RDMA_ASSERT(s.ok());
        mutex_.Unlock();

        std::map<uint32_t, std::vector<EnvBGTask>> pid_tasks;
        for (auto &task : tasks) {
            pid_tasks[task.memtable_partition_id].push_back(task);
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            auto atomic_imm = &versions_->mid_table_mapping_[imm->memtableid()];
            atomic_imm->is_immutable_ = true;
            atomic_imm->SetFlushed(dbname_,
                                   imm->meta().number);
        }

        for (auto &it : pid_tasks) {
            // New verion is installed. Then remove it from the immutable memtables.
            MemTablePartition *p = partitioned_active_memtables_[it.first];
            p->mutex.Lock();
            bool no_slots = p->available_slots.empty();
            for (auto &task : it.second) {
                RDMA_ASSERT(task.imm_slot < partitioned_imms_.size());
                RDMA_ASSERT(partitioned_imms_[task.imm_slot] != 0);
                partitioned_imms_[task.imm_slot] = 0;
                p->available_slots.push(task.imm_slot);
                RDMA_ASSERT(p->available_slots.size() <= p->imm_slots.size());
            }
            if (no_slots) {
                p->background_work_finished_signal_.SignalAll();
            }
            number_of_immutable_memtables_.fetch_add(-it.second.size());
            p->mutex.Unlock();
        }
    }

    bool DBImpl::CompactMemTableStaticPartition(leveldb::EnvBGThread *bg_thread,
                                                const std::vector<leveldb::EnvBGTask> &tasks) {
        VersionEdit edit;
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            RDMA_ASSERT(imm);
            FileMetaData &meta = imm->meta();
            meta.number = versions_->NewFileNumber();
            meta.flush_timestamp = versions_->last_sequence_;
            meta.level = 0;
            Status s;
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
                                              AccessCaller::kCompaction);
            s = BuildTable(dbname_, env_, options_, table_cache_, iter,
                           &meta, bg_thread);
            RDMA_ASSERT(s.ok()) << s.ToString();
            delete iter;
            // Note that if file_size is zero, the file has been deleted and
            // should not be added to the manifest.
            int level = 0;
            if (meta.file_size > 0) {
                RDMA_ASSERT(imm->memtableid() != 0);
                edit.AddFile(level, imm->memtableid(),
                             meta.number,
                             meta.file_size,
                             meta.converted_file_size,
                             meta.flush_timestamp,
                             meta.smallest,
                             meta.largest,
                             meta.meta_block_handle,
                             meta.data_block_group_handles);
            }
        }

        // Include the latest version.
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);

        Version *v = new Version(versions_,
                                 versions_->version_id_seq_.fetch_add(1));
        mutex_.Lock();
        Status s = versions_->LogAndApply(&edit, v);
        RDMA_ASSERT(s.ok());
        mutex_.Unlock();

        std::map<uint32_t, std::vector<EnvBGTask>> pid_tasks;
        for (auto &task : tasks) {
            pid_tasks[task.memtable_partition_id].push_back(task);
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            auto atomic_imm = &versions_->mid_table_mapping_[imm->memtableid()];
            atomic_imm->is_immutable_ = true;
            atomic_imm->SetFlushed(dbname_,
                                   imm->meta().number);
        }

        std::vector<uint32_t> closed_memtable_log_files;
        for (auto &it : pid_tasks) {
            // New verion is installed. Then remove it from the immutable memtables.
            MemTablePartition *p = partitioned_active_memtables_[it.first];
            p->mutex.Lock();
            bool no_slots = p->available_slots.empty();
            for (auto &task : it.second) {
                RDMA_ASSERT(task.imm_slot < partitioned_imms_.size());
                RDMA_ASSERT(partitioned_imms_[task.imm_slot] != 0);
                partitioned_imms_[task.imm_slot] = 0;
                p->available_slots.push(task.imm_slot);
                RDMA_ASSERT(p->available_slots.size() <= p->imm_slots.size());
            }
            if (no_slots) {
                p->background_work_finished_signal_.SignalAll();
            }
            number_of_immutable_memtables_.fetch_add(-it.second.size());
            for (auto &log_file : p->closed_log_files) {
                closed_memtable_log_files.push_back(log_file);
            }
            p->closed_log_files.clear();
            p->mutex.Unlock();
        }

        // Delete log files.
        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA) {
            for (const auto &file : closed_memtable_log_files) {
                bg_thread->dc_client()->InitiateCloseLogFile(
                        fmt::format("{}-{}-{}", server_id_, dbid_, file),
                        dbid_);
            }
        }
    }

    bool DBImpl::CompactMemTable(EnvBGThread *bg_thread,
                                 const std::vector<EnvBGTask> &tasks) {
        VersionEdit edit;
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            versions_->mid_table_mapping_[imm->memtableid()].mutex_.lock();
            RDMA_ASSERT(
                    versions_->mid_table_mapping_[imm->memtableid()].is_immutable_);
            RDMA_ASSERT(
                    !versions_->mid_table_mapping_[imm->memtableid()].is_flushed_);
            versions_->mid_table_mapping_[imm->memtableid()].mutex_.unlock();


            FileMetaData &meta = imm->meta();
            meta.number = versions_->NewFileNumber();
            meta.flush_timestamp = versions_->last_sequence_;
            meta.level = 0;

            Status s;
            Iterator *iter = imm->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
                                              AccessCaller::kCompaction);
            s = BuildTable(dbname_, env_, options_, table_cache_, iter,
                           &meta,
                           bg_thread);
            RDMA_ASSERT(s.ok()) << s.ToString();
            delete iter;
            // Note that if file_size is zero, the file has been deleted and
            // should not be added to the manifest.
            int level = 0;
            if (meta.file_size > 0) {
                RDMA_ASSERT(imm->memtableid() != 0);
                edit.AddFile(level, imm->memtableid(),
                             meta.number,
                             meta.file_size,
                             meta.converted_file_size,
                             meta.flush_timestamp,
                             meta.smallest,
                             meta.largest,
                             meta.meta_block_handle,
                             meta.data_block_group_handles);
            }
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "db[{}]: !!!!!!!!!!!!!!!!!!!!!!!!!!!! Flush memtable-{}",
                        dbid_, imm->memtableid());
        }

        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
        Version *v = new Version(versions_,
                                 versions_->version_id_seq_.fetch_add(1));
        RDMA_ASSERT(v->version_id() < MAX_LIVE_MEMTABLES);
        mutex_.Lock();
        Status s = versions_->LogAndApply(&edit, v);
        RDMA_ASSERT(s.ok());
        mutex_.Unlock();

        uint32_t num_available = 0;
        bool wakeup_all = false;
        range_lock_.Lock();
        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            if (imm->is_pinned_) {
                number_of_available_pinned_memtables_++;
                RDMA_ASSERT(number_of_available_pinned_memtables_ <=
                            min_memtables_);
            } else {
                num_available += 1;
                wakeup_all = true;
            }
        }
        number_of_immutable_memtables_ -= tasks.size();
        std::vector<uint32_t> closed_memtable_log_files(
                closed_memtable_log_files_.begin(),
                closed_memtable_log_files_.end());
        closed_memtable_log_files_.clear();
        range_lock_.Unlock();

        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA) {
            for (const auto &file : closed_memtable_log_files) {
                bg_thread->dc_client()->InitiateCloseLogFile(
                        fmt::format("{}-{}-{}", server_id_, dbid_, file),
                        dbid_);
            }
        }

        options_.memtable_pool->mutex_.lock();
        options_.memtable_pool->num_available_memtables_ += num_available;
        RDMA_ASSERT(options_.memtable_pool->num_available_memtables_ <
                    nova::NovaConfig::config->num_memtables - dbs_.size());
        options_.memtable_pool->mutex_.unlock();

        if (wakeup_all) {
            for (int i = 0; i < dbs_.size(); i++) {
                options_.memtable_pool->range_cond_vars_[i]->SignalAll();
            }
        } else {
            options_.memtable_pool->range_cond_vars_[dbid_]->SignalAll();
        }

        for (auto &task : tasks) {
            MemTable *imm = reinterpret_cast<MemTable *>(task.memtable);
            versions_->mid_table_mapping_[imm->memtableid()].SetFlushed(dbname_,
                                                                        imm->meta().number);
        }
        return true;
    }

    void DBImpl::CompactRange(const Slice *begin, const Slice *end) {
        int max_level_with_files = 1;
        {
            MutexLock l(&mutex_);
            Version *base = versions_->current();
            for (int level = 1; level < config::kNumLevels; level++) {
                if (base->OverlapInLevel(level, begin, end)) {
                    max_level_with_files = level;
                }
            }
        }
        TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
        for (int level = 0; level < max_level_with_files; level++) {
            TEST_CompactRange(level, begin, end);
        }
    }

    void DBImpl::TEST_CompactRange(int level, const Slice *begin,
                                   const Slice *end) {
        assert(level >= 0);
        assert(level + 1 < config::kNumLevels);

        InternalKey begin_storage, end_storage;

        ManualCompaction manual;
        manual.level = level;
        manual.done = false;
        if (begin == nullptr) {
            manual.begin = nullptr;
        } else {
            begin_storage = InternalKey(*begin, kMaxSequenceNumber,
                                        kValueTypeForSeek);
            manual.begin = &begin_storage;
        }
        if (end == nullptr) {
            manual.end = nullptr;
        } else {
            end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
            manual.end = &end_storage;
        }

        MutexLock l(&mutex_);
        while (!manual.done &&
               !shutting_down_.load(std::memory_order_acquire) &&
               bg_error_.ok()) {
            if (manual_compaction_ == nullptr) {  // Idle
                manual_compaction_ = &manual;
//                MaybeScheduleCompaction();
            } else {  // Running either my compaction or another compaction.
            }
        }
        if (manual_compaction_ == &manual) {
            // Cancel my manual compaction since we aborted early for some reason.
            manual_compaction_ = nullptr;
        }
    }

    Status DBImpl::TEST_CompactMemTable() {
        // nullptr batch means just wait for earlier writes to be done
//        Status s = Write(WriteOptions(), nullptr);
//        if (s.ok()) {
        // Wait until the compaction completes
//            MutexLock l(&mutex_);
//            while (imm_ != nullptr && bg_error_.ok()) {
//                background_work_finished_signal_.Wait();
//            }
//            if (imm_ != nullptr) {
//                s = bg_error_;
//            }
//        }
        return Status::OK();
//        return s;
    }

    void DBImpl::RecordBackgroundError(const Status &s) {
        mutex_.AssertHeld();
        if (bg_error_.ok()) {
            bg_error_ = s;
        }
    }

    void DBImpl::MaybeScheduleCompaction(uint32_t thread_id,
                                         MemTable *imm,
                                         uint32_t partition_id,
                                         uint32_t imm_slot,
                                         unsigned int *rand_seed) {
        uint32_t i = EnvBGThread::bg_thread_id_seq.fetch_add(1,
                                                             std::memory_order_relaxed) %
                     compaction_threads_.size();
        EnvBGTask task = {};
        task.db = this;
        task.memtable = imm;
        task.memtable_size_mb = imm->ApproximateMemoryUsage() / 1024 / 1024;
        task.memtable_partition_id = partition_id;
        task.imm_slot = imm_slot;
        if (compaction_threads_[i]->Schedule(task)) {
        }
    }

    void DBImpl::PerformSubrangeMajorReorg(leveldb::DBImpl::SubRanges *latest,
                                           double total_inserts) {
        std::vector<double> insertion_rates;
        std::vector<SubRange> &subranges = latest->subranges;
        // Perform major reorg.
        std::vector<std::vector<AtomicMemTable *>> subrange_imms;
        uint32_t nslots = (options_.num_memtables -
                           options_.num_memtable_partitions) /
                          options_.num_memtable_partitions;
        uint32_t remainder = (options_.num_memtables -
                              options_.num_memtable_partitions) %
                             options_.num_memtable_partitions;
        uint32_t slot_id = 0;
        for (int i = 0; i < options_.num_memtable_partitions; i++) {
            std::vector<AtomicMemTable *> memtables;

            partitioned_active_memtables_[i]->mutex.Lock();
            MemTable *m = partitioned_active_memtables_[i]->memtable;
            if (m) {
                RDMA_ASSERT(versions_->mid_table_mapping_[m->memtableid()].Ref(
                        nullptr));
                memtables.push_back(
                        &versions_->mid_table_mapping_[m->memtableid()]);
            }
            uint32_t slots = nslots;
            if (remainder > 0) {
                slots += 1;
                remainder--;
            }
            for (int j = 0; j < slots; j++) {
                uint32_t imm_id = partitioned_imms_[slot_id + j];
                if (imm_id == 0) {
                    continue;
                }
                MemTable *imm = versions_->mid_table_mapping_[imm_id].Ref(
                        nullptr);
                if (imm) {
                    memtables.push_back(&versions_->mid_table_mapping_[imm_id]);
                }
            }
            slot_id += slots;
            partitioned_active_memtables_[i]->mutex.Unlock();

            subrange_imms.push_back(memtables);
        }

        // We have all memtables now.
        // Determine the number of samples we retrieve from each subrange.
        uint32_t sample_size_per_subrange = UINT32_MAX;
        std::vector<uint32_t> subrange_nputs;
        std::vector<std::vector<uint32_t>> subrange_mem_nputs;
        for (int i = 0; i < subrange_imms.size(); i++) {
            uint32_t nputs = 0;
            std::vector<uint32_t> ns;
            for (int j = 0; j < subrange_imms[i].size(); j++) {
                uint32_t n = subrange_imms[i][j]->nentries_.load(
                        std::memory_order_relaxed);
                ns.push_back(n);

                nputs += n;
            }
            subrange_mem_nputs.push_back(ns);
            subrange_nputs.push_back(nputs);
            if (nputs > 100) {
                sample_size_per_subrange = std::min(sample_size_per_subrange,
                                                    nputs);
            }
        }

        // Sample from each memtable.
        sample_size_per_subrange = (double) (sample_size_per_subrange) *
                                   options_.subrange_reorg_sampling_ratio;
        auto comp = [&](const std::string &a, const std::string &b) {
            return user_comparator_->Compare(a, b);
        };
        std::map<uint64_t, double> userkey_rate;
        double total_rate = 0;

        for (int i = 0; i < subrange_imms.size(); i++) {
            SubRange &sr = subranges[i];
            uint32_t total_puts = subrange_nputs[i];
            double insertion_ratio = subranges[i].insertion_ratio;

            for (int j = 0; j < subrange_imms[i].size(); j++) {
                AtomicMemTable *mem = subrange_imms[i][j];
                uint32_t sample_size = 0;
                if (total_puts <= sample_size_per_subrange) {
                    // sample everything.
                    sample_size = sample_size_per_subrange;
                } else {
                    sample_size = ((double) subrange_mem_nputs[i][j] /
                                   (double) total_puts) *
                                  sample_size_per_subrange;
                }

                uint32_t samples = 0;
                leveldb::Iterator *it = mem->memtable_->NewIterator(
                        TraceType::MEMTABLE, AccessCaller::kUncategorized,
                        sample_size);
                it->SeekToFirst();
                ParsedInternalKey ik;
                while (it->Valid() && samples < sample_size) {
                    RDMA_ASSERT(ParseInternalKey(it->key(), &ik));
                    uint64_t k = 0;
                    nova::str_to_int(ik.user_key.data(), &k,
                                     ik.user_key.size());
                    userkey_rate[k] += insertion_ratio;
                    total_rate += insertion_ratio;
                    samples += 1;
                    it->Next();
                }

                RDMA_LOG(rdmaio::INFO)
                    << fmt::format("Sample {} {} {} {} from mid-{} subrange-{}",
                                   samples, sample_size,
                                   subrange_mem_nputs[i][j], total_puts,
                                   mem->memtable_->memtableid(), i);
                delete it;
            }
        }

        if (userkey_rate.size() <= options_.num_memtable_partitions * 2) {
            num_skipped_major_reorgs++;

            // Unref all immutable memtables.
            for (int i = 0; i < subrange_imms.size(); i++) {
                for (int j = 0; j < subrange_imms[i].size(); j++) {
                    subrange_imms[i][j]->Unref(dbname_);
                }
            }
            delete latest;
            return;
        }

        last_major_reorg_seq_ = versions_->last_sequence_;
        last_minor_reorg_seq_ = versions_->last_sequence_;
        num_major_reorgs++;
        double share_per_subrange =
                total_rate / options_.num_memtable_partitions;
        double fair_share = 1.0 /
                            (double) options_.num_memtable_partitions;
        double fair_rate = fair_share * total_rate;
        double remaining_rate = total_rate;

        int index = 0;
        double sum = 0;

        std::string lower = subranges[0].lower.ToString();
        bool li = subranges[0].lower_inclusive;
        std::string upper = subranges[subranges.size() - 1].upper.ToString();
        bool ui = subranges[subranges.size() - 1].upper_inclusive;

        for (int i = 0; i < subranges.size(); i++) {
            delete subranges[i].lower.data();
            delete subranges[i].upper.data();
            subranges[i].lower = {};
            subranges[i].upper = {};
            subranges[i].num_duplicates = 0;
        }
        subranges.clear();
        subranges.resize(options_.num_memtable_partitions);
        subranges[0].lower_inclusive = li;
        subranges[0].lower = SubRange::Copy(lower);
        subranges[subranges.size() - 1].upper_inclusive = ui;
        subranges[subranges.size() - 1].upper = SubRange::Copy(upper);

        for (auto entry = userkey_rate.begin();
             entry != userkey_rate.end(); entry++) {
            double keyrate = entry->second;
            if (keyrate >= 1.5 * fair_rate) {
                // a hot key.
                RDMA_LOG(rdmaio::INFO)
                    << fmt::format("hot key {} rate:{} fair:{}", entry->first,
                                   keyrate, fair_rate);
                std::string userkey = std::to_string(entry->first);
                if (subranges[index].IsGreaterThanLower(userkey,
                                                        user_comparator_)) {
                    // close the current subrange.
                    subranges[index].upper = SubRange::Copy(userkey);
                    subranges[index].upper_inclusive = false;
                    index++;
                }

                int num_duplicates = std::ceil(keyrate / fair_rate);
                for (int i = 0; i < num_duplicates; i++) {
                    subranges[index].num_duplicates = num_duplicates;
                    subranges[index].lower = SubRange::Copy(userkey);
                    subranges[index].lower_inclusive = true;
                    subranges[index].upper = SubRange::Copy(userkey);
                    subranges[index].upper_inclusive = true;
                    index++;
                }
                entry++;
                auto next = entry;
                subranges[index].lower = SubRange::Copy(
                        std::to_string(next->first));
                subranges[index].lower_inclusive = true;
                remaining_rate -= keyrate;
                sum = 0;
                share_per_subrange =
                        remaining_rate / (subranges.size() - index);
                entry--;
                continue;
            }

            if (sum + entry->second > share_per_subrange) {
                // Close the current subrange.
                Slice userkey = std::to_string(entry->first);
                if (user_comparator_->Compare(userkey,
                                              subranges[index].lower) == 0 &&
                    subranges[index].lower_inclusive) {
                    // A single point.
                    subranges[index].upper = SubRange::Copy(userkey);
                    subranges[index].upper_inclusive = true;

                    remaining_rate -= entry->second;

                    entry++;
                    index++;
                    subranges[index].lower = SubRange::Copy(
                            std::to_string(entry->first));
                    subranges[index].lower_inclusive = true;

                    sum = entry->second;
                    share_per_subrange =
                            remaining_rate / (subranges.size() - index);
                    remaining_rate -= entry->second;
                    continue;
                } else {
                    subranges[index].upper = SubRange::Copy(userkey);
                    subranges[index].upper_inclusive = false;

                    index++;
                    subranges[index].lower = SubRange::Copy(userkey);
                    subranges[index].lower_inclusive = true;
                    if (index == subranges.size() - 1) {
                        break;
                    }
                    share_per_subrange =
                            remaining_rate / (subranges.size() - index);
                }
                sum = 0;
            }
            sum += entry->second;
            remaining_rate -= entry->second;
        }

        RDMA_ASSERT(index == subranges.size() - 1);
        latest->AssertSubrangeBoundary(user_comparator_);

        bool bad_reorg = false;
        for (int i = 0; i < subranges.size(); i++) {
            SubRange &sr = subranges[i];
            if (user_comparator_->Compare(sr.upper, sr.lower) < 0) {
                // This is a bad reorg due to too few samples. give up.
                bad_reorg = true;
                break;
            }
            sr.ninserts = 0; //total_inserts / subranges.size();
        }

        // Unref all immutable memtables.
        for (int i = 0; i < subrange_imms.size(); i++) {
            for (int j = 0; j < subrange_imms[i].size(); j++) {
                subrange_imms[i][j]->Unref(dbname_);
            }
        }

        if (bad_reorg) {
            RDMA_LOG(rdmaio::INFO)
                << fmt::format("bad reorg: {}", latest->DebugString());
            delete latest;
            return;
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("major at {} puts with {} keys: {}",
                           processed_writes_,
                           userkey_rate.size(), latest->DebugString());
        latest_subranges_.store(latest);

        VersionEdit edit;
        for (int i = 0; i < subranges.size(); i++) {
            edit.UpdateSubRange(i, subranges[i].lower, subranges[i].upper,
                                subranges[i].lower_inclusive,
                                subranges[i].upper_inclusive);
        }
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
    }

    void DBImpl::MoveShareDuplicateSubranges(SubRanges *latest, int index) {
        SubRange &sr = latest->subranges[index];
        int nDuplicates = 0;
        int start = -1;
        int end = -1;
        for (int i = 0; i < latest->subranges.size(); i++) {
            if (!latest->subranges[i].Equals(sr, user_comparator_)) {
                continue;
            }
            end = i;
            if (start == -1) {
                start = i;
            }
        }
        RDMA_ASSERT(end - start + 1 == sr.num_duplicates);
        nDuplicates = sr.num_duplicates - 1;
        double share = sr.ninserts / nDuplicates;
        for (int i = start; i <= end; i++) {
            SubRange &dup = latest->subranges[i];
            dup.ninserts += share;
            dup.num_duplicates -= 1;
            if (nDuplicates == 1) {
                dup.num_duplicates = 0;
            }
        }
    }

    bool
    DBImpl::MinorReorgDestroyDuplicates(SubRanges *latest, int subrange_id) {
        SubRange &sr = latest->subranges[subrange_id];
        if (sr.num_duplicates == 0) {
            return false;
        }

        double fair_ratio = 1.0 / options_.num_memtable_partitions;
        double percent = sr.insertion_ratio / fair_ratio;
        if (percent >= 0.5) {
            return false;
        }

        // destroy this duplicate.
        MoveShareDuplicateSubranges(latest, subrange_id);
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Destroy subrange {} {}", subrange_id,
                           sr.DebugString());
        latest->subranges.erase(latest->subranges.begin() + subrange_id);
        VersionEdit edit;
        std::vector<SubRange> &subranges = latest->subranges;
        for (int i = 0; i < subranges.size(); i++) {
            edit.UpdateSubRange(i, subranges[i].lower, subranges[i].upper,
                                subranges[i].lower_inclusive,
                                subranges[i].upper_inclusive);
        }
        latest_subranges_.store(latest);
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
        num_minor_reorgs_for_dup += 1;
        last_minor_reorg_seq_ = versions_->last_sequence_;
        return true;
    }

    bool DBImpl::PerformSubrangeMinorReorgDuplicate(int subrange_id,
                                                    leveldb::DBImpl::SubRanges *latest,
                                                    double total_inserts) {
        double fair_ratio = 1.0 / (double) options_.num_memtable_partitions;
        std::vector<SubRange> &subranges = latest->subranges;
        SubRange &sr = subranges[subrange_id];
        RDMA_ASSERT(sr.insertion_ratio > fair_ratio);

        if (!sr.IsAPoint(user_comparator_)) {
            return false;
        }

        int num_duplicates = (int) std::floor(sr.insertion_ratio / fair_ratio);
        if (num_duplicates == 0) {
            return false;
        }

        num_minor_reorgs_for_dup++;
        last_minor_reorg_seq_ = versions_->last_sequence_;

        uint64_t total_sr_inserts = sr.ninserts;
        uint64_t remaining_sum = sr.ninserts;
        int new_num_duplicates = sr.num_duplicates + num_duplicates + 1;
        Slice lower = sr.lower;
        for (int i = 0; i < num_duplicates; i++) {
            SubRange new_sr = {};
            new_sr.lower = SubRange::Copy(lower);
            new_sr.upper = SubRange::Copy(lower);
            new_sr.lower_inclusive = true;
            new_sr.upper_inclusive = true;
            new_sr.num_duplicates = new_num_duplicates;
            new_sr.ninserts = total_sr_inserts / (num_duplicates + 1);

            remaining_sum -= new_sr.ninserts;
            subranges.insert(subranges.begin() + subrange_id + 1, new_sr);
        }

        subranges[subrange_id].ninserts = remaining_sum;
        subranges[subrange_id].num_duplicates = new_num_duplicates;

        int start = -1;
        int end = -1;
        for (int i = 0; i < subranges.size(); i++) {
            if (!subranges[i].Equals(subranges[subrange_id],
                                     user_comparator_)) {
                continue;
            }
            end = i;
            subranges[i].num_duplicates = new_num_duplicates;
            if (start == -1) {
                start = i;
            }
        }
        RDMA_ASSERT(end - start + 1 == subranges[subrange_id].num_duplicates);

        while (subranges.size() > options_.num_memtable_partitions) {
            // remove min.
            double min_ratio = 9999999;
            int min_range_id = -1;
            for (int i = 0; i < subranges.size(); i++) {
                // Skip the new subranges.
                if (i >= start && i <= end) {
                    continue;
                }

                SubRange &min_sr = subranges[i];
                if (user_comparator_->Compare(min_sr.lower, lower) == 0) {
                    continue;
                }
                if (min_sr.insertion_ratio < min_ratio) {
                    min_ratio = min_sr.insertion_ratio;
                    min_range_id = i;
                }
            }

            // merge min with neighboring subrange.
            int left = min_range_id - 1;
            int right = min_range_id + 1;
            bool merge_left = true;
            if (left >= 0 && right < subranges.size()) {
                if (subranges[left].insertion_ratio <
                    subranges[right].insertion_ratio) {
                    merge_left = true;
                } else {
                    merge_left = false;
                }
            } else if (left >= 0) {
                merge_left = true;
            } else {
                merge_left = false;
            }
            SubRange &min_sr = subranges[min_range_id];
            if (merge_left) {
                SubRange &l = subranges[left];
                if (l.num_duplicates > 0) {
                    // move its shares to other duplicates.
                    MoveShareDuplicateSubranges(latest, left);
                    // destroy this subrange.
                    subranges.erase(subranges.begin() + left);
                } else {
                    l.ninserts += min_sr.ninserts;
                    l.upper = min_sr.upper;
                    l.upper_inclusive = min_sr.upper_inclusive;
                    subranges.erase(subranges.begin() + min_range_id);
                }
            } else {
                SubRange &r = subranges[right];
                if (r.num_duplicates > 0) {
                    // move its shares to other duplicates.
                    MoveShareDuplicateSubranges(latest, right);
                    // destroy this subrange.
                    subranges.erase(subranges.begin() + right);
                } else {
                    r.ninserts += min_sr.ninserts;
                    r.lower = min_sr.lower;
                    r.lower_inclusive = min_sr.lower_inclusive;
                    subranges.erase(subranges.begin() + min_range_id);
                }
            }
        }
        VersionEdit edit;
        for (int i = 0; i < subranges.size(); i++) {
            edit.UpdateSubRange(i, subranges[i].lower, subranges[i].upper,
                                subranges[i].lower_inclusive,
                                subranges[i].upper_inclusive);
        }
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Duplicate {} subrange-{} {}", num_duplicates,
                           subrange_id, latest->DebugString());

        latest_subranges_.store(latest);
        versions_->AppendChangesToManifest(&edit, manifest_file_,
                                           options_.manifest_stoc_id);
        return true;
    }

    void DBImpl::PerformSubrangeMinorReorg(int subrange_id,
                                           leveldb::DBImpl::SubRanges *latest,
                                           double total_inserts) {
        last_minor_reorg_seq_ = versions_->last_sequence_;
        std::vector<SubRange> &subranges = latest->subranges;
        double fair_ratio = 1.0 / (double) options_.num_memtable_partitions;

        SubRange &sr = subranges[subrange_id];
        RDMA_ASSERT(sr.insertion_ratio > fair_ratio);

        if (PerformSubrangeMinorReorgDuplicate(subrange_id, latest,
                                               total_inserts)) {
            return;
        }

        // higher share.
        // Perform major reorg.
        std::vector<AtomicMemTable *> subrange_imms;
        uint32_t nslots = (options_.num_memtables -
                           options_.num_memtable_partitions) /
                          options_.num_memtable_partitions;
        uint32_t remainder = (options_.num_memtables -
                              options_.num_memtable_partitions) %
                             options_.num_memtable_partitions;
        uint32_t slot_id = 0;
        uint32_t slots = 0;
        for (int i = 0; i < subrange_id; i++) {
            slots = nslots;
            if (remainder > 0) {
                slots += 1;
                remainder--;
            }
            slot_id += slots;
        }

        slots = nslots;
        if (remainder > 0) {
            slots += 1;
        }

        partitioned_active_memtables_[subrange_id]->mutex.Lock();
        MemTable *m = partitioned_active_memtables_[subrange_id]->memtable;
        if (m) {
            RDMA_ASSERT(versions_->mid_table_mapping_[m->memtableid()].Ref(
                    nullptr));
            subrange_imms.push_back(
                    &versions_->mid_table_mapping_[m->memtableid()]);
        }

        for (int j = 0; j < slots; j++) {
            RDMA_ASSERT(slot_id + j < partitioned_imms_.size());
            uint32_t imm_id = partitioned_imms_[slot_id + j];
            if (imm_id == 0) {
                continue;
            }
            MemTable *imm = versions_->mid_table_mapping_[imm_id].Ref(
                    nullptr);
            if (imm) {
                subrange_imms.push_back(&versions_->mid_table_mapping_[imm_id]);
            }
        }
        partitioned_active_memtables_[subrange_id]->mutex.Unlock();

        // We have all memtables now.
        std::map<uint64_t, uint32_t> userkey_freq;
        uint32_t total_accesses = 0;
        for (int i = 0; i < subrange_imms.size(); i++) {
            AtomicMemTable *mem = subrange_imms[i];
            uint32_t samples = 0;
            leveldb::Iterator *it = mem->memtable_->NewIterator(
                    TraceType::MEMTABLE, AccessCaller::kUncategorized,
                    0);
            it->SeekToFirst();
            ParsedInternalKey ik;
            while (it->Valid()) {
                RDMA_ASSERT(ParseInternalKey(it->key(), &ik));
                if (sr.IsSmallerThanLower(ik.user_key, user_comparator_)) {
                    it->Next();
                    continue;
                }
                if (sr.IsGreaterThanUpper(ik.user_key, user_comparator_)) {
                    it->Next();
                    continue;
                }

                uint64_t k = 0;
                nova::str_to_int(ik.user_key.data(), &k,
                                 ik.user_key.size());
                userkey_freq[k] += 1;
                samples += 1;
                total_accesses += 1;
                it->Next();
            }
//            RDMA_LOG(rdmaio::INFO)
//                << fmt::format("minor sample {} from mid-{} subrange-{}",
//                               samples, mem->memtable_->memtableid(),
//                               subrange_id);
            delete it;
        }

        if (userkey_freq.size() <= 1 || total_accesses <= 100) {
            num_skipped_minor_reorgs++;
            // Unref all immutable memtables.
            for (int j = 0; j < subrange_imms.size(); j++) {
                subrange_imms[j]->Unref(dbname_);
            }
            delete latest;
            return;
        }

        RDMA_LOG(rdmaio::INFO)
            << fmt::format(
                    "minor at {} puts with {} keys for subrange-{}: before {}",
                    total_inserts,
                    userkey_freq.size(), subrange_id,
                    sr.DebugString());

        double inserts = (sr.insertion_ratio - fair_ratio) * total_inserts;
        RDMA_ASSERT(inserts < sr.ninserts)
            << fmt::format("{} inserts:{} fair:{}", sr.DebugString(), inserts,
                           fair_ratio);
        double remove_share = (double) (
                ((sr.insertion_ratio - fair_ratio) / sr.insertion_ratio)
                * total_accesses);
        double left_share = remove_share;
        double right_share = remove_share;
        double left_inserts = inserts;
        double right_inserts = inserts;

        double total_rate_to_distribute = sr.insertion_ratio - fair_ratio;
        double remaining_rate = total_rate_to_distribute;
        double left_rate = 0;
        double right_rate = 0;

        if (subrange_id != 0 && subrange_id != subranges.size() - 1) {
            SubRange &left = subranges[subrange_id - 1];
            SubRange &right = subranges[subrange_id + 1];
            // they are not duplicates.
            if (left.num_duplicates == 0 && right.num_duplicates == 0) {
                if (left.insertion_ratio > fair_ratio
                    && right.insertion_ratio < fair_ratio) {
                    double need_rate = fair_ratio - right.insertion_ratio;
                    if (total_rate_to_distribute < need_rate) {
                        right_rate = total_rate_to_distribute;
                        remaining_rate = 0;
                    } else {
                        right_rate = need_rate;
                        remaining_rate = total_rate_to_distribute - need_rate;
                    }
                } else if (left.insertion_ratio < fair_ratio
                           && right.insertion_ratio > fair_ratio) {
                    double need_rate = fair_ratio - left.insertion_ratio;
                    if (total_rate_to_distribute < need_rate) {
                        left_rate = total_rate_to_distribute;
                        remaining_rate = 0;
                    } else {
                        left_rate = need_rate;
                        remaining_rate = total_rate_to_distribute - need_rate;
                    }
                }

                if (remaining_rate > 0) {
                    double new_right_rate = right.insertion_ratio + right_rate;
                    double new_left_rate = left.insertion_ratio + left_rate;

                    double total = new_left_rate + new_right_rate;
                    double leftp = new_right_rate / total;
                    double rightp = new_left_rate / total;

                    left_rate += leftp * remaining_rate;
                    right_rate += rightp * remaining_rate;
                }

                RDMA_ASSERT(std::abs(
                        total_rate_to_distribute - left_rate - right_rate) <=
                            0.001);
                double leftp = left_rate / total_rate_to_distribute;
                double rightp = right_rate / total_rate_to_distribute;
                left_share = remove_share * leftp;
                left_inserts = inserts * leftp;
                right_share = remove_share * rightp;
                right_inserts = inserts * rightp;

                if (left_share < userkey_freq.begin()->second
                    && right_share < userkey_freq.rbegin()->second) {
                    // cannot move either to left or right.
                    // move to right.
                    right_share = userkey_freq.rbegin()->second;
                    right_inserts += left_inserts;
                    left_share = 0;
                    left_inserts = 0;
                }
            }
        }

        bool success = false;
        if (subrange_id > 0 && left_share > 0 &&
            subranges[subrange_id].num_duplicates == 0) {
            uint64_t sr_lower;
            uint64_t new_lower;
            nova::str_to_int(sr.lower.data(), &new_lower, sr.lower.size());
            sr_lower = new_lower;
            double removes = left_share;
            auto it = userkey_freq.begin();
            while (it != userkey_freq.end()) {
                if (removes - it->second < 0) {
                    break;
                }
                removes -= it->second;
                new_lower = it->first;
                it = userkey_freq.erase(it);
            }
            if (new_lower > sr_lower) {
                sr.lower = SubRange::Copy(std::to_string(new_lower));
                sr.lower_inclusive = true;
                sr.ninserts -= left_inserts;

                SubRange &other = subranges[subrange_id - 1];
                other.ninserts += left_inserts;
                other.upper = SubRange::Copy(sr.lower);
                other.upper_inclusive = false;
                success = true;
            }
        }
        if (subrange_id < subranges.size() - 1 && right_share > 0 &&
            subranges[subrange_id + 1].num_duplicates == 0) {
            uint64_t sr_upper;
            uint64_t new_upper;
            uint64_t sr_lower;
            nova::str_to_int(sr.upper.data(), &new_upper, sr.upper.size());
            nova::str_to_int(sr.lower.data(), &sr_lower, sr.lower.size());
            sr_upper = new_upper;
            if (!sr.lower_inclusive) {
                sr_lower += 1;
            }
            double removes = right_share;
            for (auto it = userkey_freq.rbegin();
                 it != userkey_freq.rend(); it++) {
                if (removes - it->second < 0) {
                    break;
                }
                removes -= it->second;
                new_upper = it->first;
            }
            if (new_upper < sr_upper && new_upper - sr_lower >= 1) {
                sr.upper = SubRange::Copy(std::to_string(new_upper));
                sr.upper_inclusive = false;
                sr.ninserts -= right_inserts;

                SubRange &other = subranges[subrange_id + 1];
                other.ninserts += right_inserts;
                other.lower = SubRange::Copy(sr.upper);
                other.lower_inclusive = true;
                success = true;
            }
        }

        // Unref all immutable memtables.
        for (int j = 0; j < subrange_imms.size(); j++) {
            subrange_imms[j]->Unref(dbname_);
        }

        if (success) {
            RDMA_LOG(rdmaio::INFO)
                << fmt::format(
                        "minor at {} puts with {} keys for subrange-{}: after {}",
                        total_inserts,
                        userkey_freq.size(), subrange_id,
                        sr.DebugString());
            num_minor_reorgs += 1;
            latest_subranges_.store(latest);

            VersionEdit edit;
            for (int i = -1; i <= 1; i++) {
                int sr_id = subrange_id + i;
                if (sr_id < 0 || sr_id >= subranges.size()) {
                    continue;
                }
                SubRange &sr = subranges[sr_id];
                edit.UpdateSubRange(sr_id, sr.lower, sr.upper,
                                    sr.lower_inclusive,
                                    sr.upper_inclusive);
            }
            versions_->AppendChangesToManifest(&edit, manifest_file_,
                                               options_.manifest_stoc_id);
            return;
        }

        delete latest;
        num_skipped_minor_reorgs++;
    }

    void DBImpl::PerformSubRangeReorganization() {
        RDMA_ASSERT(versions_->last_sequence_ > SUBRANGE_WARMUP_NPUTS);
        SubRanges *ref = latest_subranges_;
        double fair_ratio = 1.0 / (double) options_.num_memtable_partitions;

        // Make a copy.
        range_lock_.Lock();
        SubRanges *latest = new SubRanges(*ref);
        range_lock_.Unlock();
        std::vector<SubRange> &subranges = latest->subranges;
        double total_inserts = 0;
        double unfair_subranges = 0;

        for (int i = 0; i < subranges.size(); i++) {
            SubRange &sr = subranges[i];
            total_inserts += sr.ninserts;
        }

        uint64_t now = versions_->last_sequence_.load(
                std::memory_order_relaxed);
        RDMA_ASSERT(total_inserts <= now)
            << fmt::format("{},{}", total_inserts, now);

        double most_unfair = 0;
        uint32_t most_unfair_subrange = 0;
        for (int i = 0; i < subranges.size(); i++) {
            SubRange &sr = subranges[i];
            sr.insertion_ratio = (double) sr.ninserts / total_inserts;
            double diff = (sr.insertion_ratio - fair_ratio) * 100.0 / fair_ratio;
            if (std::abs(diff) > SUBRANGE_REORG_DIFF_FROM_FAIR_THRESHOLD &&
                !sr.IsAPoint(user_comparator_)) {
                unfair_subranges += 1;
            }

            bool eligble_for_minor = sr.ninserts > 100 &&
                                     now - last_minor_reorg_seq_ >
                                     SUBRANGE_MINOR_REORG_INTERVAL;
            if (eligble_for_minor) {
                if (MinorReorgDestroyDuplicates(latest, i)) {
                    return;
                }
                if (diff > SUBRANGE_REORG_DIFF_FROM_FAIR_THRESHOLD &&
                    diff > most_unfair) {
                    most_unfair = diff;
                    most_unfair_subrange = i;
                }
            }
        }
        if (unfair_subranges / (double) subranges.size() >
            SUBRANGE_MAJOR_REORG_THRESHOLD &&
            now - last_major_reorg_seq_ >
            SUBRANGE_MAJOR_REORG_INTERVAL) {
            PerformSubrangeMajorReorg(latest, total_inserts);
            return;
        } else if (most_unfair != 0) {
            // Perform minor.
            PerformSubrangeMinorReorg(most_unfair_subrange, latest,
                                      total_inserts);
            return;
        }
        delete latest;
    }

    void DBImpl::PerformCompaction(leveldb::EnvBGThread *bg_thread,
                                   const std::vector<EnvBGTask> &tasks) {
        if (options_.memtable_type == MemTableType::kStaticPartition) {
            bool compacted = CompactMemTableStaticPartition(bg_thread, tasks);
        } else {
            bool compacted = CompactMemTable(bg_thread, tasks);
        }

        if (options_.enable_major) {
            while (true) {
                MutexLock lock(&mutex_);
                if (is_major_compaciton_running_) {
                    return;
                }

                if (versions_->NeedsCompaction() ||
                    versions_->current()->files_[0].size() >
                    config::kL0_StopWritesTrigger) {

                    PerformMajorCompaction(bg_thread, tasks);
                } else {
                    return;
                }
            }
        }
    }

    bool DBImpl::CoordinateMajorCompaction(leveldb::EnvBGThread *bg_thread) {
        // TODO
        // Compute non-overlapping sets.
        // All SSTables in one set are overlapping.
        // SSTables in different sets are non-overlapping.
        // A set contains at least one SSTable from L0.
        // A set represents a range. Its smallest internal key is li and its largest internal key is ri.



        return false;
    }

    bool DBImpl::PerformMajorCompaction(EnvBGThread *bg_thread,
                                        const std::vector<EnvBGTask> &tasks) {
        Compaction *c = versions_->PickCompaction(bg_thread->thread_id());
        Status status;
        if (c == nullptr) {
            // Nothing to do
            return false;
        }
        if (c->IsTrivialMove()) {
            // Move file to next level
            assert(c->level() >= 0);
            assert(c->num_input_files(0) == 1);
            assert(c->num_input_files(1) == 0);
            FileMetaData *f = c->input(0, 0);
            c->edit()->DeleteFile(c->level(), f->memtable_id, f->number);
            c->edit()->AddFile(c->level() + 1,
                               0,
                               f->number, f->file_size,
                               f->converted_file_size,
                               f->flush_timestamp,
                               f->smallest,
                               f->largest,
                               f->meta_block_handle,
                               f->data_block_group_handles);
            Version *new_version = new Version(versions_,
                                               versions_->version_id_seq_.fetch_add(
                                                       1));
            status = versions_->LogAndApply(c->edit(), new_version);
            if (!status.ok()) {
                RecordBackgroundError(status);
            }
            std::string output = fmt::format(
                    "Moved #{}@{} to level-{} {} bytes {}\n",
                    f->number,
                    c->level(), c->level() + 1,
                    f->file_size,
                    status.ToString().c_str());
            Log(options_.info_log, "%s", output.c_str());
            RDMA_LOG(rdmaio::INFO) << output;
        } else {
            is_major_compaciton_running_ = true;
            CompactionState *compact = new CompactionState(c);
            status = DoCompactionWork(compact, bg_thread);
            if (!status.ok()) {
                RecordBackgroundError(status);
            }
            CleanupCompaction(compact);
            versions_->versions_[c->input_version_->version_id()].Unref(
                    dbname_);
            DeleteObsoleteVersions(bg_thread);
            DeleteObsoleteFiles(bg_thread);
            is_major_compaciton_running_ = false;
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("!!!!!!!!!!!!!Compaction complete");

        }
        delete c;
        RDMA_LOG(rdmaio::INFO)
            << fmt::format("New version: {}",
                           versions_->current()->DebugString());
        if (status.ok()) {
        } else {
            Log(options_.info_log, "Compaction error: %s",
                status.ToString().c_str());
        }
        return true;
    }

    void DBImpl::CleanupCompaction(CompactionState *compact) {
        mutex_.AssertHeld();
        if (compact->builder != nullptr) {
            // May happen if we get a shutdown call in the middle of compaction
            compact->builder->Abandon();
            delete compact->builder;
        }

        // Also delete its contained mem file.
        // Delete everything now.
        for (int i = 0; i < compact->output_files.size(); i++) {
            MemWritableFile *out = compact->output_files[i];
            if (out) {
                auto *mem_file = dynamic_cast<NovaCCMemFile *>(out->mem_file());
                delete mem_file;
                delete out;
                mem_file = nullptr;
                out = nullptr;
            }
        }


        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output &out = compact->outputs[i];
            pending_outputs_.erase(out.number);
        }
        delete compact;
    }

    Status DBImpl::OpenCompactionOutputFile(CompactionState *compact,
                                            EnvBGThread *bg_thread) {
        assert(compact != nullptr);
        assert(compact->builder == nullptr);
        uint64_t file_number;
        {
            mutex_.Lock();
            file_number = versions_->NewFileNumber();
            pending_outputs_.insert(file_number);
            CompactionState::Output out;
            out.number = file_number;
            out.smallest.Clear();
            out.largest.Clear();
            compact->outputs.push_back(out);
            mutex_.Unlock();
        }
        // Make the output file
        MemManager *mem_manager = bg_thread->mem_manager();
        std::string filename = TableFileName(dbname_, file_number);
        NovaCCMemFile *cc_file = new NovaCCMemFile(options_.env,
                                                   options_,
                                                   file_number,
                                                   mem_manager,
                                                   bg_thread->dc_client(),
                                                   dbname_,
                                                   bg_thread->thread_id(),
                                                   options_.max_dc_file_size,
                                                   bg_thread->rand_seed(),
                                                   filename);
        compact->outfile = new MemWritableFile(cc_file);
        compact->builder = new TableBuilder(options_, compact->outfile);
        compact->output_files.push_back(compact->outfile);
        return Status::OK();
    }

    Status DBImpl::FinishCompactionOutputFile(CompactionState *compact,
                                              Iterator *input) {
        assert(compact != nullptr);
        assert(compact->outfile != nullptr);
        assert(compact->builder != nullptr);
        assert(!compact->output_files.empty());

        const uint64_t output_number = compact->current_output()->number;
        assert(output_number != 0);

        // Check for iterator errors
        Status s = input->status();
        if (s.ok()) {
            s = compact->builder->Finish();
        } else {
            compact->builder->Abandon();
        }
        const uint64_t current_entries = compact->builder->NumEntries();
        const uint64_t current_data_blocks = compact->builder->NumDataBlocks();
        const uint64_t current_bytes = compact->builder->FileSize();
        compact->current_output()->file_size = current_bytes;
        compact->total_bytes += current_bytes;
        delete compact->builder;
        compact->builder = nullptr;

        FileMetaData meta;
        meta.number = output_number;
        meta.file_size = current_bytes;
        meta.smallest = compact->current_output()->smallest;
        meta.largest = compact->current_output()->largest;
        // Set meta in order to flush to the corresponding DC node.
        NovaCCMemFile *mem_file = static_cast<NovaCCMemFile *>(compact->outfile->mem_file());
        mem_file->set_meta(meta);
        mem_file->set_num_data_blocks(current_data_blocks);

        // Finish and check for file errors
        RDMA_ASSERT(s.ok());
        s = compact->outfile->Sync();
        s = compact->outfile->Close();

        mem_file->WaitForPersistingDataBlocks();
        return s;
    }

    Status DBImpl::InstallCompactionResults(CompactionState *compact,
                                            uint32_t thread_id) {
        // Now finalize all SSTables.
        for (int i = 0; i < compact->output_files.size(); i++) {
            CompactionState::Output &output = compact->outputs[i];
            MemWritableFile *out = compact->output_files[i];
            auto *mem_file = dynamic_cast<NovaCCMemFile *>(out->mem_file());
            output.converted_file_size = mem_file->Finalize();
            output.meta_block_handle = mem_file->meta_block_handle();
            output.data_block_group_handles = mem_file->rhs();

            delete mem_file;
            delete out;
            compact->output_files[i] = nullptr;
            mem_file = nullptr;
            out = nullptr;
        }

        // Add compaction outputs
        compact->compaction->AddInputDeletions(compact->compaction->edit());
        const int src_level = compact->compaction->level();
        const int dest_level = compact->compaction->level() + 1;
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output &out = compact->outputs[i];
            compact->compaction->edit()->AddFile(dest_level,
                                                 0,
                                                 out.number,
                                                 out.file_size,
                                                 out.converted_file_size,
                                                 versions_->last_sequence_,
                                                 out.smallest, out.largest,
                                                 out.meta_block_handle,
                                                 out.data_block_group_handles);
        }

        versions_->AppendChangesToManifest(compact->compaction->edit(),
                                           manifest_file_,
                                           options_.manifest_stoc_id);
        Version *new_version = new Version(versions_,
                                           versions_->version_id_seq_.fetch_add(
                                                   1));
        std::string output = fmt::format(
                "bg[{}]: Major Compacted {}@{} + {}@{} files => {} bytes",
                thread_id,
                compact->compaction->num_input_files(0),
                src_level,
                compact->compaction->num_input_files(1),
                dest_level,
                compact->total_bytes);
        Log(options_.info_log, "%s", output.c_str());
        RDMA_LOG(rdmaio::INFO) << output;

        mutex_.Lock();
        return versions_->LogAndApply(compact->compaction->edit(), new_version);
    }

    Status
    DBImpl::DoCompactionWork(CompactionState *compact, EnvBGThread *bg_thread) {
        const uint64_t start_micros = env_->NowMicros();

        std::string output = fmt::format(
                "bg[{}] Major Compacting {}@{} + {}@{} files",
                bg_thread->thread_id(),
                compact->compaction->num_input_files(0),
                compact->compaction->level(),
                compact->compaction->num_input_files(1),
                compact->compaction->level() + 1);
        Log(options_.info_log, "%s", output.c_str());
        RDMA_LOG(rdmaio::INFO) << output;

        int src_level = compact->compaction->level();
        assert(versions_->NumLevelFiles(src_level) > 0);
        assert(compact->builder == nullptr);
        assert(compact->outfile == nullptr);
        assert(compact->outputs.empty());

        if (snapshots_.empty()) {
            compact->smallest_snapshot = versions_->LastSequence();
        } else {
            compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
        }
        // Release mutex while we're actually doing the compaction work
        mutex_.Unlock();

        Iterator *input = versions_->MakeInputIterator(compact->compaction,
                                                       bg_thread);
        input->SeekToFirst();
        Status status;
        ParsedInternalKey ikey;
        std::string current_user_key;
        bool has_current_user_key = false;
        SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
        while (input->Valid() &&
               !shutting_down_.load(std::memory_order_acquire)) {
            Slice key = input->key();
            if (compact->compaction->ShouldStopBefore(key) &&
                compact->builder != nullptr) {
                status = FinishCompactionOutputFile(compact, input);
                if (!status.ok()) {
                    break;
                }
            }

            // Handle key/value, add to state, etc.
            bool drop = false;
            if (!ParseInternalKey(key, &ikey)) {
                // Do not hide error keys
                current_user_key.clear();
                has_current_user_key = false;
                last_sequence_for_key = kMaxSequenceNumber;
            } else {
                if (!has_current_user_key ||
                    user_comparator()->Compare(ikey.user_key,
                                               Slice(current_user_key)) !=
                    0) {
                    // First occurrence of this user key
                    current_user_key.assign(ikey.user_key.data(),
                                            ikey.user_key.size());
                    has_current_user_key = true;
                    last_sequence_for_key = kMaxSequenceNumber;
                }

                if (last_sequence_for_key <= compact->smallest_snapshot) {
                    // Hidden by an newer entry for same user key
                    drop = true;  // (A)
                } else if (ikey.type == kTypeDeletion &&
                           ikey.sequence <= compact->smallest_snapshot &&
                           compact->compaction->IsBaseLevelForKey(
                                   ikey.user_key)) {
                    // For this user key:
                    // (1) there is no data in higher levels
                    // (2) data in lower levels will have larger sequence numbers
                    // (3) data in layers that are being compacted here and have
                    //     smaller sequence numbers will be dropped in the next
                    //     few iterations of this loop (by rule (A) above).
                    // Therefore this deletion marker is obsolete and can be dropped.
                    drop = true;
                }
                last_sequence_for_key = ikey.sequence;
            }
#if 0
            Log(options_.info_log,
                "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
                "%d smallest_snapshot: %d",
                ikey.user_key.ToString().c_str(),
                (int)ikey.sequence, ikey.type, kTypeValue, drop,
                compact->compaction->IsBaseLevelForKey(ikey.user_key),
                (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

            if (!drop) {
                // Open output file if necessary
                if (compact->builder == nullptr) {
                    status = OpenCompactionOutputFile(compact, bg_thread);
                    if (!status.ok()) {
                        break;
                    }
                }
                if (compact->builder->NumEntries() == 0) {
                    compact->current_output()->smallest.DecodeFrom(key);
                }
                compact->current_output()->largest.DecodeFrom(key);
                compact->builder->Add(key, input->value());

                // Close output file if it is big enough
                if (compact->builder->FileSize() >=
                    compact->compaction->MaxOutputFileSize()) {
                    status = FinishCompactionOutputFile(compact, input);
                    if (!status.ok()) {
                        break;
                    }
                }
            }
            input->Next();
        }

        if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
            status = Status::IOError("Deleting DB during compaction");
        }
        if (status.ok() && compact->builder != nullptr) {
            status = FinishCompactionOutputFile(compact, input);
        }
        if (status.ok()) {
            status = input->status();
        }
        delete input;
        input = nullptr;

        CompactionStats stats;
        stats.micros = env_->NowMicros() - start_micros;
        for (int which = 0; which < 2; which++) {
            for (int i = 0;
                 i < compact->compaction->num_input_files(which); i++) {
                stats.bytes_read += compact->compaction->input(which,
                                                               i)->file_size;
                stats.num_input_files += 1;
            }
        }
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            stats.bytes_written += compact->outputs[i].file_size;
            stats.num_output_files += 1;
        }

        if (status.ok()) {
            status = InstallCompactionResults(compact, bg_thread->thread_id());
        }
        if (!status.ok()) {
            RecordBackgroundError(status);
        }
//        stats_[compact->compaction->level() + 1].Add(stats);

        Log(options_.info_log, "%s",
            fmt::format("Major compaction stats,{},{},{},{},{}",
                        stats.num_input_files, stats.bytes_read,
                        stats.num_output_files, stats.bytes_written,
                        stats.micros).c_str());

        versions_->AddCompactedInputs(compact->compaction, &compacted_tables_);
        return status;
    }

    namespace {
        struct IterState {
            port::Mutex *const mu;
            Version *const version
            GUARDED_BY(mu);
            MemTable *const mem
            GUARDED_BY(mu);
            MemTable *const imm
            GUARDED_BY(mu);

            IterState(port::Mutex *mutex, MemTable *mem, MemTable *imm,
                      Version *version)
                    : mu(mutex), version(version), mem(mem), imm(imm) {}
        };

        static void CleanupIteratorState(void *arg1, void *arg2) {
            IterState *state = reinterpret_cast<IterState *>(arg1);
            state->mu->Lock();
            state->mem->Unref();
            if (state->imm != nullptr) state->imm->Unref();
            state->version->Unref();
            state->mu->Unlock();
            delete state;
        }
    }  // anonymous namespace

    Iterator *DBImpl::NewInternalIterator(const ReadOptions &options,
                                          SequenceNumber *latest_snapshot,
                                          uint32_t *seed) {
        return nullptr;
//        mutex_.Lock();
//        *latest_snapshot = versions_->LastSequence();
//
//        // Collect together all needed child iterators
//        std::vector<Iterator *> list;
//        list.push_back(mem_->NewIterator(TraceType::MEMTABLE,
//                                         AccessCaller::kUserIterator));
//        mem_->Ref();

//
//        if (imm_ != nullptr) {
//            list.push_back(imm_->NewIterator(TraceType::IMMUTABLE_MEMTABLE,
//                                             AccessCaller::kUserIterator));
//            imm_->Ref();
//        }
//        versions_->current()->AddIterators(options, &list);
//        Iterator *internal_iter =
//                NewMergingIterator(&internal_comparator_, &list[0],
//                                   list.size());
//        versions_->current()->Ref();
//
//        IterState *cleanup = new IterState(&mutex_, mem_, imm_,
//                                           versions_->current());
//        internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

//        *seed = ++seed_;
//        mutex_.Unlock();
//        return internal_iter;
    }

    Iterator *DBImpl::TEST_NewInternalIterator() {
        SequenceNumber ignored;
        uint32_t ignored_seed;
        return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
    }

    int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
        MutexLock l(&mutex_);
        return versions_->MaxNextLevelOverlappingBytes();
    }

    Status DBImpl::Get(const ReadOptions &options, const Slice &key,
                       std::string *value) {
        number_of_gets_ += 1;
        Status s = Status::OK();
        std::string tmp;
        SequenceNumber snapshot = kMaxSequenceNumber;
        MemTable *memtable = nullptr;
        uint64_t l0_file_number = 0;
        if (table_locator_ != nullptr) {
            uint32_t memtableid = table_locator_->Lookup(key, options.hash);
            if (memtableid != 0) {
                RDMA_ASSERT(memtableid < MAX_LIVE_MEMTABLES) << memtableid;
                memtable = versions_->mid_table_mapping_[memtableid].Ref(
                        &l0_file_number);
            }
            RDMA_ASSERT(memtable != nullptr || l0_file_number != 0)
                << options.hash;
            LookupKey lkey(key, snapshot);
            if (memtable != nullptr) {
                number_of_memtable_hits_ += 1;
                RDMA_ASSERT(memtable->memtableid() == memtableid);
                RDMA_ASSERT(memtable->Get(lkey, value, &s))
                    << fmt::format("key:{} memtable:{} s:{}",
                                   key.ToString(),
                                   memtable->memtableid(), s.ToString());
                versions_->mid_table_mapping_[memtableid].Unref(dbname_);
            } else if (l0_file_number != 0) {
                Version *current = nullptr;
                uint32_t vid = 0;
                while (current == nullptr) {
                    vid = versions_->current_version_id();
                    RDMA_ASSERT(vid < MAX_LIVE_MEMTABLES) << vid;
                    current = versions_->versions_[vid].Ref();
                }
                RDMA_ASSERT(current->version_id() == vid);
                s = current->Get(options, l0_file_number, lkey, value);
                versions_->versions_[vid].Unref(dbname_);
            }
            return s;
        }
        return s;
    }

    void DBImpl::StartTracing() {
        if (db_profiler_) {
            db_profiler_->StartTracing();
        }
    }

    Iterator *DBImpl::NewIterator(const ReadOptions &options) {
        SequenceNumber latest_snapshot;
        uint32_t seed;
        Iterator *iter = NewInternalIterator(options, &latest_snapshot, &seed);
        return NewDBIterator(this, user_comparator(), iter,
                             (options.snapshot != nullptr
                              ? static_cast<const SnapshotImpl *>(options.snapshot)
                                      ->sequence_number()
                              : latest_snapshot),
                             seed);
    }

    void DBImpl::RecordReadSample(Slice key) {
//        MutexLock l(&mutex_);
//        if (versions_->current()->RecordReadSample(key)) {
//            MaybeScheduleCompaction();
//        }
    }

    const Snapshot *DBImpl::GetSnapshot() {
        MutexLock l(&mutex_);
        return snapshots_.New(versions_->LastSequence());
    }

    void DBImpl::ReleaseSnapshot(const Snapshot *snapshot) {
        MutexLock l(&mutex_);
        snapshots_.Delete(static_cast<const SnapshotImpl *>(snapshot));
    }

// Convenience methods
    Status
    DBImpl::Put(const WriteOptions &o, const Slice &key, const Slice &val) {
        processed_writes_ += 1;
        if (options_.memtable_type == MemTableType::kStaticPartition) {
            if (options_.enable_subranges && o.update_subranges) {
                // Update subranges is false during loading the database.
                return WriteSubrange(o, key, val);
            } else {
                return WriteStaticPartition(o, key, val);
            }
        }
        return Write(o, key, val);
    }

    Status DBImpl::Delete(const WriteOptions &options, const Slice &key) {
        return DB::Delete(options, key);
    }

    Status DBImpl::GenerateLogRecords(const leveldb::WriteOptions &options,
                                      leveldb::WriteBatch *updates) {
//        mutex_.Lock();
//        std::string logfile = current_log_file_name_;
//        std::list<std::string> closed_files(closed_log_files_.begin(),
//                                            closed_log_files_.end());
//        closed_log_files_.clear();
//        mutex_.Unlock();
//        // Synchronous replication.
//        uint32_t server_id = 0;
//        uint32_t dbid = 0;
//        nova::ParseDBIndexFromFile(current_log_file_name_, &server_id, &dbid);
//
//        auto dc = reinterpret_cast<leveldb::NovaBlockCCClient *>(options.dc_client);
//        RDMA_ASSERT(dc);
//        dc->set_dbid(dbid);
//        options.dc_client->InitiateReplicateLogRecords(
//                logfile, options.thread_id,
//                WriteBatchInternal::Contents(updates));
//
//        for (const auto &file : closed_files) {
//            options.dc_client->InitiateCloseLogFile(file);
//        }
        return Status::OK();
    }

    void DBImpl::StealMemTable(const leveldb::WriteOptions &options) {
        uint32_t number_of_tries = std::min((size_t) 3, dbs_.size() - 1);
        uint32_t rand_range_index = rand_r(options.rand_seed) % dbs_.size();

        for (int i = 0; i < number_of_tries; i++) {
            // steal a memtable from another range.
            rand_range_index = (rand_range_index + 1) % dbs_.size();
            if (rand_range_index == dbid_) {
                rand_range_index = (rand_range_index + 1) % dbs_.size();
            }

            auto steal_from_range = reinterpret_cast<DBImpl *>(dbs_[rand_range_index]);
            if (!steal_from_range->range_lock_.TryLock()) {
                continue;
            }
            bool steal_success = false;
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "db[{}]: Try Steal from range {} {}",
                        dbid_,
                        steal_from_range->dbid_,
                        steal_from_range->active_memtables_.size());

            double expected_share =
                    (double) steal_from_range->processed_writes_ /
                    (double) options.total_writes;
            double actual_share =
                    (double) (steal_from_range->number_of_active_memtables_ +
                              steal_from_range->number_of_immutable_memtables_)
                    /
                    (double) nova::NovaConfig::config->num_memtables;

            if (steal_from_range->active_memtables_.size() > 1 &&
                processed_writes_ >=
                (double) (steal_from_range->processed_writes_) * 1.1 &&
                actual_share > expected_share) {
                uint32_t memtable_index = rand_r(options.rand_seed) %
                                          steal_from_range->active_memtables_.size();

                AtomicMemTable *steal_table = steal_from_range->active_memtables_[memtable_index];
                if (steal_table->mutex_.try_lock()) {
                    if (steal_table->number_of_pending_writes_ == 0 &&
                        !steal_table->is_immutable_ &&
                        steal_table->memtable_size_ >
                        0.5 * options_.write_buffer_size) {
                        number_of_steals_ += 1;
                        steal_table->is_immutable_ = true;
                        RDMA_LOG(rdmaio::DEBUG)
                            << fmt::format(
                                    "db[{}]: Steal memtable {} from range {}",
                                    dbid_,
                                    steal_table->memtable_->memtableid(),
                                    steal_from_range->dbid_);
                        steal_from_range->active_memtables_.erase(
                                steal_from_range->active_memtables_.begin() +
                                memtable_index);

                        steal_from_range->number_of_active_memtables_ -= 1;
                        steal_from_range->number_of_immutable_memtables_ += 1;

                        steal_from_range->MaybeScheduleCompaction(
                                options.thread_id, steal_table->memtable_, 0, 0,
                                options.rand_seed);
                        steal_success = true;
                    }
                    steal_table->mutex_.unlock();
                }
            }
            steal_from_range->range_lock_.Unlock();
            if (steal_success) {
                break;
            }
        }
    }

    bool DBImpl::WriteStaticPartition(const leveldb::WriteOptions &options,
                                      const leveldb::Slice &key,
                                      const leveldb::Slice &value,
                                      uint32_t partition_id,
                                      bool should_wait,
                                      uint64_t last_sequence,
                                      SubRange *subrange) {
        MemTablePartition *partition = partitioned_active_memtables_[partition_id];
        partition->mutex.Lock();
        if (subrange != nullptr) {
            subrange->ninserts += 1;
        }

        MemTable *table = nullptr;
        bool wait = false;
        bool schedule_compaction = false;
        int next_imm_slot = -1;
        while (true) {
            table = partition->memtable;
            next_imm_slot = -1;
            if (table->ApproximateMemoryUsage() <= options_.write_buffer_size) {
                break;
            } else {
                // The table is full.
                if (!partition->available_slots.empty()) {
                    next_imm_slot = partition->available_slots.front();
                    partition->available_slots.pop();
                }
                if (next_imm_slot == -1) {
                    // We have filled up all memtables, but the previous
                    // one is still being compacted, so we wait.
                    if (!should_wait) {
                        partition->mutex.Unlock();
                        return false;
                    }

                    Log(options_.info_log,
                        "Current memtable full; Make room waiting... pid-%u-tid-%lu\n",
                        partition_id, options.thread_id);
                    // Try a different table.
                    partition->background_work_finished_signal_.Wait();
                    if (!wait) {
                        number_of_puts_wait_ += 1;
                    }
                    wait = true;
                } else {
                    // Create a new table.
                    RDMA_ASSERT(partitioned_imms_[next_imm_slot] == 0);
                    partitioned_imms_[next_imm_slot] = table->memtableid();
                    uint32_t memtable_id = memtable_id_seq_.fetch_add(1);
                    partition->closed_log_files.push_back(table->memtableid());
                    table = new MemTable(internal_comparator_, memtable_id,
                                         db_profiler_);
                    RDMA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                    versions_->mid_table_mapping_[memtable_id].SetMemTable(
                            table);
                    partition->memtable = table;
                    schedule_compaction = true;
                    number_of_immutable_memtables_.fetch_add(1);// += 1;
                    break;
                }
            }
        }

        if (!wait) {
            number_of_puts_no_wait_ += 1;
        }
        if (wait) {
            Log(options_.info_log,
                "Make room; resuming... pid-%u-tid-%lu\n",
                partition_id, options.thread_id);
        }
        uint32_t memtable_id = table->memtableid();
        table->Add(last_sequence, ValueType::kTypeValue, key, value);
        versions_->mid_table_mapping_[memtable_id].nentries_ += 1;
        if (table_locator_ != nullptr) {
            table_locator_->Insert(key, options.hash, table->memtableid());
        }
        partition->mutex.Unlock();

        if (schedule_compaction) {
            MaybeScheduleCompaction(options.thread_id + 1000,
                                    versions_->mid_table_mapping_[partitioned_imms_[next_imm_slot]].memtable_,
                                    partition_id,
                                    next_imm_slot, options.rand_seed);
        }
        return true;
    }

    Status DBImpl::WriteStaticPartition(const WriteOptions &options,
                                        const Slice &key,
                                        const Slice &val) {
        uint64_t last_sequence = versions_->last_sequence_.fetch_add(1);
        uint32_t partition_id =
                rand_r(options.rand_seed) %
                partitioned_active_memtables_.size();
        if (options_.num_memtable_partitions > 1) {
            int tries = 2;
            int i = 0;
            while (i < tries) {
                if (WriteStaticPartition(options, key, val, partition_id, false,
                                         last_sequence, nullptr)) {
                    return Status::OK();
                }
                i++;
                partition_id = (partition_id + 1) %
                               partitioned_active_memtables_.size();
            }
        }
        partition_id =
                (partition_id + 1) % partitioned_active_memtables_.size();
        RDMA_ASSERT(
                WriteStaticPartition(options, key, val, partition_id, true,
                                     last_sequence, nullptr));
        return Status::OK();
    }

    bool DBImpl::BinarySearch(leveldb::DBImpl::SubRanges *ref,
                              const leveldb::Slice &key, int *subrange_id) {
        int l = 0, r = ref->subranges.size() - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;
            SubRange &subrange = ref->subranges[m];

            if (subrange.IsSmallerThanLower(key, user_comparator_)) {
                r = m - 1;
            } else if (subrange.IsGreaterThanUpper(key, user_comparator_)) {
                l = m + 1;
            } else {
                *subrange_id = m;
                return true;
            }
        }
        // if we reach here, then element was
        // not present
        if (ref->subranges.size() > 0) {
            if (l == ref->subranges.size()) {
                l--;
            }
            RDMA_ASSERT(l < ref->subranges.size()) << "";
            if (ref->subranges[l].IsGreaterThanUpper(key, user_comparator_)) {
                RDMA_ASSERT(l == ref->subranges.size() - 1);
            } else {
                RDMA_ASSERT(ref->subranges[l].IsSmallerThanLower(key,
                                                                 user_comparator_));
            }
        }
        *subrange_id = l;
        return false;
    }

    bool
    DBImpl::BinarySearchWithDuplicate(SubRanges *ref, const leveldb::Slice &key,
                                      unsigned int *rand_seed,
                                      int *subrange_id) {
        bool found = BinarySearch(ref, key, subrange_id);
        if (!found) {
            return false;
        }

        RDMA_ASSERT(*subrange_id >= 0);
        SubRange &sr = ref->subranges[*subrange_id];
        if (sr.num_duplicates == 0) {
            return true;
        }

        int i = (*subrange_id) - 1;
        while (i >= 0) {
            if (ref->subranges[i].Equals(sr, user_comparator_)) {
                i--;
            } else {
                break;
            }
        }
        *subrange_id = i + 1 +
                       rand_r(rand_seed) % sr.num_duplicates;
        sr = ref->subranges[*subrange_id];
        RDMA_ASSERT(!sr.IsSmallerThanLower(key, user_comparator_) &&
                    !sr.IsGreaterThanUpper(key, user_comparator_));
        return true;
    }


    Status DBImpl::WriteSubrange(const leveldb::WriteOptions &options,
                                 const leveldb::Slice &key,
                                 const leveldb::Slice &val) {
        uint64_t last_sequence = versions_->last_sequence_.fetch_add(1);

        if (processed_writes_ > SUBRANGE_WARMUP_NPUTS &&
            processed_writes_ % SUBRANGE_REORG_INTERVAL == 0) {
            // wake up reorg thread.
            EnvBGTask task = {};
            task.db = this;
            reorg_thread_->Schedule(task);
        }

        SubRanges *ref = latest_subranges_;
//        ref->AssertSubrangeBoundary(user_comparator_);
        int subrange_id = -1;
        if (ref->subranges.size() == options_.num_memtable_partitions) {
            // steady state.
            bool found = BinarySearchWithDuplicate(ref, key,
                                                   options.rand_seed,
                                                   &subrange_id);
            if (found) {
                RDMA_ASSERT(subrange_id >= 0);
                RDMA_ASSERT(WriteStaticPartition(options, key, val, subrange_id,
                                                 true,
                                                 last_sequence,
                                                 &ref->subranges[subrange_id]));
                return Status::OK();
            }
        }

        // Require updating boundary of subranges.
        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("Put key {} {}", key.ToString(), ref->DebugString());

        range_lock_.Lock();
        while (true) {
            ref = latest_subranges_;
            bool found = BinarySearchWithDuplicate(ref, key,
                                                   options.rand_seed,
                                                   &subrange_id);
            if (found &&
                ref->subranges.size() == options_.num_memtable_partitions) {
                break;
            }

            if (found &&
                ref->subranges.size() < options_.num_memtable_partitions) {
                SubRange &sr = ref->subranges[subrange_id];
                if (sr.lower_inclusive &&
                    user_comparator_->Compare(sr.lower, key) == 0) {
                    // Cannot split this subrange since it will create a new subrange with no keys.
                    break;
                }

                // find the key but not enough subranges. Split this subrange.
                SubRange new_sr = {}; // [k, sr.upper]
                new_sr.lower = SubRange::Copy(key);
                new_sr.upper = SubRange::Copy(sr.upper);
                new_sr.lower_inclusive = true;
                new_sr.upper_inclusive = sr.upper_inclusive;
                // [lower, k)
                delete sr.upper.data();
                sr.upper = SubRange::Copy(new_sr.lower);
                sr.upper_inclusive = false;

                // update stats.
                sr.ninserts /= 2;
                new_sr.ninserts = sr.ninserts;

                // insert new subrange.
                subrange_id += 1;
                ref->subranges.insert(ref->subranges.begin() + subrange_id,
                                      new_sr);
                break;
            }

            if (!found &&
                ref->subranges.size() == options_.num_memtable_partitions) {
                // didn't find the key but we have enough subranges.
                // Extend the lower boundary of the next subrange to include this key.
                if (ref->subranges[subrange_id].IsSmallerThanLower(key,
                                                                   user_comparator_)) {
                    SubRange &sr = ref->subranges[subrange_id];
                    sr.lower = SubRange::Copy(key);
                    sr.lower_inclusive = true;
                    if (sr.num_duplicates > 0) {
                        int i = subrange_id - 1;
                        while (i >= 0) {
                            if (ref->subranges[i].Equals(sr,
                                                         user_comparator_)) {
                                i--;
                            } else {
                                break;
                            }
                        }

                        int start = i + 1;
                        for (int i = 0; i < sr.num_duplicates; i++) {
                            SubRange &dup = ref->subranges[start + i];
                            dup.lower = SubRange::Copy(key);
                            dup.lower_inclusive = true;
                        }
                    }
                } else {
                    RDMA_ASSERT(
                            ref->subranges[subrange_id].IsGreaterThanUpper(key,
                                                                           user_comparator_));
                    SubRange &sr = ref->subranges[subrange_id];
                    sr.upper = SubRange::Copy(key);
                    sr.upper_inclusive = true;
                    if (sr.num_duplicates > 0) {
                        int i = subrange_id - 1;
                        while (i >= 0) {
                            if (ref->subranges[i].Equals(sr,
                                                         user_comparator_)) {
                                i--;
                            } else {
                                break;
                            }
                        }

                        int start = i + 1;
                        for (int i = 0; i < sr.num_duplicates; i++) {
                            SubRange &dup = ref->subranges[start + i];
                            dup.upper = SubRange::Copy(key);
                            dup.upper_inclusive = true;
                        }
                    }
                }
                break;
            }

            // not found and not enough subranges.
            // no subranges. construct a point.
            if (ref->subranges.empty()) {
                SubRange sr = {};
                sr.lower = SubRange::Copy(key);
                sr.upper = SubRange::Copy(key);

                sr.lower_inclusive = true;
                sr.upper_inclusive = true;
                ref->subranges.push_back(sr);
                subrange_id = 0;
                break;
            }

            if (ref->subranges.size() == 1 &&
                ref->subranges[0].IsAPoint(user_comparator_)) {
                if (ref->subranges[0].IsSmallerThanLower(key,
                                                         user_comparator_)) {
                    ref->subranges[0].lower_inclusive = true;
                    ref->subranges[0].lower = SubRange::Copy(key);
                } else {
                    ref->subranges[0].upper_inclusive = true;
                    ref->subranges[0].upper = SubRange::Copy(key);
                }
                subrange_id = 0;
                break;
            }

            // key is less than the smallest user key.
            if (ref->subranges[subrange_id].IsSmallerThanLower(key,
                                                               user_comparator_)) {
                SubRange sr = {};
                sr.lower = SubRange::Copy(key);
                sr.upper = SubRange::Copy(ref->subranges[subrange_id].lower);
                sr.lower_inclusive = true;
                sr.upper_inclusive = false;

                ref->subranges.insert(ref->subranges.begin(), sr);
                break;
            }

            // key is greater than the largest user key.
            if (ref->subranges[subrange_id].IsGreaterThanUpper(key,
                                                               user_comparator_)) {
                SubRange sr = {};
                sr.lower = SubRange::Copy(ref->subranges[subrange_id].upper);
                sr.upper = SubRange::Copy(key);
                sr.lower_inclusive = false;
                sr.upper_inclusive = true;
                ref->subranges.push_back(sr);
                subrange_id = ref->subranges.size() - 1;
                break;
            }
            RDMA_ASSERT(false);
        }
        range_lock_.Unlock();

        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("Expand subranges at {} for key {} ",
                           subrange_id, key.ToString());

        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("Expand subranges at {} for key {} subranges:{}",
                           subrange_id, key.ToString(), ref->DebugString());

        RDMA_ASSERT(subrange_id != -1);
        RDMA_ASSERT(subrange_id < ref->subranges.size());
        RDMA_ASSERT(WriteStaticPartition(options, key, val, subrange_id, true,
                                         last_sequence,
                                         &ref->subranges[subrange_id]));
        return Status::OK();
    }

    Status DBImpl::Write(const WriteOptions &options, const Slice &key,
                         const Slice &val) {
        // TODO: Support tracing past accesses.
        uint64_t last_sequence = versions_->last_sequence_.fetch_add(1);

        std::vector<MemTable *> full_memtables;
        AtomicMemTable *atomic_memtable = nullptr;
        bool wait = false;
        bool all_busy = true;
        bool enable_stickness = true;

        range_lock_.Lock();
        int expected_share =
                round(((double) processed_writes_ /
                       (double) options.total_writes) *
                      nova::NovaConfig::config->num_memtables);
        int actual_share = 0;
        AtomicMemTable *emptiest_memtable = nullptr;
        uint64_t smallest_size = UINT64_MAX;
        int emptiest_index = 0;

        uint32_t atomic_memtable_index = 0;
        while (true) {
            emptiest_memtable = nullptr;
            smallest_size = UINT64_MAX;
            emptiest_index = -1;
            all_busy = true;
            atomic_memtable = nullptr;
            enable_stickness = active_memtables_.size() < 5;

            int number_of_retries = std::min((size_t) 3,
                                             active_memtables_.size());
            RDMA_ASSERT(full_memtables.empty());
            atomic_memtable_index = 0;
            if (!enable_stickness) {
                atomic_memtable_index = rand_r(options.rand_seed);
            }

            uint32_t first_memtable_id = 0;
            if (!active_memtables_.empty()) {
                first_memtable_id = active_memtables_[0]->memtable_->memtableid();
            }
            for (int i = 0; i < number_of_retries; i++) {
                atomic_memtable_index =
                        (atomic_memtable_index + 1) % active_memtables_.size();
                if (i == 1 && enable_stickness) {
                    atomic_memtable_index = rand_r(options.rand_seed) %
                                            active_memtables_.size();
                    if (active_memtables_[atomic_memtable_index]->memtable_->memtableid() ==
                        first_memtable_id) {
                        atomic_memtable_index =
                                (atomic_memtable_index + 1) %
                                active_memtables_.size();
                    }
                }

                RDMA_ASSERT(atomic_memtable_index < active_memtables_.size())
                    << fmt::format("{} {} {} {}", atomic_memtable_index,
                                   active_memtables_.size(),
                                   i, number_of_retries);

                atomic_memtable = active_memtables_[atomic_memtable_index];
                RDMA_ASSERT(atomic_memtable);

                uint64_t ms = atomic_memtable->nentries_;
                if (ms < smallest_size &&
                    !atomic_memtable->is_immutable_) {
                    emptiest_memtable = atomic_memtable;
                    smallest_size = ms;
                    emptiest_index = atomic_memtable_index;
                }

                if (!atomic_memtable->mutex_.try_lock()) {
                    atomic_memtable = nullptr;
                    continue;
                }
                all_busy = false;
                RDMA_ASSERT(atomic_memtable->memtable_);
                RDMA_ASSERT(!atomic_memtable->is_flushed_);
                if (atomic_memtable->memtable_size_ >
                    options_.write_buffer_size ||
                    atomic_memtable->is_immutable_) {
                    atomic_memtable->is_immutable_ = true;

                    if (emptiest_index == atomic_memtable_index) {
                        smallest_size = UINT64_MAX;
                        emptiest_index = -1;
                        emptiest_memtable = nullptr;
                    }

                    if (atomic_memtable->number_of_pending_writes_ == 0) {
                        full_memtables.push_back(atomic_memtable->memtable_);
                        closed_memtable_log_files_.push_back(
                                atomic_memtable->memtable_->memtableid());
                        active_memtables_.erase(
                                active_memtables_.begin() +
                                atomic_memtable_index);

                        number_of_active_memtables_ -= 1;
                        number_of_immutable_memtables_ += 1;

                        if (atomic_memtable_index < emptiest_index) {
                            emptiest_index -= 1;
                        }
                    }
                    atomic_memtable->mutex_.unlock();
                    atomic_memtable = nullptr;
                    continue;
                }
                break;
            }

            if (atomic_memtable) {
                RDMA_ASSERT(atomic_memtable->memtable_->memtableid() ==
                            active_memtables_[atomic_memtable_index]->memtable_->memtableid());
                number_of_puts_no_wait_ += 1;
                range_lock_.Unlock();
                break;
            }

            actual_share = number_of_active_memtables_ +
                           number_of_immutable_memtables_;

            if (all_busy && actual_share >= expected_share &&
                !active_memtables_.empty()) {
                number_of_wait_due_to_contention_ += 1;
                // wait on another random table.
                atomic_memtable_index =
                        (atomic_memtable_index + 1) % active_memtables_.size();
                atomic_memtable = active_memtables_[atomic_memtable_index];
                RDMA_ASSERT(atomic_memtable);

                atomic_memtable->mutex_.lock();
                RDMA_ASSERT(atomic_memtable->memtable_);
                RDMA_ASSERT(!atomic_memtable->is_flushed_);

                if (atomic_memtable->memtable_size_ >
                    options_.write_buffer_size ||
                    atomic_memtable->is_immutable_) {
                    atomic_memtable->is_immutable_ = true;

                    if (emptiest_index == atomic_memtable_index) {
                        smallest_size = UINT64_MAX;
                        emptiest_index = -1;
                        emptiest_memtable = nullptr;
                    }

                    if (atomic_memtable->number_of_pending_writes_ == 0) {
                        full_memtables.push_back(atomic_memtable->memtable_);
                        closed_memtable_log_files_.push_back(
                                atomic_memtable->memtable_->memtableid());
                        active_memtables_.erase(
                                active_memtables_.begin() +
                                atomic_memtable_index);

                        number_of_active_memtables_ -= 1;
                        number_of_immutable_memtables_ += 1;

                        if (atomic_memtable_index < emptiest_index) {
                            emptiest_index -= 1;
                        }
                    }
                    atomic_memtable->mutex_.unlock();
                    atomic_memtable = nullptr;
                }
            }

            if (atomic_memtable) {
                RDMA_ASSERT(atomic_memtable->memtable_->memtableid() ==
                            active_memtables_[atomic_memtable_index]->memtable_->memtableid());
                range_lock_.Unlock();
                break;
            }

            // is full.
            bool has_available_memtable = false;
            bool pin = false;

            RDMA_ASSERT(number_of_available_pinned_memtables_ >= 0);
            if (number_of_available_pinned_memtables_ > 0) {
                has_available_memtable = true;
                pin = true;
                number_of_available_pinned_memtables_--;
            } else {
                if (actual_share < expected_share) {
                    options_.memtable_pool->mutex_.lock();
                    if (options_.memtable_pool->num_available_memtables_ > 0) {
                        has_available_memtable = true;
                        options_.memtable_pool->num_available_memtables_ -= 1;
                    }
                    options_.memtable_pool->mutex_.unlock();
                }
            }

            if (has_available_memtable) {
                number_of_active_memtables_ += 1;
                uint32_t memtable_id = memtable_id_seq_.fetch_add(1);
                MemTable *new_table = new MemTable(internal_comparator_,
                                                   memtable_id,
                                                   db_profiler_);
                if (pin) {
                    new_table->is_pinned_ = true;
                }
                RDMA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                versions_->mid_table_mapping_[memtable_id].SetMemTable(
                        new_table);
                atomic_memtable = &versions_->mid_table_mapping_[memtable_id];
                active_memtables_.push_back(atomic_memtable);

                atomic_memtable->mutex_.lock();
                range_lock_.Unlock();
                break;
            } else {
//                if (nova::NovaConfig::config->num_memtables >
//                    2 * dbs_.size()) {
//                    StealMemTable(options);
//                }
                if (emptiest_memtable) {
                    RDMA_ASSERT(emptiest_index >= 0);
                    atomic_memtable = emptiest_memtable;

                    atomic_memtable->mutex_.lock();
                    RDMA_ASSERT(atomic_memtable->memtable_);
                    RDMA_ASSERT(!atomic_memtable->is_flushed_);
                    RDMA_ASSERT(!active_memtables_.empty());
                    RDMA_ASSERT(emptiest_index < active_memtables_.size())
                        << fmt::format("{} {} {}",
                                       atomic_memtable->memtable_->memtableid(),
                                       emptiest_index,
                                       active_memtables_.size());

                    if (atomic_memtable->memtable_size_ >
                        options_.write_buffer_size ||
                        atomic_memtable->is_immutable_) {
                        atomic_memtable->is_immutable_ = true;

                        if (atomic_memtable->number_of_pending_writes_ == 0) {
                            full_memtables.push_back(
                                    atomic_memtable->memtable_);
                            closed_memtable_log_files_.push_back(
                                    atomic_memtable->memtable_->memtableid());
                            active_memtables_.erase(
                                    active_memtables_.begin() +
                                    emptiest_index);

                            number_of_active_memtables_ -= 1;
                            number_of_immutable_memtables_ += 1;
                        }

                        atomic_memtable->mutex_.unlock();
                        atomic_memtable = nullptr;

                        smallest_size = UINT64_MAX;
                        emptiest_index = -1;
                        emptiest_memtable = nullptr;
                    }
                }

                for (auto imm : full_memtables) {
                    MaybeScheduleCompaction(options.thread_id, imm,
                                            0, 0,
                                            options.rand_seed);
                }
                full_memtables.clear();

                if (atomic_memtable) {
                    range_lock_.Unlock();
                    break;
                }
                number_of_puts_wait_++;
                RDMA_LOG(rdmaio::DEBUG)
                    << fmt::format("db[{}]: Insert {} wait for pool",
                                   dbid_, key.ToString());
                Log(options_.info_log,
                    "Current memtable full; Make room waiting... tid-%lu\n",
                    options.thread_id);
                wait = true;
                memtable_available_signal_.Wait();
            }
        }

        if (wait) {
            RDMA_LOG(rdmaio::DEBUG)
                << fmt::format("db[{}]: Insert {} resume",
                               dbid_, key.ToString());
            Log(options_.info_log,
                "Make room; resuming... tid-%lu\n", options.thread_id);
        }


        RDMA_ASSERT(atomic_memtable);
        RDMA_ASSERT(!atomic_memtable->is_immutable_);
        RDMA_ASSERT(!atomic_memtable->is_flushed_);
        RDMA_ASSERT(atomic_memtable->memtable_);
        atomic_memtable->memtable_size_ += (key.size() + val.size());

        if (nova::NovaConfig::config->log_record_mode ==
            nova::NovaLogRecordMode::LOG_RDMA && !options.local_write) {
            // this memtable is selected. Replicate the log records first.
            // Increment the pending writes counter.
            atomic_memtable->number_of_pending_writes_ += 1;
            atomic_memtable->mutex_.unlock();

            auto dc = reinterpret_cast<leveldb::NovaBlockCCClient *>(options.dc_client);
            RDMA_ASSERT(dc);
            dc->set_dbid(dbid_);
            LevelDBLogRecord log_record = {};
            log_record.sequence_number = last_sequence;
            log_record.key = key;
            log_record.value = val;
            options.dc_client->InitiateReplicateLogRecords(
                    fmt::format("{}-{}-{}", server_id_, dbid_,
                                atomic_memtable->memtable_->memtableid()),
                    options.thread_id, dbid_,
                    atomic_memtable->memtable_->memtableid(),
                    options.rdma_backing_mem, log_record,
                    options.replicate_log_record_states);
            dc->Wait();
            atomic_memtable->mutex_.lock();
            atomic_memtable->number_of_pending_writes_ -= 1;
        }

        atomic_memtable->memtable_->Add(last_sequence, ValueType::kTypeValue,
                                        key, val);
        atomic_memtable->nentries_ += 1;
        if (table_locator_ != nullptr) {
            table_locator_->Insert(key, options.hash,
                                   atomic_memtable->memtable_->memtableid());
        }
        uint32_t full_memtable_id = 0;
        if (atomic_memtable->number_of_pending_writes_ == 0) {
            if (atomic_memtable->memtable_size_ >
                options_.write_buffer_size ||
                atomic_memtable->is_immutable_) {
                atomic_memtable->is_immutable_ = true;
                full_memtable_id = atomic_memtable->memtable_->memtableid();
            }
        }
        atomic_memtable->mutex_.unlock();

        if (full_memtable_id != 0) {
            range_lock_.Lock();
            for (int i = 0; i < active_memtables_.size(); i++) {
                if (active_memtables_[i]->memtable_->memtableid() ==
                    full_memtable_id) {
                    atomic_memtable_index = i;

                    full_memtables.push_back(atomic_memtable->memtable_);
                    closed_memtable_log_files_.push_back(
                            atomic_memtable->memtable_->memtableid());
                    active_memtables_.erase(
                            active_memtables_.begin() + atomic_memtable_index);
                    number_of_active_memtables_ -= 1;
                    number_of_immutable_memtables_ += 1;
                    break;
                }
            }
            range_lock_.Unlock();
        }

        for (auto imm : full_memtables) {
            MaybeScheduleCompaction(options.thread_id, imm, 0, 0,
                                    options.rand_seed);
        }
        return Status::OK();
    }

    void DBImpl::QueryDBStats(leveldb::DBStats *db_stats) {
        Version *current = nullptr;
        uint32_t vid = 0;
        while (current == nullptr) {
            vid = versions_->current_version_id();
            RDMA_ASSERT(vid < MAX_LIVE_MEMTABLES) << vid;
            current = versions_->versions_[vid].Ref();
        }
        RDMA_ASSERT(current->version_id() == vid);
        SubRanges *ref = latest_subranges_;
        db_stats->num_major_reorgs = num_major_reorgs;
        db_stats->num_minor_reorgs = num_minor_reorgs;
        db_stats->num_skipped_major_reorgs = num_skipped_major_reorgs;
        db_stats->num_skipped_minor_reorgs = num_skipped_minor_reorgs;
        db_stats->num_minor_reorgs_for_dup = num_minor_reorgs_for_dup;

        if (options_.enable_subranges) {
            if (nova::NovaConfig::config->client_access_pattern == "uniform") {
                uint64_t lower = 0;
                uint64_t upper = 0;
                uint64_t keys = 0;
                uint64_t fair_nkeys_per_subrange =
                        10000000.0 / options_.num_memtable_partitions;
                uint64_t max_keys = 0;
                for (int i = 0; i < ref->subranges.size(); i++) {
                    SubRange &sr = ref->subranges[i];
                    nova::str_to_int(sr.lower.data(), &lower, sr.lower.size());
                    nova::str_to_int(sr.upper.data(), &upper, sr.upper.size());

                    keys = upper - lower;
                    if (sr.upper_inclusive) {
                        keys += 1;
                    }
                    if (!sr.lower_inclusive) {
                        keys -= 1;
                    }
                    max_keys = std::max(max_keys, keys);
                }
                db_stats->maximum_load_imbalance =
                        ((double) max_keys / (double) fair_nkeys_per_subrange) *
                        100.0 - 100.0;
            } else {
                // Zipfian.
                uint64_t lower = 0;
                uint64_t upper = 0;
                uint64_t fair_naccesses_per_subrange =
                        nova::NovaConfig::config->zipfian_dist.sum /
                        options_.num_memtable_partitions;
                uint64_t max_accesses = 0;
                std::map<uint64_t, uint64_t> duplicated_keys;
                for (int i = 0; i < ref->subranges.size(); i++) {
                    SubRange &as = ref->subranges[i];
                    if (as.num_duplicates == 0) {
                        continue;
                    }
                    uint64_t lower;
                    uint64_t upper;
                    nova::str_to_int(as.lower.data(), &lower, as.lower.size());
                    nova::str_to_int(as.upper.data(), &upper, as.upper.size());
                    if (!as.lower_inclusive) {
                        lower++;
                    }
                    if (!as.upper_inclusive) {
                        upper -= 1;
                    }
                    for (uint64_t k = lower; k <= upper; k++) {
                        duplicated_keys[k] += 1;
                    }
                }
                for (int i = 0; i < ref->subranges.size(); i++) {
                    uint64_t accesses = 0;
                    SubRange &sr = ref->subranges[i];
                    nova::str_to_int(sr.lower.data(), &lower, sr.lower.size());
                    nova::str_to_int(sr.upper.data(), &upper, sr.upper.size());

                    if (!sr.upper_inclusive) {
                        upper -= 1;
                    }
                    if (!sr.lower_inclusive) {
                        lower += 1;
                    }
                    for (uint64_t key = lower; key <= upper; key++) {
                        if (duplicated_keys.find(key) !=
                            duplicated_keys.end()) {
                            accesses +=
                                    nova::NovaConfig::config->zipfian_dist.accesses[key] /
                                    duplicated_keys[key];
                        } else {
                            accesses += nova::NovaConfig::config->zipfian_dist.accesses[key];
                        }
                    }
                    max_accesses = std::max(max_accesses, accesses);
                }
                db_stats->maximum_load_imbalance =
                        ((double) max_accesses /
                         (double) fair_naccesses_per_subrange) *
                        100.0 - 100.0;
            }
        }
        current->QueryStats(db_stats, user_comparator_);
        versions_->versions_[vid].Unref(dbname_);
    }

    bool DBImpl::GetProperty(const Slice &property, std::string *value) {
        value->clear();

        MutexLock l(&mutex_);
        Slice in = property;
        Slice prefix("leveldb.");
        if (!in.starts_with(prefix)) return false;
        in.remove_prefix(prefix.size());

        if (in.starts_with("num-files-at-level")) {
            in.remove_prefix(strlen("num-files-at-level"));
            uint64_t level;
            bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
            if (!ok || level >= config::kNumLevels) {
                return false;
            } else {
                char buf[100];
                snprintf(buf, sizeof(buf), "%d",
                         versions_->NumLevelFiles(static_cast<int>(level)));
                *value = buf;
                return true;
            }
        } else if (in == "stats") {
            char buf[200];
            snprintf(buf, sizeof(buf),
                     "                               Compactions\n"
                     "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                     "--------------------------------------------------\n");
            value->append(buf);
            for (int level = 0; level < config::kNumLevels; level++) {
                int files = versions_->NumLevelFiles(level);
                if (stats_[level].micros > 0 || files > 0) {
                    snprintf(buf, sizeof(buf),
                             "%3d %8d %8.0f %9.0f %8.0f %9.0f\n", level,
                             files, versions_->NumLevelBytes(level) / 1048576.0,
                             stats_[level].micros / 1e6,
                             stats_[level].bytes_read / 1048576.0,
                             stats_[level].bytes_written / 1048576.0);
                    value->append(buf);
                }
            }
            return true;
        } else if (in == "sstables") {
            *value = versions_->current()->DebugString();
            return true;
        } else if (in == "approximate-memory-usage") {
            size_t total_usage = 0; //options_.block_cache->TotalCharge();
            for (auto mem : active_memtables_) {
//                total_usage += mem->ApproximateMemoryUsage();
            }
            char buf[50];
            snprintf(buf, sizeof(buf), "%llu",
                     static_cast<unsigned long long>(total_usage));
            value->append(buf);
            return true;
        }

        return false;
    }

    void
    DBImpl::GetApproximateSizes(const Range *range, int n, uint64_t *sizes) {
        // TODO(opt): better implementation
        MutexLock l(&mutex_);
        Version *v = versions_->current();
        for (int i = 0; i < n; i++) {
            // Convert user_key into a corresponding internal key.
            InternalKey k1(range[i].start, kMaxSequenceNumber,
                           kValueTypeForSeek);
            InternalKey k2(range[i].limit, kMaxSequenceNumber,
                           kValueTypeForSeek);
            uint64_t start = versions_->ApproximateOffsetOf(v, k1);
            uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
            sizes[i] = (limit >= start ? limit - start : 0);
        }
    }

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
    Status
    DB::Put(const WriteOptions &opt, const Slice &key, const Slice &value) {
        return Write(opt, key, value);
    }

    Status DB::Delete(const WriteOptions &opt, const Slice &key) {
        return Write(opt, key, Slice());
    }

    DB::~DB() = default;

    Status
    DB::Open(const Options &options, const std::string &dbname, DB **dbptr) {
        *dbptr = nullptr;
        DBImpl *impl = new DBImpl(options, dbname);
        impl->mutex_.Lock();
        impl->server_id_ = nova::NovaConfig::config->my_server_id;

        if (options.enable_subranges) {
            impl->latest_subranges_.store(new DBImpl::SubRanges);
        }
        if (!options.debug) {
            std::string manifest_file_name = DescriptorFileName(dbname, 0);
            impl->manifest_file_ = new NovaCCMemFile(options.env,
                                                     impl->options_, 0,
                                                     options.mem_manager,
                                                     options.dc_client,
                                                     impl->dbname_, 0,
                                                     options.max_file_size,
                                                     &impl->rand_seed_,
                                                     manifest_file_name);
        }


        for (int i = 0; i < MAX_LIVE_MEMTABLES; i++) {
            impl->versions_->mid_table_mapping_[i].nentries_ = 0;
        }

        if (options.memtable_type == MemTableType::kMemTablePool) {
            for (int i = 0; i < impl->min_memtables_; i++) {
                uint32_t memtable_id = impl->memtable_id_seq_.fetch_add(1);
                MemTable *new_table = new MemTable(impl->internal_comparator_,
                                                   memtable_id,
                                                   impl->db_profiler_);
                new_table->is_pinned_ = true;
                RDMA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                impl->versions_->mid_table_mapping_[memtable_id].SetMemTable(
                        new_table);
                impl->active_memtables_.push_back(
                        &impl->versions_->mid_table_mapping_[memtable_id]);
                RDMA_ASSERT(
                        options.memtable_pool->num_available_memtables_ >= 1);
                options.memtable_pool->num_available_memtables_ -= 1;

            }
            options.memtable_pool->range_cond_vars_[impl->dbid_] = &impl->memtable_available_signal_;
            impl->number_of_active_memtables_ = impl->min_memtables_;
        } else {
            impl->partitioned_active_memtables_.resize(
                    options.num_memtable_partitions);
            impl->partitioned_imms_.resize(options.num_memtables -
                                           options.num_memtable_partitions);

            for (int i = 0; i < impl->partitioned_imms_.size(); i++) {
                impl->partitioned_imms_[i] = 0;
            }

            uint32_t nslots = (options.num_memtables -
                               options.num_memtable_partitions) /
                              options.num_memtable_partitions;
            uint32_t remainder = (options.num_memtables -
                                  options.num_memtable_partitions) %
                                 options.num_memtable_partitions;
            uint32_t slot_id = 0;
            for (int i = 0; i < options.num_memtable_partitions; i++) {
                uint64_t memtable_id = impl->memtable_id_seq_.fetch_add(1);

                MemTable *table = new MemTable(impl->internal_comparator_,
                                               memtable_id,
                                               impl->db_profiler_);
                RDMA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                impl->versions_->mid_table_mapping_[memtable_id].SetMemTable(
                        table);
                impl->partitioned_active_memtables_[i] = new DBImpl::MemTablePartition;
                impl->partitioned_active_memtables_[i]->memtable = table;
                impl->partitioned_active_memtables_[i]->partition_id = i;
                uint32_t slots = nslots;
                if (remainder > 0) {
                    slots += 1;
                    remainder--;
                }
                impl->partitioned_active_memtables_[i]->imm_slots.resize(slots);
                for (int j = 0; j < slots; j++) {
                    impl->partitioned_active_memtables_[i]->imm_slots[j] =
                            slot_id + j;
                    impl->partitioned_active_memtables_[i]->available_slots.push(
                            slot_id + j);
                }
                slot_id += slots;
            }
            RDMA_ASSERT(slot_id == options.num_memtables -
                                   options.num_memtable_partitions);
            slot_id = 0;
            for (int i = 0; i < options.num_memtable_partitions; i++) {
                for (int j = 0; j <
                                impl->partitioned_active_memtables_[i]->imm_slots.size(); j++) {
                    RDMA_ASSERT(slot_id ==
                                impl->partitioned_active_memtables_[i]->imm_slots[j]);
                    slot_id++;
                }
            }
            impl->number_of_active_memtables_ = impl->partitioned_active_memtables_.size();
        }

        impl->number_of_available_pinned_memtables_ = 0;
        impl->number_of_memtable_hits_ = 0;
        impl->number_of_gets_ = 0;
        impl->number_of_wait_due_to_contention_ = 0;
        impl->number_of_steals_ = 0;
        impl->number_of_immutable_memtables_ = 0;
        impl->processed_writes_ = 0;
        impl->number_of_puts_no_wait_ = 0;
        impl->number_of_puts_wait_ = 0;
//        VersionEdit edit;
//        // Recover handles create_if_missing, error_if_exists
//        bool save_manifest = false;
//        Status s = impl->Recover(&edit, &save_manifest);
//        if (s.ok()) {
//            // Create new log and a corresponding memtable.
//            uint64_t new_log_number = impl->versions_->NewFileNumber();
//            WritableFile *lfile;
//            s = options.env->NewWritableFile(
//                    LogFileName(dbname, new_log_number),
//                    {.level = -1},
//                    &lfile);
//            impl->current_log_file_name_ = LogFileName(dbname, new_log_number);
//        }
//        if (s.ok() && save_manifest) {
//            Version *v = new Version(impl->versions_,
//                                     impl->versions_->version_id_seq_.fetch_add(
//                                             1));
//            s = impl->versions_->LogAndApply(&edit, v);
//        }
        impl->mutex_.Unlock();
        *dbptr = impl;
        return Status::OK();
    }

    Snapshot::~Snapshot() = default;

    Status DestroyDB(const std::string &dbname, const Options &options) {
        Env *env = options.env;
        std::vector<std::string> filenames;
        Status result = env->GetChildren(dbname, &filenames);
        if (!result.ok()) {
            // Ignore error in case directory does not exist
            return Status::OK();
        }

        FileLock *lock;
        const std::string lockname = LockFileName(dbname);
        result = env->LockFile(lockname, &lock);
        if (result.ok()) {
            uint64_t number;
            FileType type;
            for (size_t i = 0; i < filenames.size(); i++) {
                if (ParseFileName(filenames[i], &number, &type) &&
                    type != kDBLockFile) {  // Lock file will be deleted at end
                    Status del = env->DeleteFile(dbname + "/" + filenames[i]);
                    if (result.ok() && !del.ok()) {
                        result = del;
                    }
                }
            }
            env->UnlockFile(lock);  // Ignore error since state is already gone
            env->DeleteFile(lockname);
            env->DeleteDir(
                    dbname);  // Ignore error in case dir contains other files
        }
        return result;
    }

}  // namespace leveldb
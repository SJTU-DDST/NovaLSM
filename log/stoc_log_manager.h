
//
// Created by Haoyu Huang on 12/12/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_STOC_LOG_MANAGER_H
#define LEVELDB_STOC_LOG_MANAGER_H

#include <unordered_map>

#include "db/memtable.h"
#include "common/nova_mem_manager.h"
#include "common/nova_pm_manager.h"
#include "db/dbformat.h"
#include "leveldb/log_writer.h"
#include "leveldb/stoc_client.h"
#include "stoc/persistent_stoc_file.h"
//#include "common/nova_common.h"

namespace nova {
    // Manage in-memory log files to provide high availability.
    class StoCInMemoryLogFileManager {
    public:
        StoCInMemoryLogFileManager(NovaMemManager *mem_manager, NovaPMManager *pm_manager, leveldb::StocPersistentFileManager *stoc_file_manager);

        bool AddLocalBuf(const std::string &log_file, char *buf, leveldb::StoCLogType log_type);

        bool AddRemoteBuf(const std::string &log_file, uint32_t remote_server_id, uint64_t remote_buf_offset, leveldb::StoCLogType log_type);

        void DeleteLogBuf(const std::vector<std::string> &log_files, bool is_ltc);

        void QueryLogFiles(uint32_t range_id,
                           std::unordered_map<std::string, uint64_t> *logfile_offset); // 换config才会调用

        uint32_t EncodeLogFiles(char *buf, uint32_t dbid);

        bool
        DecodeLogFiles(leveldb::Slice *buf, std::unordered_map<uint32_t, leveldb::MemTableLogFilePair> *mid_table_map);

    private:
        struct LogRecords {
            leveldb::StoCLogType log_type;
            std::vector<char *> local_backing_mems;
            std::unordered_map<uint32_t, uint64_t> remote_backing_mems;
            std::mutex mu;
        };

//这个是什么结构呢?
        struct DBLogFiles {
            std::unordered_map<std::string, LogRecords *> logfiles_;
            leveldb::port::Mutex mutex_;
        };
        NovaMemManager *mem_manager_;
        NovaPMManager *pm_manager_;
        leveldb::StocPersistentFileManager *stoc_file_manager_;
        DBLogFiles **db_log_files_;
    };
}

#endif //LEVELDB_STOC_LOG_MANAGER_H

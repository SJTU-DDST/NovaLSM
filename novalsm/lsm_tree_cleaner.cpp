//
// Created by haoyuhua on 8/6/20.
//

#include <db/db_impl.h>
#include "lsm_tree_cleaner.h"

#include "common/nova_config.h"
#include "log/stoc_log_manager.h"

namespace leveldb {

//初始化
    LSMTreeCleaner::LSMTreeCleaner(nova::StoCInMemoryLogFileManager *log_manager, leveldb::StoCBlockClient *client)
            : log_manager_(log_manager), client_(client) {

    }

//只有cfg多于1个的才会开启这个线程，不知道是什么任务
    void LSMTreeCleaner::CleanLSM() const {
//周期性调用本地server管理的，不知道是什么任务???
        while (true) {
            sleep(5);
            int current_cfg_id = nova::NovaConfig::config->current_cfg_id;
            for (int fragid = 0; fragid < nova::NovaConfig::config->cfgs[current_cfg_id]->fragments.size(); fragid++) {
                auto current_frag = nova::NovaConfig::config->cfgs[current_cfg_id]->fragments[fragid];
                if (current_frag->is_complete_ &&
                    current_frag->ltc_server_id == nova::NovaConfig::config->my_server_id) {
                    auto db = reinterpret_cast<DBImpl *>(current_frag->db);
                    NOVA_ASSERT(db);
                    db->ScheduleFileDeletionTask();
                }
            }
        }
    }

// 定时冲刷immutable memtable
    void LSMTreeCleaner::FlushingMemTables() const {
//定时调用flush memtables
        while (true) {
            sleep(1);
            int current_cfg_id = nova::NovaConfig::config->current_cfg_id;
            for (int fragid = 0; fragid < nova::NovaConfig::config->cfgs[current_cfg_id]->fragments.size(); fragid++) {
                auto current_frag = nova::NovaConfig::config->cfgs[current_cfg_id]->fragments[fragid];
//如果当前
                if (current_frag->ltc_server_id == nova::NovaConfig::config->my_server_id) {
                    auto db = reinterpret_cast<DBImpl *>(current_frag->db);
                    NOVA_ASSERT(db);
                    db->FlushMemTables(false);
                }
            }
        }
    }

//周期性检查是否更新cfg并且有一些compaction log相关的工作
    void LSMTreeCleaner::CleanLSMAfterCfgChange() const {
        int current_cfg_id = nova::NovaConfig::config->current_cfg_id;

        while (true) {
            sleep(1);
            int new_cfg_id = nova::NovaConfig::config->current_cfg_id;
            NOVA_ASSERT(new_cfg_id == current_cfg_id || current_cfg_id + 1 == new_cfg_id);

            // A new config
            if (new_cfg_id == current_cfg_id + 1) {
                sleep(30);
                new_cfg_id = nova::NovaConfig::config->current_cfg_id;
                NOVA_ASSERT(current_cfg_id + 1 == new_cfg_id);

                for (int fragid = 0;
                     fragid < nova::NovaConfig::config->cfgs[current_cfg_id]->fragments.size(); fragid++) {
                    auto old_frag = nova::NovaConfig::config->cfgs[current_cfg_id]->fragments[fragid];
                    auto current_frag = nova::NovaConfig::config->cfgs[new_cfg_id]->fragments[fragid];
                    if (old_frag->ltc_server_id != current_frag->ltc_server_id &&
                        old_frag->ltc_server_id == nova::NovaConfig::config->my_server_id) {
                        auto db = reinterpret_cast<DBImpl *>(old_frag->db);
                        db->StopCompaction();
                        db->StopCoordinatedCompaction();

                        std::unordered_map<std::string, uint64_t> logfile_offset;
                        log_manager_->QueryLogFiles(old_frag->dbid, &logfile_offset);
                        std::vector<std::string> logfiles;
                        for (auto log : logfile_offset) {
                            logfiles.push_back(log.first);
                        }
                        if (!logfiles.empty()) {
                            client_->InitiateCloseLogFiles(logfiles, old_frag->dbid);
                        }
                        NOVA_LOG(rdmaio::INFO)
                            << fmt::format("Clean db-{} with log files:{}", old_frag->dbid, logfiles.size());
                    }
                }
                new_cfg_id = nova::NovaConfig::config->current_cfg_id;
                NOVA_ASSERT(current_cfg_id + 1 == new_cfg_id);
                current_cfg_id = new_cfg_id;
            }
        }
    }
}


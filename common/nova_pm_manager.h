#ifndef NOVA_PM_MANAGER_H
#define NOVA_PM_MANAGER_H

#include <string>
#include <stdint.h>
#include <cstring>
#include <vector>
#include <queue>
#include <mutex>
#include "leveldb/db_types.h"
// #include "nova_mem_manager.h"
#include "common/nova_console_logging.h"
#include <unordered_map>

namespace nova {

#define PMUNITSIZE 100
#define MANIFESTMAXSIZE 64 * 1024 * 1024
#define WALLOGMAXSIZE 18 * 1024 * 1024
#define SSTABLEMAXSIZE 18 * 1024 * 1024
#define SSTABLEMETAMAXSIZE 210 * 1024

// enum AllocType : char {
//     Manifest = 'm',
//     Sstable = 's',
//     SstableMeta = 'x'
// }

    struct PMUnit{
        std::mutex mutex_;
        std::queue<char *> units_; 
        std::atomic<uint64_t> freecount_;

        std::mutex mapmutex_;        
        std::unordered_map<char*, std::string> buf2filename_;
        std::unordered_map<std::string, char*> filename2buf_;


        void addunitinit(char* buf){
            units_.push(buf);
            freecount_++;
        }

        void addunit(std::string key, char* buf){
            mutex_.lock();
            NOVA_ASSERT(buf2filename_.find(buf) != buf2filename_.end()) << "free an incorrect buf";
            buf2filename_.erase(buf);
            filename2buf_.erase(key);
            units_.push(buf);
            mutex_.unlock();
            freecount_++;
        }

        char* getunit(std::string key){
            mutex_.lock();
            if(units_.empty()){
                NOVA_ASSERT(false) << "no pm space";
                mutex_.unlock();
                return nullptr;
            }
            char* buf = units_.front();
            units_.pop();
            buf2filename_[buf] = key;
            filename2buf_[key] = buf;
            mutex_.unlock();
            freecount_--;
            return buf;
        }
    };

    class NovaPartitionedPMManager {
    public:
        NovaPartitionedPMManager(int db_index, char *buf, uint64_t partition_size);

        char *ItemAlloc(int db_index, std::string key);

        void FreeItem(int db_index, std::string key, char *buf);

    private:
        int db_index_; // 负责的数据库编号
        char *buf_; // 负责的区域地址
        uint64_t partition_size_; // 负责的区域大小

        PMUnit sstable_units_[PMUNITSIZE]; // 用于分配sstable 哈希减少冲突
        PMUnit sstable_meta_units_[PMUNITSIZE]; // 用于分配sstable meta 哈希减少冲突
        PMUnit manifest_units_; // 用于分配manifest
        PMUnit wallog_units_; // 用于分配wal log

        // std::unordered_map<std::string, PMUnit*> filename2unit_; // 文件名到相应的区
        // std::unordered_map<char*, std::string> buf2filename_; // 申请的区域到相应的文件名

    };

    class NovaPMManager : public leveldb::MemManager {
    public:
        NovaPMManager(char** buf, std::string pm_pool_name, uint64_t pm_pool_size_gb, int db_numbers);

        char *ItemAlloc(uint64_t key, uint32_t scid) override;

        void FreeItem(uint64_t key, char *buf, uint32_t scid) override;

        void FreeItems(uint64_t key, const std::vector<char *> &items, uint32_t scid) override;

        uint32_t slabclassid(uint64_t key, uint64_t  size) override;

        char *ItemAlloc(int db_index, std::string key);

        void FreeItem(int db_index, std::string key, char *buf);

        char *ItemAlloc(int db_index, leveldb::Slice key); // 大概也没有

        void FreeItem(int db_index, leveldb::Slice key, char *buf); // 大概也没有

    private:
        char* GetdbBufStart(int db_index);
        char* GetdbBufEnd(int db_index);

        std::string pm_pool_name_;
        uint64_t pm_pool_size_;
        int db_numbers_;
        int fd_;
        char *mmap_base_ = nullptr;
        std::vector<NovaPartitionedPMManager *> db_partitioned_pm_managers_;
    };
}
#endif //NOVA_PM_MANAGER_H
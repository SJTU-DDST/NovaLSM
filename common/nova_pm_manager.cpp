#include <fmt/core.h>

#include "nova_pm_manager.h"
#include "nova_common.h"
#include "city_hash.h"
#include "db/filename.h"
#include<sys/mman.h>
#include<fcntl.h>
#include<unistd.h>
#include "common/nova_console_logging.h"

namespace nova{

// 64MB 18MB 210KB
// partition 按 30G算 
    NovaPartitionedPMManager::NovaPartitionedPMManager(int db_index, char *buf, uint64_t partition_size)
            : db_index_(db_index),
            buf_(buf),
            partition_size_(partition_size){

        // 准备manifest的空间
        for(int i = 0; i < 2; i++){
            manifest_units_.addunitinit(buf);
            buf += MANIFESTMAXSIZE;
        }

        uint64_t space_left = partition_size - 2 * MANIFESTMAXSIZE; // 除去manifest区之后的剩余空间
        int count = space_left / (SSTABLEMAXSIZE + SSTABLEMETAMAXSIZE); // 让sstable和sstable meta个数相同 总个数
        int count_per_slot = count / PMUNITSIZE; // 每个分区的数量


        // 准备sstable的空间
        for(int i = 0; i < PMUNITSIZE; i++){
            for(int j = 0; j < count_per_slot; j++){
                sstable_units_[i].addunitinit(buf);
                buf += SSTABLEMAXSIZE;
            }
        }

        // 准备sstable meta的空间

        for(int i = 0; i < PMUNITSIZE; i++){
            for(int j = 0; j < count_per_slot; j++){
                sstable_meta_units_[i].addunitinit(buf);
                buf += SSTABLEMETAMAXSIZE;
            }
        }
    }

    char* NovaPartitionedPMManager::ItemAlloc(int db_index, std::string key){
        leveldb::FileType type;
        ParseFileName(key, &type);
        char* buf = nullptr;
        // manifest
        if(type == leveldb::FileType::kDescriptorFile){
            buf = manifest_units_.getunit(key);
            NOVA_ASSERT(buf != nullptr) << "pm allocator oom for manifest";
        }
        // meta
        if(key.find("meta") != std::string::npos){
            uint64_t index = CityHash64(key.c_str(), key.size());
            buf = sstable_meta_units_[index].getunit(key);
            NOVA_ASSERT(buf != nullptr) << "pm allocator oom for sstable meta";
        }else{// sstable
            uint64_t index = CityHash64(key.c_str(), key.size());
            buf = sstable_units_[index].getunit(key);
            NOVA_ASSERT(buf != nullptr) << "pm allocator oom for sstable";
        }


    }

    void NovaPartitionedPMManager::FreeItem(int db_index, std::string key, char *buf){
        leveldb::FileType type;
        ParseFileName(key, &type);
        // manifest
        if(type == leveldb::FileType::kDescriptorFile){
            manifest_units_.addunit(key, buf);
        }
        // meta
        if(key.find("meta") != std::string::npos){
            uint64_t index = CityHash64(key.c_str(), key.size());
            sstable_meta_units_[index].addunit(key, buf);
        }else{// sstable
            uint64_t index = CityHash64(key.c_str(), key.size());
            sstable_units_[index].addunit(key, buf);
        }
    }

// 最大的块64MB 64MB 18MB 210KB 
// pm_pool_name这个持久内存池应该是本node所有的db公用的
    NovaPMManager::NovaPMManager(char** buf, std::string pm_pool_name, uint64_t pm_pool_size_gb, int db_numbers)
            :pm_pool_name_(pm_pool_name), 
            pm_pool_size_(pm_pool_size_gb * 1024 * 1024 * 1024), 
            db_numbers_(db_numbers) {
        // 打开并且mmap
        fd_ = ::open(pm_pool_name.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        NOVA_ASSERT(fd_ >= 0) << "pm pool file open failed";
        //NOVA_ASSERT(ftruncate(fd_, pm_pool_size_) >= 0) << "pm pool file ftruncate failed";
        NOVA_ASSERT(posix_fallocate(fd_, 0, pm_pool_size_)) << "pm pool file fallocate failed";
        mmap_base_ = (char *)::mmap(nullptr, pm_pool_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        *buf = mmap_base_;
        // NOVA_ASSERT(mmap_base_ == buf_) << "pm pool file mmap unproperly";
        // 按db划分pm区域
        uint64_t partition_size =  pm_pool_size_ / db_numbers;
        char *base = mmap_base_;
        for (int i = 0; i < db_numbers; i++) {
            db_partitioned_pm_managers_.push_back(new NovaPartitionedPMManager(i, base, partition_size));
            base += partition_size;
        }
    }


// 不会用到
    uint32_t NovaPMManager::slabclassid(uint64_t key, uint64_t size) {
        ;
    }

    char *NovaPMManager::ItemAlloc(uint64_t key, uint32_t scid) {
        ;
    }

    void NovaPMManager::FreeItem(uint64_t key, char *buf, uint32_t scid) {
        ;
    }

    void NovaPMManager::FreeItems(uint64_t key, const std::vector<char *> &items,
                                              uint32_t scid) {
        ;
    }


// 下面是重点
    char* NovaPMManager::GetdbBufStart(int db_index){
        return mmap_base_ + pm_pool_size_ / (uint64_t)db_numbers_ * db_index;
    }

    char* NovaPMManager::GetdbBufEnd(int db_index){
        char *calend = mmap_base_ + pm_pool_size_ / (uint64_t)db_numbers_ * (db_index + 1);
        return calend > mmap_base_ + pm_pool_size_ ? mmap_base_ + pm_pool_size_ : calend;
    }


// string和slice
    char *NovaPMManager::ItemAlloc(int db_index, std::string key){
        NOVA_ASSERT(db_index < db_numbers_) << "invalid db_index";
        return db_partitioned_pm_managers_[db_index]->ItemAlloc(db_index, key);
    }

    void NovaPMManager::FreeItem(int db_index, std::string key, char *buf){
        NOVA_ASSERT(db_index < db_numbers_) << "invalid db_index";
        db_partitioned_pm_managers_[db_index]->FreeItem(db_index, key, buf);
    }

    char *NovaPMManager::ItemAlloc(int db_index, leveldb::Slice key){
        NOVA_ASSERT(db_index < db_numbers_) << "invalid db_index";
        std::string strKey = std::string(key.data(), key.size());
        return db_partitioned_pm_managers_[db_index]->ItemAlloc(db_index, strKey);
    }

    void NovaPMManager::FreeItem(int db_index, leveldb::Slice key, char *buf){
        NOVA_ASSERT(db_index < db_numbers_) << "invalid db_index";
        std::string strKey = std::string(key.data(), key.size());
        db_partitioned_pm_managers_[db_index]->FreeItem(db_index, strKey, buf);
    }

}
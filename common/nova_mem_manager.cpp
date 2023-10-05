
//
// Created by Haoyu Huang on 4/8/19.
// Copyright (c) 2019 University of Southern California. All rights reserved.
//

#include <fmt/core.h>

#include "nova_mem_manager.h"
#include "nova_common.h"

namespace nova {

//slab初始化
    Slab::Slab(char *base, uint64_t slab_size_mb) {
        next_ = base;
        slab_size_mb_ = slab_size_mb;
    }

//取出slab的时候进行初始化
    void Slab::Init(uint32_t item_size) {
        uint64_t size = slab_size_mb_ * 1024 * 1024;
        item_size_ = item_size;
        auto num_items = static_cast<uint32_t>(size / item_size);
        available_bytes_ = item_size * num_items;
    }

//从slab里面分配
    char *Slab::AllocItem() {
        if (available_bytes_ < item_size_) {
            return nullptr;
        }
        char *buf = next_;
        next_ += item_size_;
        available_bytes_ -= item_size_;
        return buf;
    }

    char *SlabClass::AllocItem() {
//如果freelist不空，那就分配一个出来
        // check free list first.
        if (!free_list.empty()) {
            char *ptr = free_list.front();
            NOVA_ASSERT(ptr != nullptr);
            free_list.pop();
            return ptr;
        }

//如果free_list空且slabs也为空，那分配不出来
        if (slabs.empty()) {
            return nullptr;
        }

//如果slabs不空，从里面分配
        Slab *slab = slabs[slabs.size() - 1];
        return slab->AllocItem();
    }

//释放下来的放进item里面
    void SlabClass::FreeItem(char *buf) {
        free_list.push(buf);
    }

//向slabclass里面加一个slab
    void SlabClass::AddSlab(Slab *slab) {
        slabs.push_back(slab);
    }

//pid是当前partitionmemanager的标号
    NovaPartitionedMemManager::NovaPartitionedMemManager(int pid, char *buf,
                                                         uint64_t data_size,
                                                         uint64_t slab_size_mb)
            : slab_size_mb_(slab_size_mb) {
//这里又换成B为单位
        uint64_t slab_size = slab_size_mb * 1024 * 1024;
//        uint64_t slab_sizes[] = {8192, 1024 };

        uint64_t size = 1200;
        for (int i = 0; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
//当前这个级别的slab的size，以及一个slab能装多少个这个size的item
            slab_classes_[i].size = size;
            slab_classes_[i].nitems_per_slab = slab_size / size;
            if (pid == 0) {
                NOVA_LOG(INFO) << "slab class " << i << " size:" << size
                               << " nitems:"
                               << slab_size / size;
            }
            size *= SLAB_SIZE_FACTOR;
            if (size > slab_size) {
                size = slab_size;
            }
        }
//管理的内存大小有多少slab
        uint64_t ndataslabs = data_size / slab_size;
        if (pid == 0) {
            NOVA_LOG(INFO)
                << fmt::format("slab size mb:{} nslabs:{}", slab_size_mb,
                               ndataslabs);
        }

        free_slabs_ = (Slab **) malloc(ndataslabs * sizeof(Slab *));
        free_slab_index_ = ndataslabs - 1;
        char *slab_buf = buf;
//给各个slab建立相应的管理结构
        for (int i = 0; i < ndataslabs; i++) {
            auto *slab = new Slab(slab_buf, slab_size_mb);
            free_slabs_[i] = slab;
            slab_buf += slab_size;
        }
    }

//找到分配size的空间所需要的slab的级别
    uint32_t NovaPartitionedMemManager::slabclassid(uint64_t size) {
        NOVA_ASSERT(size > 0 && size <= slab_size_mb_ * 1024 * 1024)
            << fmt::format("alloc size:{} max size:{}", size,
                           slab_size_mb_ * 1024 * 1024);
        uint32_t res = 0;
        while (size > slab_classes_[res].size) {
            res++;
        }
        NOVA_ASSERT(res < MAX_NUMBER_OF_SLAB_CLASSES) << size;
        return res;
    }

    char *NovaPartitionedMemManager::ItemAlloc(uint32_t scid) {
        char *free_item = nullptr;
        Slab *slab = nullptr;

        slab_class_mutex_[scid].lock();
        free_item = slab_classes_[scid].AllocItem();
//如果可以分配，直接返回
        if (free_item != nullptr) {
            slab_class_mutex_[scid].unlock();
            return free_item;
        }
        // Grab a slab from the free list.
        free_slabs_mutex_.lock();
//如果分配不出来，且没有空闲的slab了
        if (free_slab_index_ == -1) {
            free_slabs_mutex_.unlock();
            slab_class_mutex_[scid].unlock();
//打印oom信息
            oom_lock.lock();
            if (!print_class_oom) {
                NOVA_LOG(INFO) << "No free slabs: Print slab class usages.";
                print_class_oom = true;
                for (int i = 0; i < MAX_NUMBER_OF_SLAB_CLASSES; i++) {
                    slab_class_mutex_[scid].lock();
                    NOVA_LOG(INFO) << fmt::format(
                                "slab class {} size:{} nfreeitems:{} slabs:{}",
                                i,
                                slab_classes_[i].size,
                                slab_classes_[i].free_list.size(),
                                slab_classes_[i].slabs.size());
                    slab_class_mutex_[scid].unlock();
                }
            }
            oom_lock.unlock();
            return nullptr;
        }
//如果分配不出来，那就取出一个slab
        slab = free_slabs_[free_slab_index_];
        free_slab_index_--;
        free_slabs_mutex_.unlock();

        slab->Init(static_cast<uint32_t>(slab_classes_[scid].size));
//把取出来的slab装到slabclass里面，然后再分配后返回
        slab_classes_[scid].AddSlab(slab);
        free_item = slab->AllocItem();
        slab_class_mutex_[scid].unlock();
        return free_item;
    }

    void NovaPartitionedMemManager::FreeItem(char *buf, uint32_t scid) {
//        memset(buf, 0, slab_classes_[scid].size);
        slab_class_mutex_[scid].lock();
        slab_classes_[scid].FreeItem(buf);
        slab_class_mutex_[scid].unlock();
    }

    void NovaPartitionedMemManager::FreeItems(const std::vector<char *> &items,
                                              uint32_t scid) {
        slab_class_mutex_[scid].lock();
        for (auto buf : items) {
            slab_classes_[scid].FreeItem(buf);
        }
        slab_class_mutex_[scid].unlock();
    }

//初始化memmanager
    NovaMemManager::NovaMemManager(char *buf, uint32_t num_mem_partitions,
                                   uint64_t mem_pool_size_gb,
                                   uint64_t slab_size_mb) {
//计算出一个partition的大小
        uint64_t partition_size = mem_pool_size_gb * 1024 * 1024 * 1024 /
                                  num_mem_partitions;//这里单位好像是B
        char *base = buf;
        for (int i = 0; i < num_mem_partitions; i++) {
            partitioned_mem_managers_.push_back(
                    new NovaPartitionedMemManager(i, base, partition_size,
                                                  slab_size_mb));
            base += partition_size;
        }
    }

//根据key找到对应的partition并且从scid这个级别分配空间
    char *NovaMemManager::ItemAlloc(uint64_t key, uint32_t scid) {
        return partitioned_mem_managers_[key %
                                         partitioned_mem_managers_.size()]->ItemAlloc(
                scid);
    }

//根据key找到对应的partition并得到分配size空间所需要的slab的级别
    uint32_t NovaMemManager::slabclassid(uint64_t key, uint64_t size) {
        return partitioned_mem_managers_[key %
                                         partitioned_mem_managers_.size()]->slabclassid(
                size);
    }

    void NovaMemManager::FreeItem(uint64_t key, char *buf, uint32_t scid) {
        partitioned_mem_managers_[key %
                                  partitioned_mem_managers_.size()]->FreeItem(
                buf, scid);
    }

    void NovaMemManager::FreeItems(uint64_t key,
                                   const std::vector<char *> &items,
                                   uint32_t scid) {
        partitioned_mem_managers_[key %
                                  partitioned_mem_managers_.size()]->FreeItems(
                items, scid);
    }

}

//
// Created by Haoyu Huang on 1/29/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//
#include <fmt/core.h>
#include "leveldb/cache.h"
#include "db/filename.h"
#include "util/env_posix.h"

#include "persistent_stoc_file.h"
#include "common/nova_console_logging.h"
#include "common/nova_common.h"
#include "common/nova_config.h"

namespace leveldb {

    std::string StoCBlockHandle::DebugString() const {
        return fmt::format("[{} {} {} {}]", server_id, stoc_file_id, offset,
                           size);
    }

// handle 是为了 在block中进行缓存所需要的key
    void StoCBlockHandle::EncodeHandle(char *buf) const {
        EncodeFixed32(buf, server_id);
        EncodeFixed32(buf + 4, stoc_file_id);
        EncodeFixed64(buf + 8, offset);
        EncodeFixed32(buf + 16, size);
    }

    void StoCBlockHandle::DecodeHandle(const char *buf) {
        server_id = DecodeFixed32(buf);
        stoc_file_id = DecodeFixed32(buf + 4);
        offset = DecodeFixed64(buf + 8);
        size = DecodeFixed32(buf + 16);
    }

    bool StoCBlockHandle::DecodeHandle(leveldb::Slice *data,
                                       leveldb::StoCBlockHandle *handle) {
        if (data->size() < HandleSize()) {
            return false;
        }

        handle->DecodeHandle(data->data());
        *data = Slice(data->data() + HandleSize(), data->size() - HandleSize());
        return true;
    }

    bool StoCBlockHandle::DecodeHandles(leveldb::Slice *data,
                                        std::vector<leveldb::StoCBlockHandle> *handles) {
        uint32_t size = 0;
        if (!DecodeFixed32(data, &size)) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            StoCBlockHandle handle = {};
            if (!DecodeHandle(data, &handle)) {
                return false;
            }
            handles->push_back(handle);
        }
        return true;
    }
// FileInternalType::kFileData 既可能是sstable也可能是manifest
// done 
// 新建文件并且 分配预先指定的空间 需要加一个判断是不是在pm里的东西 也可以从文件名判断
    StoCPersistentFile::StoCPersistentFile(uint32_t file_id,
                                           leveldb::Env *env,
                                           std::string filename,
                                           MemManager *mem_manager,
                                           uint32_t thread_id,
                                           uint32_t file_size,
                                           bool is_manifest) :
            file_id_(file_id), env_(env), stoc_file_name_(filename), is_pm_file_(IsPMfile(filename)), is_manifest_(is_manifest),
            mem_manager_(mem_manager), thread_id_(thread_id) {
        EnvFileMetadata meta; // 这个meta没有任何用处
        meta.level = 0;
        Status s = env_->NewReadWriteFile(filename, meta, file_size, &file_); // 也许会用pm使用 有则直接打开 无则新建 
        NOVA_ASSERT(s.ok()) << s.ToString();

        // 如果不在pm中就申请内存作为缓冲区
        if(!is_pm_file_){ // maniest或者更深层的sstable
            uint32_t scid = mem_manager_->slabclassid(thread_id,
                                                    file_size);
            backing_mem_ = mem_manager->ItemAlloc(thread_id, scid);
            file_size_ = 0;
            allocated_mem_size_ = file_size;

            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format(
                        "StoC file {} created with t:{} file size {} allocated size {}",
                        file_id_, thread_id,
                        file_size_, allocated_mem_size_);

            NOVA_ASSERT(backing_mem_) << "Running out of memory";
        }else{ // 为l0层文件的话就pm mmap
            backing_mem_ = dynamic_cast<leveldb::PosixPMReadWriteFile*>(file_)->GetMmapBase();
            file_size_ = 0; // 这两个变量先与之前保持一致
            allocated_mem_size_ = file_size; 
        }
        // pm的话不用做什么
    }
// no need
// change config的时候调用 不change config的话不会用到
    Status
    StoCPersistentFile::ReadForReplication(uint64_t offset, uint32_t size,
                                           char *scratch, Slice *result) {
        mutex_.lock();
        if (deleted_) {
            mutex_.unlock();
            return Status::NotFound("");
        }
        reading_cnt++;
        mutex_.unlock();

        StoCBlockHandle h = {};
        nova::NovaGlobalVariables::global.stoc_queue_depth += 1;
        nova::NovaGlobalVariables::global.stoc_pending_disk_reads += size;
        nova::NovaGlobalVariables::global.total_disk_reads += size;
        Status status = file_->Read(h, offset, size, result, scratch);
        nova::NovaGlobalVariables::global.stoc_queue_depth -= 1;
        nova::NovaGlobalVariables::global.stoc_pending_disk_reads -= size;

        mutex_.lock();
        reading_cnt--;
        if (reading_cnt == 0 && waiting_to_be_deleted && !deleted_) {
            waiting_to_be_deleted = false;
            deleted_ = true;
            NOVA_LOG(rdmaio::DEBUG) << fmt::format(
                        "Delete  Stoc File {}.", stoc_file_name_);
            NOVA_ASSERT(file_);
            Status s = file_->Close();
            NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
            delete file_;
            file_ = nullptr;
            s = env_->DeleteFile(stoc_file_name_);
            NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        }
        if (deleted_) {
            mutex_.unlock();
            return Status::NotFound("");
        }
        mutex_.unlock();
        return status;
    }

// no need
// pm的话 无需改动
    Status
    StoCPersistentFile::Read(uint64_t offset, uint32_t size, char *scratch,
                             Slice *result) {
        StoCBlockHandle h = {};
        nova::NovaGlobalVariables::global.stoc_queue_depth += 1;
        nova::NovaGlobalVariables::global.stoc_pending_disk_reads += size;
        nova::NovaGlobalVariables::global.total_disk_reads += size;
        Status status = file_->Read(h, offset, size, result, scratch); // 普通文件系统调用读 pm文件用mmap读
        nova::NovaGlobalVariables::global.stoc_queue_depth -= 1;
        nova::NovaGlobalVariables::global.stoc_pending_disk_reads -= size;
        return status;
    }
// done
//用来标记persistent文件中哪部分写了没有 标记buf写入
    bool StoCPersistentFile::MarkOffsetAsWritten(
            uint32_t given_file_id_for_assertion,
            uint64_t offset) {
        NOVA_ASSERT(given_file_id_for_assertion == file_id_)
            << fmt::format("{} {}", given_file_id_for_assertion, file_id_);
        if(is_manifest_){
            bool found = false;
            mutex_.lock();
            uint64_t relative_off = offset - (uint64_t) (backing_mem_);
            for (auto it = allocated_bufs_.rbegin();
                it != allocated_bufs_.rend(); it++) {
                if (it->offset == relative_off) {
                    it->written_to_mem = true;
                    found = true;
                    break;
                }
            }
            mutex_.unlock();
            if (!found) {
                NOVA_LOG(rdmaio::INFO)
                    << fmt::format("stocfile:{} id:{}", stoc_file_name_,
                                file_id_);
            }
            return found;               
        }else{ // 其余只需要1个buf
            bool found = false;
            uint64_t relative_off = offset - (uint64_t) (backing_mem_);
            if(sstable_buf_.offset == relative_off){
                sstable_buf_.written_to_mem = true;
                found = true;
            }
            if (!found) {
                NOVA_LOG(rdmaio::INFO)
                    << fmt::format("stocfile:{} id:{}", stoc_file_name_,
                                file_id_);
            }
            return found;   
        }
    }
// done
// 规划申请的空间 除了 manifest之外应该没有人需要调用两次以上?? 返回了要写入的虚拟地址
    uint64_t StoCPersistentFile::AllocateBuf(const std::string &filename,
                                             uint32_t size,
                                             FileInternalType internal_type) {
        // manifest file需要多个其他的全部都是1个
        if(is_manifest_){
            NOVA_ASSERT(current_mem_offset_ + size <= allocated_mem_size_) // 
                << "exceed maximum stoc file size "
                << size << ","
                << allocated_mem_size_;
            leveldb::FileType type = leveldb::FileType::kCurrentFile;
            NOVA_ASSERT(ParseFileName(filename, &type));
            mutex_.lock();
            // is_ful只有close和forceseal更改了 manifest永远不会调用 其他的也没有
            if (is_full_ || current_mem_offset_ + size > allocated_mem_size_) { //第二个条件不可能 这两个条件都不可能 不可能会进入
                Seal();
                mutex_.unlock();
                return UINT64_MAX;
            }
            NOVA_ASSERT(!sealed_);
            uint32_t off = current_mem_offset_; // 当前内存缓冲区的offset 只在allocatebuf的时候改
            // NOVA_ASSERT(file_block_offset_.find(filename) == file_block_offset_.end());
            // 如果是manifest文件的话无所谓 因为要多加好几次
            current_mem_offset_ += size;
            AllocatedBuf allocated_buf = {};
            allocated_buf.filename = filename;
            allocated_buf.offset = off;
            allocated_buf.size = size;
            allocated_buf.written_to_mem = false;
            allocated_buf.internal_type = internal_type;
            allocated_bufs_.push_back(allocated_buf);
            file_size_ += size;
            mutex_.unlock();
            return (uint64_t) (backing_mem_) + off;            
        }else{//其余的都是1次allocate buf解决
            leveldb::FileType type = leveldb::FileType::kCurrentFile;
            NOVA_ASSERT(ParseFileName(filename, &type));
            mutex_.lock();
            uint32_t off = current_mem_offset_;
            // 这里应该可以直接加入 不过无所谓了
            if (internal_type == FileInternalType::kFileMetadata) {
                NOVA_ASSERT(file_meta_block_offset_.find(filename) == file_meta_block_offset_.end());
            } else if (internal_type == FileInternalType::kFileParity) {
                NOVA_ASSERT(file_parity_block_offset_.find(filename) == file_parity_block_offset_.end());
            } else if (type == leveldb::FileType::kTableFile) {
                NOVA_ASSERT(file_block_offset_.find(filename) == file_block_offset_.end());
            }
            current_mem_offset_ += size; // 这里大概率直接设置为目标大小了
            sstable_buf_.filename = filename;
            sstable_buf_.offset = off;
            sstable_buf_.size = size;
            sstable_buf_.written_to_mem = false;
            sstable_buf_.internal_type = internal_type;
            //allocated_bufs_.push_back(allocated_buf); 无需加入大的buf
            file_size_ += size; // 同上
            mutex_.unlock();
            return (uint64_t) (backing_mem_) + off; // backing mem已经做过适配了
        }
    }

// 这说明这三种情况都不会有alloc buf的情况??
// 只有manifest可能出现一些奇怪的情况
    uint64_t
    StoCPersistentFile::Persist(uint32_t given_file_id_for_assertion) {
        NOVA_ASSERT(given_file_id_for_assertion == file_id_)
            << fmt::format("{} {}", given_file_id_for_assertion, file_id_);
        // manifest可能有多个要按之前的判断
        if(is_manifest_){
            uint64_t persisted_bytes = 0;
            mutex_.lock();
            if (allocated_bufs_.empty()) { // manifest可能但是seal不会做任何事情
                Seal();
                mutex_.unlock();
                return persisted_bytes;
            }

            // sequential IOs to disk.
            auto buf = allocated_bufs_.begin();
            while (buf != allocated_bufs_.end()) {
                if (!buf->written_to_mem) {
                    buf++;
                    continue;
                }

                // 设置handle相关的元数据
                leveldb::FileType type = leveldb::FileType::kCurrentFile;
                NOVA_ASSERT(leveldb::ParseFileName(buf->filename, &type));
    // 在这里加入当前handle的信息 改为确认是manifest
                NOVA_ASSERT(type == leveldb::FileType::kDescriptorFile);
                StoCPersistStatus &s = file_block_offset_[buf->filename]; // 对于manifest来说好像无所谓因为不会读取这个所谓的handle?
                s.disk_handle.set_offset(current_disk_offset_);
                s.disk_handle.set_size(buf->size);
                s.persisted = false;

// 为批量写入做好准备
                BlockHandle mem_handle = {};
                mem_handle.set_offset(buf->offset);
                mem_handle.set_size(buf->size);
                persisting_cnt += 1;
                current_disk_offset_ += buf->size;

                BatchWrite bw = {};
                bw.mem_handle.set_offset(buf->offset);
                bw.mem_handle.set_size(buf->size);
                bw.sstable = buf->filename;
                bw.internal_type = buf->internal_type;
                written_mem_blocks_.push_back(bw);
                buf = allocated_bufs_.erase(buf);
            }

            NOVA_ASSERT(current_disk_offset_ <= file_size_);
            mutex_.unlock();

            persist_mutex_.lock();
            // Make a copy of written_mem_blocks.
            mutex_.lock();
            std::vector<BatchWrite> writes;
            for (int i = 0; i < written_mem_blocks_.size(); i++) {
                writes.push_back(written_mem_blocks_[i]);
            }
            if (writes.empty()) { // 可能其他进程完成了 seal不可能做任何事情
                Seal();
            }
            mutex_.unlock();

            if (writes.empty()) {
                persist_mutex_.unlock();
                return persisted_bytes;
            }

            int i = 1;
            int persisted_i = 0;
            uint64_t offset = writes[0].mem_handle.offset();
            uint64_t size = writes[0].mem_handle.size();
            while (i < writes.size()) { // 把连续的写组织在一起
                if (offset + size == writes[i].mem_handle.offset()) {
                    size += writes[i].mem_handle.size();
                    i++;
                    continue;
                }

                // persist offset -> size.
                nova::NovaGlobalVariables::global.stoc_queue_depth += 1;
                nova::NovaGlobalVariables::global.stoc_pending_disk_writes += size;
                nova::NovaGlobalVariables::global.total_disk_writes += size;
                persisted_bytes += size;

                Status s = file_->Append(Slice(backing_mem_ + offset, size));
                NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
                s = file_->Sync();
                NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());

                nova::NovaGlobalVariables::global.stoc_queue_depth -= 1;
                nova::NovaGlobalVariables::global.stoc_pending_disk_writes -= size;

                mutex_.lock();
                for (int j = persisted_i; j < i; j++) {
                    NOVA_ASSERT(file_block_offset_.find(writes[j].sstable) != file_block_offset_.end());
                    file_block_offset_[writes[j].sstable].persisted = true;
                    persisting_cnt -= 1;
                }
                // persisting_cnt = persisting_cnt - (i - persisted_i);
                mutex_.unlock();
                persisted_i = i;
                offset = writes[i].mem_handle.offset();
                size = writes[i].mem_handle.size();
                i += 1;
            }
            // Persist the last range.
            nova::NovaGlobalVariables::global.stoc_queue_depth += 1;
            nova::NovaGlobalVariables::global.stoc_pending_disk_writes += size;
            nova::NovaGlobalVariables::global.total_disk_writes += size;
            persisted_bytes += size;

            Status s = file_->Append(Slice(backing_mem_ + offset, size));
            NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
            s = file_->Sync();
            NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());

            nova::NovaGlobalVariables::global.stoc_queue_depth -= 1;
            nova::NovaGlobalVariables::global.stoc_pending_disk_writes -= size;

            mutex_.lock();
            for (int j = persisted_i; j < writes.size(); j++) {
                NOVA_ASSERT(file_block_offset_.find(writes[j].sstable) != file_block_offset_.end());
                file_block_offset_[writes[j].sstable].persisted = true;
                persisting_cnt -= 1;
            }
            mutex_.unlock();

            mutex_.lock();
            written_mem_blocks_.erase(written_mem_blocks_.begin(),
                                    written_mem_blocks_.begin() + writes.size());
            Seal();
            mutex_.unlock();
            persist_mutex_.unlock();
            return persisted_bytes;
        }else{ // 正常文件只有一个
            uint64_t persisted_bytes = 0;
            leveldb::FileType type = leveldb::FileType::kCurrentFile;
            NOVA_ASSERT(leveldb::ParseFileName(sstable_buf_.filename, &type));
            mutex_.lock();
            NOVA_ASSERT(!sstable_buf_.filename.empty());
            if (sstable_buf_.internal_type == FileInternalType::kFileMetadata) {
                NOVA_ASSERT(
                        file_meta_block_offset_.find(sstable_buf_.filename) ==
                        file_meta_block_offset_.end());
                StoCPersistStatus &s = file_meta_block_offset_[sstable_buf_.filename];
                s.disk_handle.set_offset(current_disk_offset_);
                s.disk_handle.set_size(sstable_buf_.size);
                s.persisted = false;
            } else if (sstable_buf_.internal_type == FileInternalType::kFileParity) {
                NOVA_ASSERT(
                        file_parity_block_offset_.find(sstable_buf_.filename) ==
                        file_parity_block_offset_.end());
                StoCPersistStatus &s = file_parity_block_offset_[sstable_buf_.filename];
                s.disk_handle.set_offset(current_disk_offset_);
                s.disk_handle.set_size(sstable_buf_.size);
                s.persisted = false;
            } else {
                if (type == leveldb::FileType::kTableFile) {
                    NOVA_ASSERT(
                            file_block_offset_.find(sstable_buf_.filename) ==
                            file_block_offset_.end());
                }
                StoCPersistStatus &s = file_block_offset_[sstable_buf_.filename];
                s.disk_handle.set_offset(current_disk_offset_);
                s.disk_handle.set_size(sstable_buf_.size);
                s.persisted = false;
            }
            // persist cnt不用改因为rdma写过来就是写完了
            current_disk_offset_ += sstable_buf_.size;
            NOVA_ASSERT(current_disk_offset_ <= file_size_);
            mutex_.unlock();

            nova::NovaGlobalVariables::global.total_disk_writes += sstable_buf_.size;
            persisted_bytes += sstable_buf_.size;

            mutex_.lock();            
            if (sstable_buf_.internal_type == FileInternalType::kFileMetadata) {
                NOVA_ASSERT(file_meta_block_offset_.find(sstable_buf_.filename) != file_meta_block_offset_.end());
                file_meta_block_offset_[sstable_buf_.filename].persisted = true;
            } else if (sstable_buf_.internal_type == FileInternalType::kFileParity) {
                NOVA_ASSERT(file_parity_block_offset_.find(sstable_buf_.filename) != file_parity_block_offset_.end());
                file_parity_block_offset_[sstable_buf_.filename].persisted = true;
            } else {
                NOVA_ASSERT(file_block_offset_.find(sstable_buf_.filename) != file_block_offset_.end());
                file_block_offset_[sstable_buf_.filename].persisted = true;
            }
            mutex_.unlock();
            return persisted_bytes;
        }
    }

// 删除这个文件
// 只有sstable会调用这个所以根本不用assert
    bool
    StoCPersistentFile::DeleteSSTable(uint32_t given_fileid_for_assertion,
                                      const std::string &filename) {
        NOVA_ASSERT(given_fileid_for_assertion == file_id_)
            << fmt::format("{} {}", given_fileid_for_assertion, file_id_);
        bool delete_file = false;

        mutex_.lock();
        Seal(); // 根本不会进入..
        {
            auto it = file_block_offset_.find(filename);
            if (it != file_block_offset_.end()) {
                NOVA_ASSERT(it->second.persisted);
                int n = file_block_offset_.erase(filename);
                NOVA_ASSERT(n == 1);
            }
        }
        {
            auto it = file_meta_block_offset_.find(filename);
            if (it != file_meta_block_offset_.end()) {
                NOVA_ASSERT(it->second.persisted);
                int n = file_meta_block_offset_.erase(filename);
                NOVA_ASSERT(n == 1);
            }
        }
        {
            auto it = file_parity_block_offset_.find(filename);
            if (it != file_parity_block_offset_.end()) {
                NOVA_ASSERT(it->second.persisted);
                int n = file_parity_block_offset_.erase(filename);
                NOVA_ASSERT(n == 1);
            }
        }
        if (file_block_offset_.empty() && file_meta_block_offset_.empty() && file_parity_block_offset_.empty() &&
            is_full_ &&
            allocated_bufs_.empty() &&
            persisting_cnt == 0 && sealed_) {

            waiting_to_be_deleted = true;
            if (reading_cnt == 0) {
                if (!deleted_) {
                    deleted_ = true;
                    delete_file = true;
                }
            }
        }
        if (delete_file) {
            NOVA_ASSERT(current_disk_offset_ == file_size_);
        }
        mutex_.unlock();

        if (!delete_file) {
            return false;
        }
        NOVA_LOG(rdmaio::DEBUG) << fmt::format("Delete SSTable {} from Stoc File {}.", filename, stoc_file_name_);
        NOVA_ASSERT(file_);
        Status s = file_->Close();
        NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        delete file_;
        file_ = nullptr;
        s = env_->DeleteFile(stoc_file_name_);
        NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        return true;
    }

// 关闭这个文件 只有close和forceseal会将is_full_设置为true
// 并且回收之前分配的资源
// 有些地方会调用 或者没有地方会调用
    void StoCPersistentFile::Close() {
        mutex_.lock();
        NOVA_ASSERT(allocated_bufs_.empty());
        NOVA_ASSERT(persisting_cnt == 0);
        is_full_ = true;
        Seal();

        NOVA_ASSERT(file_);
        Status s = file_->Close();
        NOVA_ASSERT(s.ok()) << fmt::format("{}", s.ToString());
        delete file_;
        file_ = nullptr;
        mutex_.unlock();
    }

// 如果不是manifest的话会直接调用forseal 因为不会再写了
// 有一些地方提前调用了 有可能有问题???
    void StoCPersistentFile::ForceSeal() {
        mutex_.lock();
        NOVA_ASSERT(allocated_bufs_.empty());
        NOVA_ASSERT(persisting_cnt == 0);
        is_full_ = true;
        Seal();
        mutex_.unlock();
    }

// 资源清理 尝试不一定 回收掉之前申请的file buffer的资源 调用的时候带锁
// 只在这个文件里面有调用
    void StoCPersistentFile::Seal() {
        bool seal = false;
        if (allocated_bufs_.empty() && is_full_ && persisting_cnt == 0) {
            if (!sealed_) {
                seal = true;
                sealed_ = true;
            }
        }

        if (seal) {
            NOVA_ASSERT(current_disk_offset_ == file_size_); // 这里是保证该写的都写完了??
        }

        if (!seal) {
            return;
        }

        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format(
                    "StoC file {} closed with t:{} file size {} allocated size {}",
                    file_id_, thread_id_,
                    file_size_, allocated_mem_size_);
        NOVA_ASSERT(backing_mem_);
        if(!is_pm_file_){
             uint32_t scid = mem_manager_->slabclassid(thread_id_,
                                                  allocated_mem_size_);
            mem_manager_->FreeItem(thread_id_, backing_mem_, scid);
            backing_mem_ = nullptr;
        }else{
            mmap_base_ = backing_mem_;
            backing_mem_ = nullptr;
        }
    }

// 直接拿到相应的偏移等 
    BlockHandle
    StoCPersistentFile::Handle(const std::string &filename,
                               FileInternalType internal_type) {
        BlockHandle handle = {};
        while (true) {
            mutex_.lock();
            if (internal_type == FileInternalType::kFileMetadata) {
                auto it = file_meta_block_offset_.find(filename);
                NOVA_ASSERT(it != file_meta_block_offset_.end());
                StoCPersistStatus &s = it->second;
                if (s.persisted) {
                    handle = s.disk_handle;
                    mutex_.unlock();
                    break;
                }
            } else if (internal_type == FileInternalType::kFileParity) {
                auto it = file_parity_block_offset_.find(filename);
                NOVA_ASSERT(it != file_parity_block_offset_.end());
                StoCPersistStatus &s = it->second;
                if (s.persisted) {
                    handle = s.disk_handle;
                    mutex_.unlock();
                    break;
                }
            } else {
                auto it = file_block_offset_.find(filename);
                NOVA_ASSERT(it != file_block_offset_.end());
                StoCPersistStatus &s = it->second;
                if (s.persisted) {
                    handle = s.disk_handle;
                    mutex_.unlock();
                    break;
                }
            }
            mutex_.unlock();
        }

        return handle;
    }

    // 释放掉之前为了存储在cache中所申请的空间
    static void DeleteCachedBlock(const Slice &key, void *value) {
        char *block = reinterpret_cast<char *>(value);
        delete block;
    }

    bool StocPersistentFileManager::ReadDataBlockForReplication(
            const StoCBlockHandle &stoc_block_handle, uint64_t offset,
            uint32_t size, char *scratch, Slice *result) {
        StoCPersistentFile *stoc_file = FindStoCFile(stoc_block_handle.stoc_file_id);
        if (!stoc_file) {
            return false;
        }

        if (!block_cache_) {
            leveldb::FileType type;
            NOVA_ASSERT(ParseFileName(stoc_file->stoc_file_name_, &type));
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("Read {} from stoc file {} offset:{} size:{}",
                               stoc_block_handle.DebugString(),
                               stoc_file->file_id(), offset, size);
            auto status = stoc_file->ReadForReplication(offset, size, scratch, result);
            if (status.IsNotFound()) {
                return false;
            }
            NOVA_ASSERT(status.ok()) << status.ToString();
            NOVA_ASSERT(type == leveldb::FileType::kTableFile);
            NOVA_ASSERT(result->size() == size)
                << fmt::format("fn:{} given size:{} read size:{}",
                               stoc_file->stoc_file_name_,
                               size,
                               result->size());
            NOVA_ASSERT(scratch[size - 1] != 0)
                << fmt::format(
                        "Read {} from stoc file {} offset:{} size:{}",
                        stoc_block_handle.DebugString(),
                        stoc_file->file_id(), offset, size);
            return true;
        }

        char cache_key_buffer[StoCBlockHandle::HandleSize()];
        stoc_block_handle.EncodeHandle(cache_key_buffer);
        Slice key(cache_key_buffer, sizeof(cache_key_buffer));
        auto cache_handle = block_cache_->Lookup(key);
        if (cache_handle != nullptr) {
            auto block = reinterpret_cast<char *>(block_cache_->Value(
                    cache_handle));
            memcpy(scratch, block, stoc_block_handle.size);
        } else {
            stoc_file->Read(offset, size, scratch, result);
            char *block = new char[size];
            memcpy(block, scratch, size);
            cache_handle = block_cache_->Insert(key, block,
                                                size,
                                                &DeleteCachedBlock);
        }
        block_cache_->Release(cache_handle);
        return true;
    }

// stoc读数据块到 目标buf
    void StocPersistentFileManager::ReadDataBlock(
            const leveldb::StoCBlockHandle &stoc_block_handle, uint64_t offset, uint32_t size, char *scratch,
            Slice *result) {
        StoCPersistentFile *stoc_file = FindStoCFile(stoc_block_handle.stoc_file_id);
        NOVA_ASSERT(stoc_file) << stoc_block_handle.stoc_file_id;

        // 如果设置的是没有cache
        if (!block_cache_) {
            leveldb::FileType type;
            NOVA_ASSERT(ParseFileName(stoc_file->stoc_file_name_, &type));
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("Read {} from stoc file {} offset:{} size:{}",
                               stoc_block_handle.DebugString(), stoc_file->file_id(), offset, size);
            NOVA_ASSERT(stoc_file->Read(offset, size, scratch, result).ok());
            if (type == leveldb::FileType::kTableFile) {
                NOVA_ASSERT(result->size() == size)
                    << fmt::format("fn:{} given size:{} read size:{}",
                                   stoc_file->stoc_file_name_, size, result->size());
                NOVA_ASSERT(stoc_file->sealed()) << fmt::format("Read but not sealed {}", stoc_file->stoc_file_name_);
//                NOVA_ASSERT(scratch[size - 1] != 0)
//                    << fmt::format(
//                            "Read {} from stoc file {} offset:{} size:{} result:{}",
//                            stoc_block_handle.DebugString(), stoc_file->file_id(), offset, size, result->size());
            } else {
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("Read file {} read size {}:{}", stoc_file->stoc_file_name_, size,
                                   result->size());
            }
            return;
        }

        char cache_key_buffer[StoCBlockHandle::HandleSize()];
        stoc_block_handle.EncodeHandle(cache_key_buffer);
        Slice key(cache_key_buffer, sizeof(cache_key_buffer));
        auto cache_handle = block_cache_->Lookup(key);

        // 找到了就直接 复制到目标位置
        if (cache_handle != nullptr) {
            auto block = reinterpret_cast<char *>(block_cache_->Value(
                    cache_handle));
            memcpy(scratch, block, stoc_block_handle.size);
        } else {
        // 没找到就读到目标位置 然后把新的插入
            stoc_file->Read(offset, size, scratch, result);
            char *block = new char[size];
            memcpy(block, scratch, size);
            cache_handle = block_cache_->Insert(key, block,
                                                size,
                                                &DeleteCachedBlock);
        }
        block_cache_->Release(cache_handle); //  还要release????
    }

// 传入一个filename -> fileid的集合 打开里面所有的文件
    void StocPersistentFileManager::OpenStoCFiles(
            const std::unordered_map<std::string, uint32_t> &fn_files) {
        mutex_.lock();
        for (const auto &it : fn_files) {
            const auto &fn = it.first;
            const auto &fileid = it.second;
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("Open StoC file {} for file {}", fileid, fn);
            StoCPersistentFile *stoc_file = new StoCPersistentFile(
                    fileid, env_,
                    fn,
                    mem_manager_,
                    0, stoc_file_size_, false);
            stoc_file->ForceSeal();
            NOVA_ASSERT(stoc_files_[fileid] == nullptr)
                << fmt::format("{} {} {}", fileid, it.first,
                               stoc_files_[fileid]->stoc_file_name_);
            stoc_files_[fileid] = stoc_file; //重点是建立file id->file
            fn_stoc_file_map_[fn] = stoc_file; //以及filename->file
            current_stoc_file_id_ = std::max(current_stoc_file_id_, fileid);
        }
        current_stoc_file_id_ += 1;
        mutex_.unlock();
    }

// stoc端打开 stocfile 有就返回 没有新建之后返回
    StoCPersistentFile *
    StocPersistentFileManager::OpenStoCFile(uint32_t thread_id, std::string &filename) {
        mutex_.lock();
        auto stoc_file_ptr = fn_stoc_file_map_.find(filename);
        if (stoc_file_ptr != fn_stoc_file_map_.end()) {
            auto stoc_file = stoc_file_ptr->second;
            mutex_.unlock();
            return stoc_file;
        }
        // not found.
        FileType type;
        NOVA_ASSERT(leveldb::ParseFileName(filename, &type)) << filename;
        uint32_t id = 0;
        if (type == FileType::kDescriptorFile) {
            id = current_manifest_file_stoc_file_id_;
            current_manifest_file_stoc_file_id_ += 1;
            NOVA_LOG(rdmaio::DEBUG) << fmt::format("Open manifest file {} id:{}", filename, id);
            NOVA_ASSERT(
                    current_manifest_file_stoc_file_id_ <= MAX_MANIFEST_FILE_ID) << filename;
        } else {
            id = current_stoc_file_id_;
            current_stoc_file_id_ += 1;
        }
        NOVA_LOG(rdmaio::DEBUG)
            << fmt::format("Create a new stoc file {} for thread {} fn:{}", id,
                           thread_id, filename);
        mutex_.unlock();
        uint32_t file_size = stoc_file_size_;
        if (type == FileType::kDescriptorFile) {
            file_size = nova::NovaConfig::config->manifest_file_size;
        }

        StoCPersistentFile *stoc_file = new StoCPersistentFile(id, env_,
                                                               filename,
                                                               mem_manager_,
                                                               thread_id,
                                                               file_size,
                                                               (type == FileType::kDescriptorFile));
        mutex_.lock();
        NOVA_ASSERT(stoc_files_[id] == nullptr);
        stoc_files_[id] = stoc_file;
        fn_stoc_file_map_[filename] = stoc_file;
        mutex_.unlock();
        return stoc_file;
    }

// 删除这个名字的sstable
    void
    StocPersistentFileManager::DeleteSSTable(const std::string &filename) {
        mutex_.lock();
        auto it = fn_stoc_file_map_.find(filename);
        StoCPersistentFile *stoc_file = nullptr;
        if (it != fn_stoc_file_map_.end()) {
            stoc_file = it->second;
            fn_stoc_file_map_.erase(filename);
        }
        mutex_.unlock();
        if (stoc_file) {
            stoc_file->DeleteSSTable(stoc_file->file_id(), filename);
        }
    }

//找到stocfile
    StoCPersistentFile *
    StocPersistentFileManager::FindStoCFile(uint32_t stoc_file_id) {
        mutex_.lock();
        StoCPersistentFile *stoc_file = stoc_files_[stoc_file_id];
        NOVA_ASSERT(stoc_file) << fmt::format("stoc file {} is null.", stoc_file_id);
        NOVA_ASSERT(stoc_file->file_id() == stoc_file_id)
            << fmt::format("stoc file {} {}.", stoc_file->file_id(), stoc_file_id);
        mutex_.unlock();
        return stoc_file;
    }

//建立stoc的持久介质中的文件的管理器
    StocPersistentFileManager::StocPersistentFileManager(
            leveldb::Env *env,
            leveldb::MemManager *mem_manager,
            const std::string &stoc_file_path,
            uint32_t stoc_file_size) :
            env_(env), mem_manager_(mem_manager),
            stoc_file_path_(stoc_file_path),
            stoc_file_size_(stoc_file_size) {
    }
}
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>
#include "leveldb/db_types.h"

namespace leveldb {

    class Arena {
    public:
        Arena();

        Arena(char* buf, uint32_t scid, uint64_t size, MemManager* mem_manager, uint32_t db_index);

        Arena(const Arena &) = delete;

        Arena &operator=(const Arena &) = delete;

        ~Arena();

        void Set(char* buf, uint32_t scid, uint64_t size, MemManager* mem_manager, uint32_t db_index);

        // Return a pointer to a newly allocated memory block of "bytes" bytes.
        char* Allocate(size_t bytes);

        // Allocate memory with the normal alignment guarantees provided by malloc.
        char* AllocateAligned(size_t bytes);

        // Returns an estimate of the total memory usage of data allocated
        // by the arena.
        size_t MemoryUsage() const {
            return memory_usage_;
        }

        char* Buf() const {
            return buf_;
        }

    // private:
        // char *AllocateFallback(size_t bytes);

        // char *AllocateNewBlock(size_t block_bytes);

        char* buf_ = nullptr; // 起始

        uint32_t scid_ = 0; // 这个其实不用存

        uint64_t size_ = 0; // 这个也不大用        

        // Allocation state
        char *alloc_ptr_ = nullptr;
        // size_t alloc_bytes_remaining_ = 0;

        // Array of new[] allocated memory blocks
        // std::vector<char *> blocks_;

        // Total memory usage of the arena.
        //
        // TODO(costan): This member is accessed via atomics, but the others are
        //               accessed without any locking. Is this OK?
        size_t memory_usage_ = 0;

        MemManager* mem_manager_ = nullptr;
        uint32_t dbindex_ = 0;
    };

    // 改为分配指针 只有l0读和压缩的时候做差 memtable和原来基本相同

    inline char* Arena::Allocate(size_t bytes) {
        // The semantics of what to return are a bit messy if we allow
        // 0-byte allocations, so we disallow them here (we don't need
        // them for our internal use).
        assert(bytes > 0);
        // if (bytes <= alloc_bytes_remaining_) {
        //     char *result = alloc_ptr_;
        //     alloc_ptr_ += bytes;
        //     alloc_bytes_remaining_ -= bytes;
        //     return result;
        // }
        // return AllocateFallback(bytes);
        assert(alloc_ptr_ + bytes <= buf_ + size_);
        char* result = alloc_ptr_;
        alloc_ptr_ += bytes;
        memory_usage_ += bytes;
        return result;        
    }

    inline char* Arena::AllocateAligned(size_t bytes){
        const int align = (sizeof(void *) > 8) ? sizeof(void *) : 8; // 8字节对齐
        assert((align & (align - 1)) == 0);
        size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1); // 目前这个偏移比8字节多的字节数
        size_t slop = (current_mod == 0 ? 0 : align - current_mod); // 应该补的字节数
        size_t needed = bytes + slop;

        assert(alloc_ptr_ + needed <= buf_ + size_);

        char* result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        memory_usage_ += needed;
        return result;        
    }

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_

#ifndef STORAGE_LEVELDB_UTIL_PMARENA_H_
#define STORAGE_LEVELDB_UTIL_PMARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb{
    
    class PMArena{
    public:
        // 已经分配好的buf地址和对应的scid
        PMArena();

        PMArena(char* buf, uint32_t scid, uint64_t size);

        PMArena(const PMArena&) = delete;

        PMArena& operator=(const PMArena&) = delete;

        ~PMArena();

        void Set(char* buf, uint32_t scid, uint64_t size);

        // 用偏移来进行分配
        uint64_t Allocate(size_t bytes);

        uint64_t AllocateAligned(size_t bytes);

        // 有一个专门的域用来记录内存使用量
        size_t MemoryUsage() const {
            return memory_usage_;
        }

        char* Buf() const {
            return buf_;
        }

    // private:
        // 只需要上面两个就可以
        // uint64_t AllocateFallback(size_t bytes);
        
        // uint64_t AllocateNewBlock(size_t block_bytes);

        char* buf_ = nullptr; // 起始

        uint32_t scid_ = 0; // 这个其实不用存

        uint64_t size_ = 0; // 这个也不大用

        char* alloc_ptr_ = nullptr; // 目前分配到哪里了

        // size_t alloc_bytes_remaining_ = 0;

        // std::vector<char*> blocks;

        // alloc_ptr和buf_做差即可
        size_t memory_usage_ = 0;
    };

    inline uint64_t PMArena::Allocate(size_t bytes) {
        assert(bytes > 0);
        // 确保大小不超过
        assert(alloc_ptr_ + bytes <= buf_ + size_);
        uint64_t result = alloc_ptr_ - buf_;
        alloc_ptr_ += bytes;
        memory_usage_ += bytes;
        return result;
    }

    // 使分配的这个东西起始地址处于一个对齐的地址
    inline uint64_t PMArena::AllocateAligned(size_t bytes){
        const int align = (sizeof(void *) > 8) ? sizeof(void *) : 8; // 8字节对齐
        assert((align & (align - 1)) == 0);
        size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1); // 目前这个偏移比8字节多的字节数
        size_t slop = (current_mod == 0 ? 0 : align - current_mod); // 应该补的字节数
        size_t needed = bytes + slop;

        assert(alloc_ptr_ + needed <= buf_ + size_);

        uint64_t result = (alloc_ptr_ + slop) - buf_;
        alloc_ptr_ += needed;
        memory_usage_ += needed;
        return result;
    }
}


#endif  // STORAGE_LEVELDB_UTIL_PMARENA_H_


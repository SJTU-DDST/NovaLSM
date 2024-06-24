#include "util/pmarena.h"

namespace leveldb{
    static const int kBlockSize = 4096;

    PMArena::PMArena():
        buf_(nullptr),
        scid_(0),
        size_(0),
        alloc_ptr_(nullptr),
        memory_usage_(0)
    {

    }

    PMArena::PMArena(char* buf, uint32_t scid, uint64_t size):
        buf_(buf),
        scid_(scid),
        size_(size),
        alloc_ptr_(buf),
        memory_usage_(0)
    {

    }

    PMArena::~PMArena(){
        // 原来是在这里销毁的， emmm
    }

    void PMArena::Set(char* buf, uint32_t scid, uint64_t size){
        buf_ = buf;
        scid_ = scid;
        size = size_;
        alloc_ptr_ = buf;
    }

}
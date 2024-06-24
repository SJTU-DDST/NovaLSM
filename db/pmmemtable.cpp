#include <leveldb/db_profiler.h>
#include <common/nova_console_logging.h>
#include <fmt/core.h>
#include "common/nova_config.h"
#include "db/pmmemtable.h" // pmmemtable
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb{

    // 长度为 variint32前置的slice
    static Slice GetLengthPrefixedSlice(const char *data) {
        uint32_t len;
        const char *p = data;
        p = GetVarint32Ptr(p, p + 5,
                           &len);  // +5: we assume "p" is not corrupted
        return Slice(p, len);
    }

    PMMemTable::PMMemTable(const InternalKeyComparator &comparator,
                           uint32_t memtable_id,
                           DBProfiler *db_profiler,
                           bool is_ready,
                        //    char* buf,
                        //    uint32_t scid, 
                        //    uint64_t size)
                           MemManager* mem_manager,
                           uint32_t db_index)
                : comparator_(comparator), memtable_id_(memtable_id),
                // pmarena_(buf, scid, size), 
                pmtable_(comparator_, &pmarena_),
                db_profiler_(db_profiler), is_ready_(is_ready),is_ready_signal_(&is_ready_mutex_),
                mem_manager_(mem_manager_), db_index_(db_index), size_(16 * 1024 * 1024){
                    scid_ = mem_manager_->slabclassid(db_index_, size_); // 这里size设为多大??? 这里一般是搞成16mb了
                    buf_ = mem_manager_->ItemAlloc(db_index_, scid_);
                    pmarena_.Set(buf_, scid_, size_);
                    pmtable_.init();
                }

    void PMMemTable::WaitUntilReady() {
        if (nova::NovaConfig::config->cfgs.size() == 1 || is_ready_) {
            return;
        }
        is_ready_mutex_.Lock();
        while (!is_ready_) {
            is_ready_signal_.Wait();
        }
        is_ready_mutex_.Unlock();
    }    

    void PMMemTable::SetReadyToProcessRequests() {
        is_ready_mutex_.Lock();
        is_ready_ = true;
        is_ready_signal_.SignalAll();
        is_ready_mutex_.Unlock();
    }

    PMMemTable::~PMMemTable() { assert(refs_ == 0); }    

    size_t PMMemTable::ApproximateMemoryUsage() { return pmarena_.MemoryUsage(); }    

    int PMMemTable::KeyComparator::operator()(const char* aptr,
                                            const char *bptr) const {

        Slice a = GetLengthPrefixedSlice(aptr);
        Slice b = GetLengthPrefixedSlice(bptr);
        return comparator.Compare(a, b);
    }

    static const char* EncodeKey(std::string *scratch, const Slice &target){
        scratch->clear();
        PutVarint32(scratch, target.size());
        scratch->append(target.data(), target.size());
        return scratch->data();
    }

// 这里和pmmemtable尽量保证接口不变，只把接口的改变放在skiplist，和更上层（申请空间那些）
    class PMMemTableIterator : public Iterator {
    public:
        explicit PMMemTableIterator(PMMemTable *pmtable, TraceType trace_type,
                                    AccessCaller caller, uint32_t sample_size):
                                    iter_(&(pmtable->pmtable_), sample_size), trace_type_(trace_type),
                                    caller_(caller)
                                    {
                                        if (db_profiler_ != nullptr) {
                                            Access access = {
                                                    .trace_type = trace_type_,
                                                    .access_caller = caller_,
                                                    .block_id = 0,
                                                    .sstable_id = 0,
                                                    .level = 0,
                                                    .size = 0
                                            };
                                            db_profiler_->Trace(access);
                                        }
                                    }

        PMMemTableIterator(const PMMemTableIterator &) = delete;

        PMMemTableIterator &operator=(const PMMemTableIterator &) = delete;

        ~PMMemTableIterator() override = default;

        bool Valid() const override {return iter_.Valid(); };

        void Seek(const Slice &k) override {
            iter_.Seek(EncodeKey(&tmp_, k)); // 来自外面的地址
            seeked_ = true;
        }

        void SeekToFirst() override {
            iter_.SeekToFirst();
            seeked_ = true;
        }

        void SeekToLast() override {
            iter_.SeekToLast();
            seeked_ = true;
        }

// ??
        void SkipToNextUserKey(const Slice &target) override {
            if(!seeked_){
                Seek(target);
            }
            auto userkey = ExtractUserKey(target);
            uint64_t userkeyint;
            nova::str_to_int(userkey.data(), &userkeyint, userkey.size()); // 提取出基准的userkey
            while (Valid()) {
                auto current_key = ExtractUserKey(key());
                uint64_t pivot = 0;
                nova::str_to_int(current_key.data(), &pivot,
                                 current_key.size());
                NOVA_LOG(rdmaio::DEBUG)
                    << fmt::format("memtable skip:{} {}", userkeyint, pivot);
                if (userkeyint != pivot) {
                    return;
                }
                Next();
            }
        }

    void Next() override {
        iter_.Next();
    }

    void Prev() override {
        iter_.Prev();
    }

    Slice key() const override {
        return GetLengthPrefixedSlice(iter_.key());
    }

    Slice value() const override {
        Slice key_slice = GetLengthPrefixedSlice(iter_.key());
        return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
    }

    Status status() const override { return Status::OK(); }

    private:
        DBProfiler *db_profiler_ = nullptr;
        TraceType trace_type_;
        AccessCaller caller_;
        PMMemTable::PMTable::Iterator iter_;
        bool seeked_ = false;
        std::string tmp_;
    };

    Iterator *PMMemTable::NewIterator(TraceType trace_type,
                                    AccessCaller caller,
                                    uint32_t sample_size) {
        WaitUntilReady();
        return new PMMemTableIterator(this, trace_type, caller, sample_size);
    }

    void PMMemTable::Add(SequenceNumber s, ValueType type, const Slice &key,
                       const Slice &value) {
        // Format of an entry is concatenation of:
        //  key_size     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]
        size_t key_size = key.size();
        size_t val_size = value.size();
        size_t internal_key_size = key_size + 8;
        const size_t encoded_len = VarintLength(internal_key_size) +
                                   internal_key_size + VarintLength(val_size) +
                                   val_size;
        
        uint64_t buf_offset = pmarena_.Allocate(encoded_len);
        char* p = EncodeVarint32(buf_ + buf_offset, internal_key_size); // 换成offset

        // char *buf = arena_.Allocate(encoded_len);
        // char *p = EncodeVarint32(buf, internal_key_size);
        memcpy(p, key.data(), key_size);
        p += key_size;
        EncodeFixed64(p, (s << 8) | type);
        p += 8;
        p = EncodeVarint32(p, val_size);
        memcpy(p, value.data(), val_size);
        
        assert(p + val_size == buf_ + buf_offset + encoded_len); //
        
        pmtable_.Insert(buf_ + buf_offset);
    } 

    bool PMMemTable::Get(const LookupKey &key, std::string *value, Status *s) {
        WaitUntilReady();
        Slice memkey = key.memtable_key();
        PMTable::Iterator iter(&pmtable_);
        iter.Seek(memkey.data());
        if (iter.Valid()) {
            // entry format is:
            //    klength  varint32          5
            //    userkey  char[klength]     8
            //    tag      uint64            8
            //    vlength  varint32          5
            //    value    char[vlength]     1024
            // Check that it belongs to same user key.  We do not check the
            // sequence number since the Seek() call above should have skipped
            // all entries with overly large sequence numbers.
            const char *entry = iter.key();
            uint32_t key_length;
            const char *key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length); // 读出长度并且key_pt指向userkey 这里的key length 包括了tag 也就是类型和序列号的结合
            if (comparator_.comparator.user_comparator()->Compare(
                    Slice(key_ptr, key_length - 8), key.user_key()) == 0) { // 对比user key
                // Correct user key
                const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8); //
                switch (static_cast<ValueType>(tag & 0xff)) {
                    case kTypeValue: {
                        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
                        value->assign(v.data(), v.size());
                        return true;
                    }
                    case kTypeDeletion:
                        *s = Status::NotFound(Slice());
                        return true;
                }
            }
        }
        return false;
    }

}
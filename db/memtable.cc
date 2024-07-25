// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <leveldb/db_profiler.h>
#include <common/nova_console_logging.h>
#include <fmt/core.h>
#include "common/nova_config.h"
#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "common/city_hash.h"

namespace leveldb {

    static Slice GetLengthPrefixedSlice(const char *data) {
        uint32_t len;
        const char *p = data;
        p = GetVarint32Ptr(p, p + 5,
                           &len);  // +5: we assume "p" is not corrupted
        return Slice(p, len);
    }

    MemTable::MemTable(const InternalKeyComparator &comparator, // internalcomparator 用于初始化comparator
                       uint32_t memtable_id,
                       DBProfiler *db_profiler,
                       bool is_ready, 
                       MemManager* mem_manager,
                       uint32_t db_index)
            : comparator_(comparator), memtable_id_(memtable_id), refs_(0),
              table_(comparator_, &arena_),
              db_profiler_(db_profiler), is_ready_(is_ready),
              is_ready_signal_(&is_ready_mutex_),
              mem_manager_(mem_manager),
              db_index_(db_index),
              size_(18 * 1024 * 1024) { // 数字还是要查一下 可能是这里的问题
        scid_ = mem_manager_->slabclassid(db_index_, size_); // 这里size设为多大??? 这里一般是搞成16mb了
        buf_ = mem_manager_->ItemAlloc(db_index_, scid_);
        arena_.Set(buf_, scid_, size_, mem_manager_, db_index_);
        table_.init();
    }

// 一个config永远ready
    void MemTable::WaitUntilReady() {
        if (nova::NovaConfig::config->cfgs.size() == 1 || is_ready_) {
            return;
        }
        is_ready_mutex_.Lock();
        while (!is_ready_) {
            is_ready_signal_.Wait();
        }
        is_ready_mutex_.Unlock();
    }

    void MemTable::SetReadyToProcessRequests() {
        is_ready_mutex_.Lock();
        is_ready_ = true;
        is_ready_signal_.SignalAll();
        is_ready_mutex_.Unlock();
    }

// 加到这个generation的
    void MemTablePartition::AddMemTable(uint64_t generation_id, uint32_t memtableid) {
        NOVA_LOG(rdmaio::DEBUG) << fmt::format("Add memtable {}:{}", generation_id, memtableid);
        auto it = generation_num_memtables_.find(generation_id);
        generation_num_memtables_[generation_id].insert(memtableid);
    }

// 从generation中删掉
    void MemTablePartition::RemoveMemTable(uint64_t generation_id, uint32_t memtableid) {
        NOVA_LOG(rdmaio::DEBUG) << fmt::format("Remove memtable {}:{}", generation_id, memtableid);
        auto it = generation_num_memtables_.find(generation_id);
        NOVA_ASSERT(it != generation_num_memtables_.end());
        it->second.erase(memtableid);
        if (it->second.empty()) {
            generation_num_memtables_.erase(generation_id);
        }
    }

    MemTable::~MemTable() { assert(refs_ == 0); }

    size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

// keycomparator的比较
// lookupkey和里面的key前面格式相同
    int MemTable::KeyComparator::operator()(const char *aptr,
                                            const char *bptr) const {
        // Internal keys are encoded as length-prefixed strings.
        Slice a = GetLengthPrefixedSlice(aptr);
        Slice b = GetLengthPrefixedSlice(bptr);
        return comparator.Compare(a, b);
    }

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
    static const char *EncodeKey(std::string *scratch, const Slice &target) {
        scratch->clear();
        PutVarint32(scratch, target.size());
        scratch->append(target.data(), target.size());
        return scratch->data();
    }

// 专注于这个提供的接口!!!!!
    class MemTableIterator : public Iterator {
    public:
        explicit MemTableIterator(MemTable *table, TraceType trace_type,
                                  AccessCaller caller, uint32_t sample_size)
                // 去掉sample_size
                : iter_(
                &(table->table_)), trace_type_(trace_type),
                  caller_(caller) {
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

        MemTableIterator(const MemTableIterator &) = delete;

        MemTableIterator &operator=(const MemTableIterator &) = delete;

        ~MemTableIterator() override = default;

        bool Valid() const override { return iter_.Valid(); }

        void Seek(const Slice &k) override {
            iter_.Seek(EncodeKey(&tmp_, k));
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

        void SkipToNextUserKey(const Slice &target) override {
            if (!seeked_) {
                Seek(target);
            }
            auto userkey = ExtractUserKey(target);
            uint64_t userkeyint;
            nova::str_to_int(userkey.data(), &userkeyint, userkey.size());
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

        void Prev() override { iter_.Prev(); }

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
        MemTable::Table::Iterator iter_;
        bool seeked_ = false;
        std::string tmp_;  // For passing to EncodeKey
    };

    Iterator *MemTable::NewIterator(TraceType trace_type,
                                    AccessCaller caller,
                                    uint32_t sample_size) {
        WaitUntilReady();
        return new MemTableIterator(this, trace_type, caller, sample_size);
    }

// 这里的key和value是最原始的
    void MemTable::Add(SequenceNumber s, ValueType type, const Slice &key,
                       const Slice &value) {
        // Format of an entry is concatenation of:
        //  key_size     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]    ->这个里面包括原本的user_key和tag和seq的东西
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]
        size_t key_size = key.size();
        size_t val_size = value.size();
        size_t internal_key_size = key_size + 8;
        const size_t encoded_len = VarintLength(internal_key_size) +
                                   internal_key_size + VarintLength(val_size) +
                                   val_size;
        char *buf = arena_.Allocate(encoded_len);
        char *p = EncodeVarint32(buf, internal_key_size);
        // uint64_t buf_offset = arena_.Allocate(encoded_len);
        // char *p = EncodeVarint32(buf_ + buf_offset, internal_key_size);
        
        uint64_t hash = nova::CityHash64(key.data(), key.size());
        // NOVA_LOG(rdmaio::INFO) << "put key: " << std::string(key.data(), key.size()) << " " << "hash: " << hash;


        memcpy(p, key.data(), key_size);
        p += key_size;
        EncodeFixed64(p, (s << 8) | type);
        p += 8;
        p = EncodeVarint32(p, val_size);
        memcpy(p, value.data(), val_size);


        assert(p + val_size == buf + encoded_len);
        table_.Insert(buf, hash);
    }


    bool MemTable::Get(const LookupKey &key, std::string *value, Status *s) {
        WaitUntilReady();
        Slice memkey = key.memtable_key(); // 整个 包括长度 userkey和 sequence和tag的结合体

        // 这里直接get到
        // 找到第一个>next的
        uint64_t hash = nova::cityhash(key.user_key().data(), key.user_key().size());
        // NOVA_LOG(rdmaio::INFO) << "get key: " << std::string(key.user_key().data(), key.user_key().size()) << " " << "hash: " << hash;
        // char* found_key = nullptr;
        bool found = false;
        auto found_key = table_.Get(memkey.data(), hash, &found);
        if(found){
            uint32_t key_length;
            const char *key_ptr = GetVarint32Ptr(found_key, found_key + 5, &key_length);
            if (comparator_.comparator.user_comparator()->Compare(
                    Slice(key_ptr, key_length - 8), key.user_key()) == 0) { // 对比user key
                // Correct user key
                const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8); // 去看tag
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
        }else{
            return false; // 代表没有找到
        }



        // Table::Iterator iter(&table_);
        // iter.Seek(memkey.data()); // 这里到底是怎么找的
        // if (iter.Valid()) { // 这里也使用了iter 
        //     // entry format is:
        //     //    klength  varint32          5
        //     //    userkey  char[klength]     8
        //     //    tag      uint64            8
        //     //    vlength  varint32          5
        //     //    value    char[vlength]     1024
        //     // Check that it belongs to same user key.  We do not check the
        //     // sequence number since the Seek() call above should have skipped
        //     // all entries with overly large sequence numbers.
        //     const char *entry = iter.key();
        //     uint32_t key_length;
        //     const char *key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length); // 读出长度并且key_pt指向userkey 这里的key length 包括了tag 也就是类型和序列号的结合
        //     if (comparator_.comparator.user_comparator()->Compare(
        //             Slice(key_ptr, key_length - 8), key.user_key()) == 0) { // 对比user key
        //         // Correct user key
        //         const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8); // 去看tag
        //         switch (static_cast<ValueType>(tag & 0xff)) {
        //             case kTypeValue: {
        //                 Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
        //                 value->assign(v.data(), v.size());
        //                 return true;
        //             }
        //             case kTypeDeletion:
        //                 *s = Status::NotFound(Slice());
        //                 return true;
        //         }
        //     }
        // }
        // return false;
    }

    AtomicMemTable::AtomicMemTable(MemManager* mem_manager, uint32_t dbid):
                                                                        mem_manager_(mem_manager),
                                                                        dbid_(dbid){}

    void AtomicMemTable::SetMemTable(uint64_t generation_id, leveldb::MemTable *mem) {
        mutex_.lock();
        l0_file_numbers_.clear();
        is_flushed_ = false;
        is_immutable_ = false;
        is_scheduled_for_flushing = false;
        generation_id_ = generation_id;
        memtable_id_ = mem->memtableid();

//        NOVA_ASSERT(!memtable_);
        memtable_ = mem;
        memtable_->Ref();
        mutex_.unlock();
    }

// 压缩后 表明
    void AtomicMemTable::SetFlushed(const std::string &dbname,
                                    const std::vector<uint64_t> &l0_file_numbers,
                                    uint32_t version_id) {
        mutex_.lock();
        NOVA_ASSERT(!is_flushed_);
        NOVA_ASSERT(l0_file_numbers_.empty());
        NOVA_ASSERT(is_immutable_);
        last_version_id_ = std::max(last_version_id_, version_id);
        l0_file_numbers_.insert(l0_file_numbers.begin(), l0_file_numbers.end());
        is_flushed_ = true;
        NOVA_ASSERT(memtable_)
            << fmt::format("{}:{}:{}", memtable_id_, l0_file_numbers.empty() ? 0 : l0_file_numbers[0], version_id);
        uint32_t mid = memtable_->memtableid();
        uint32_t refs = memtable_->Unref();
        if (refs <= 0) {
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("flush delete db-{} mid-{}", dbname, mid);
            delete memtable_;
            memtable_ = nullptr;
        }
        mutex_.unlock();
    }

    std::string MemTableL0FilesEdit::DebugString() const {
        std::string added;
        std::string removed;
        for (auto add : add_fns) {
            added += std::to_string(add);
            added += ",";
        }
        for (auto rm : remove_fns) {
            removed += std::to_string(rm);
            removed += ",";
        }
        std::string output = fmt::format("dbid: {} add:[{}] rm:[{}]", dbid_,
                                         added,
                                         removed);
        return output;
    }

    AtomicMemTable *AtomicMemTable::RefMemTable() {
        AtomicMemTable *mem = nullptr;
        mutex_.lock();
        if (memtable_ != nullptr) {
            memtable_->Ref();
            mem = this;
            mutex_.unlock();
            return mem;
        }
        mutex_.unlock();
        return mem;
    }

    uint32_t AtomicMemTable::Encode(char *buf) {
        uint32_t msg_size = 0;
        msg_size += EncodeBool(buf + msg_size, is_immutable_);
        msg_size += EncodeBool(buf + msg_size, is_flushed_);
        msg_size += EncodeFixed32(buf + msg_size, last_version_id_);
        msg_size += EncodeFixed32(buf + msg_size, memtable_id_);
        msg_size += EncodeFixed32(buf + msg_size, nentries_);
        msg_size += EncodeFixed32(buf + msg_size, memtable_size_);
        msg_size += EncodeFixed32(buf + msg_size, l0_file_numbers_.size());
        for (auto tableid : l0_file_numbers_) {
            msg_size += EncodeFixed64(buf + msg_size, tableid);
        }
        return msg_size;
    }

    bool AtomicMemTable::Decode(Slice *buf, const InternalKeyComparator &cmp) {
        NOVA_ASSERT(DecodeBool(buf, &is_immutable_));
        NOVA_ASSERT(DecodeBool(buf, &is_flushed_));
        NOVA_ASSERT(DecodeFixed32(buf, &last_version_id_));
        NOVA_ASSERT(DecodeFixed32(buf, &memtable_id_));
        uint32_t nentries = 0;
        NOVA_ASSERT(DecodeFixed32(buf, &nentries));
        nentries_ = nentries;
        NOVA_ASSERT(DecodeFixed32(buf, &memtable_size_));
        uint32_t size;
        NOVA_ASSERT(DecodeFixed32(buf, &size));
        for (int i = 0; i < size; i++) {
            uint64_t l0;
            NOVA_ASSERT(DecodeFixed64(buf, &l0));
            l0_file_numbers_.insert(l0);
        }

        bool memtable_exists = true;
        if (!is_flushed_ && !memtable_) {
            memtable_ = new MemTable(cmp, memtable_id_, nullptr, false, mem_manager_, dbid_);
            memtable_exists = false;
        }
        return memtable_exists;
    }

// 该加的加 该删的删
    void AtomicMemTable::UpdateL0Files(uint32_t version_id, const MemTableL0FilesEdit &edit) {
        mutex_.lock();
        NOVA_ASSERT(is_immutable_) << fmt::format("{}:{},{}", memtable_id_, memtable_size_, edit.DebugString());
        NOVA_ASSERT(is_flushed_);
        last_version_id_ = std::max(last_version_id_, version_id);
        for (auto add : edit.add_fns) {
            l0_file_numbers_.insert(add);
        }
        for (auto rm : edit.remove_fns) {
            l0_file_numbers_.erase(rm);
        }
        mutex_.unlock();
    }

// done
    void AtomicMemTable::Unref(const std::string &dbname, uint32_t unrefcount) {
        mutex_.lock();
        NOVA_ASSERT(memtable_);
        uint32_t mid = memtable_->memtableid();
        uint32_t refs = memtable_->Unref(unrefcount);
        if (refs == 0) {
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("unref delete db-{} mid-{}", dbname, mid);
            delete memtable_;
            memtable_ = nullptr;
        }
        mutex_.unlock();
    }

    std::string MemTablePartition::DebugString() const {
        std::string msg = fmt::format("p-{}:", partition_id);
        for (auto gen : generation_num_memtables_) {
            std::string m;
            for (auto id : gen.second) {
                m += fmt::format(",{}", id);
            }
            msg += fmt::format("gen-{}:{},", gen.first, m);
        }
        msg += "\n";
        msg += "immutable slots:";
        for (auto slots : immutable_memtable_ids) {
            msg += fmt::format("{},", slots);
        }
        msg += "\n";
        msg += "slot-id-map:";
        for (auto slots : slot_imm_id) {
            msg += fmt::format("s-{}:{},", slots.first, slots.second);
        }
        return msg;
    }
}  // namespace leveldb

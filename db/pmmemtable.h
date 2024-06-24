#ifndef STORAGE_LEVELDB_DB_PMMEMTABLE_H_
#define STORAGE_LEVELDB_DB_PMMEMTABLE_H_

#include <string>
#include <set>
#include <queue>

#include "leveldb/db_profiler.h"
#include "db/dbformat.h"
#include "db/pmskiplist.h"
#include "leveldb/db.h"
#include "leveldb/stoc_client.h"
#include "db/memtable.h"
#include "util/pmarena.h"

namespace leveldb{
    class InternalKeyComparator;
    
    class PMMemTableIterator;

    class PMMemTable{
    public:
        explicit PMMemTable(const InternalKeyComparator& comparator,
                          uint32_t memtable_id,
                          DBProfiler *db_profile,
                          bool is_ready,
                          //char* buf, // 预先分配比较好 可以考虑将mem——manager传入
                          //uint32_t scid,
                          //uint32_t size);
                          MemManager* mem_manager,
                          uint32_t db_index);
        
        PMMemTable(const PMMemTable &) = delete;

        PMMemTable& operator=(const PMMemTable &) = delete;

        void Ref() {
            ++refs_;
        }

        uint32_t memtableid() {
            return memtable_id_;
        }

        void SetReadyToProcessRequests();

        uint32_t Unref(uint32_t unrefcount = 1) {
            refs_ -= unrefcount;
            uint32_t refs = refs_;
            assert(refs_ >= 0);
            return refs;
        }

        size_t ApproximateMemoryUsage();

        Iterator *NewIterator(TraceType trace_type, AccessCaller caller,
                              uint32_t sample_size = 0);

        void Add(SequenceNumber seq, ValueType type, const Slice &key,
                 const Slice &value);

        bool Get(const LookupKey &key, std::string *value, Status *s);

        FileMetaData &meta() {
            return flushed_meta_;
        }

        ~PMMemTable();

        // 有一个专门用来存链表的数据的地方
        // 其余的都是那种管理的数据
        // 这里全是log相关的 读出来的时候完全可以不管

        // 管理
        std::mutex mu_;
        std::vector<leveldb::LevelDBLogRecord> log_records_;
        uint32_t log_records_size_ = 0;
        uint32_t current_log_size_ = 0;

        bool is_pinned_ = false;
    
        struct KeyComparator {
            const InternalKeyComparator comparator;

            explicit KeyComparator(const InternalKeyComparator &c) : comparator(
                    c) {}

            int operator()(const char *a, const char *b) const;
        };    
    
    private:
        void WaitUntilReady();

        friend class PMMemTableIterator;

        friend class PMMemTableBackwardIterator;

        // 应该修改???

        // 管理
        std::atomic_bool is_ready_;
        port::Mutex is_ready_mutex_;
        port::CondVar is_ready_signal_;

        // 数据!!! ??? 应该是什么类型??
        typedef PMSkipList<const char *, KeyComparator> PMTable; // arena与之对应，只有这个放入~
        PMArena pmarena_;
        PMTable pmtable_;

        DBProfiler *db_profiler_ = nullptr;
        KeyComparator comparator_;
        int refs_ = 0;
        uint32_t memtable_id_ = 0;
        FileMetaData flushed_meta_;

        MemManager *mem_manager_; // 负责分配和回收空间的
        uint32_t db_index_; // 负责存储这个是第几个db对应的index
        char* buf_;
        uint32_t scid_;
        uint64_t size_; // 16mb大小
    };
}


#endif  // STORAGE_LEVELDB_DB_PMMEMTABLE_H_
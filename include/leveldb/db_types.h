
//
// Created by Haoyu Huang on 1/11/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_DB_TYPES_H
#define LEVELDB_DB_TYPES_H

#include <stdint.h>
#include <stdio.h>
#include <string>
#include <vector>
#include <map>
#include <mutex>

#include "slice.h"

namespace leveldb {
    class RTableHandle {
    public:
        uint32_t server_id = 0;
        uint32_t rtable_id = 0;
        uint64_t offset = 0;
        uint32_t size = 0;

        static int HandleSize() {
            return 4 + 4 + 8 + 4;
        }

        void EncodeHandle(char *buf);

        void DecodeHandle(const char *buf);
    };

    struct SSTableRTablePair {
        std::string sstable_id;
        uint32_t rtable_id;
    };

    typedef uint64_t SequenceNumber;

    // Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
    enum ValueType {
        kTypeDeletion = 0x0, kTypeValue = 0x1
    };

    struct ParsedInternalKey {
        Slice user_key;
        SequenceNumber sequence;
        ValueType type;

        ParsedInternalKey() {}  // Intentionally left uninitialized (for speed)
        ParsedInternalKey(const Slice &u, const SequenceNumber &seq,
                          ValueType t)
                : user_key(u), sequence(seq), type(t) {}

        std::string DebugString() const;
    };

    // Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
    class InternalKey {
    private:
        std::string rep_;

    public:
        InternalKey() {}  // Leave rep_ as empty to indicate it is invalid
        InternalKey(const Slice &user_key, SequenceNumber s, ValueType t);

        bool DecodeFrom(const Slice &s, bool copy = false);

        Slice Encode() const {
            assert(!rep_.empty());
            return rep_;
        }

        Slice user_key() const;

        void SetFrom(const ParsedInternalKey &p);

        void Clear();

        std::string DebugString() const;
    };

    enum FileCompactionStatus {
        NONE = 0,
        COMPACTING = 1,
        COMPACTED = 2
    };

    struct DeletedFileIdentifier {
        DeletedFileIdentifier() {}

        uint32_t memtable_id = 0;
        uint64_t fnumber = 0;
    };

    struct FileMetaData {
        FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0),
                         converted_file_size(0),
                         compaction_status(FileCompactionStatus::NONE) {}

        int refs;
        int allowed_seeks;  // Seeks allowed until compaction
        uint32_t memtable_id = 0;
        uint64_t number;
        uint64_t file_size;    // File size in bytes in original SSTable format.
        uint64_t converted_file_size; // File size in bytes after converted to RTable.
        InternalKey smallest;  // Smallest internal key served by table
        InternalKey largest;   // Largest internal key served by table
        FileCompactionStatus compaction_status;

        std::vector<RTableHandle> data_block_group_handles;
    };

    uint32_t
    EncodeFileMetaData(const FileMetaData &meta, char *buf, uint32_t buf_size);

    void DecodeFileMetaData(const Slice &s, FileMetaData *meta);

    class LEVELDB_EXPORT MemManager {
    public:
        virtual char *ItemAlloc(uint64_t key, uint32_t scid) = 0;

        virtual void FreeItem(uint64_t key, char *buf, uint32_t scid) = 0;

        virtual void
        FreeItems(uint64_t key, const std::vector<char *> &items,
                  uint32_t scid) = 0;

        virtual uint32_t slabclassid(uint64_t key, uint64_t size) = 0;
    };

    class CCServer {
    public:
        virtual int PullAsyncCQ() = 0;
    };

    class WBTable {
    public:
        uint64_t thread_id;
        char *backing_mem;
        uint64_t used_size;
        uint64_t allocated_size;

        void Ref();

        void Unref();

        bool is_deleted();

        void Delete();

    private:
        MemManager *mem_manager_;
        int refcount = 0;
        bool deleted = false;

        std::mutex mutex_;
    };

    class SSTableManager {
    public:
        virtual void AddSSTable(const std::string &dbname, uint64_t file_number,
                                uint64_t thread_id,
                                char *backing_mem, uint64_t used_size,
                                uint64_t allocated_size, bool async_flush) = 0;

        virtual void GetSSTable(const std::string &dbname, uint64_t file_number,
                                WBTable **table) = 0;

        virtual void
        RemoveSSTable(const std::string &dbname, uint64_t file_number) = 0;

        virtual void
        RemoveSSTables(const std::string &dbname,
                       const std::vector<uint64_t> &file_number) = 0;
    };
}

#endif //LEVELDB_DB_TYPES_H
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_

#include <stdint.h>

#include "leveldb/export.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
// 学习tablebuiler的写法 之后改数据结构的话也要改成类似table builder这种加的同时内存就准备好了的结构
// 总体来说就是要把nvm memtable和l0实现为一个数据结构 通过 slice 向文件直接写入 同时可以简单读出来相应的东西
namespace leveldb {

    class BlockBuilder;

    class BlockHandle;

    class WritableFile;

    class LEVELDB_EXPORT TableBuilder {
    public:
        // Create a builder that will store the contents of the table it is
        // building in *file.  Does not close the file.  It is up to the
        // caller to close the file after calling Finish().
        TableBuilder(const Options &options, WritableFile *file);

        TableBuilder(const TableBuilder &) = delete;

        TableBuilder &operator=(const TableBuilder &) = delete;

        // REQUIRES: Either Finish() or Abandon() has been called.
        ~TableBuilder();

        // Change the options used by this builder.  Note: only some of the
        // option fields can be changed after construction.  If a field is
        // not allowed to change dynamically and its value in the structure
        // passed to the constructor is different from its value in the
        // structure passed to this method, this method will return an error
        // without changing any fields.
        Status ChangeOptions(const Options &options);

        // Add key,value to the table being constructed.
        // REQUIRES: key is after any previously added key according to comparator.
        // REQUIRES: Finish(), Abandon() have not been called
        bool Add(const Slice &key, const Slice &value);

        // Advanced operation: flush any buffered key/value pairs to file.
        // Can be used to ensure that two adjacent entries never live in
        // the same data block.  Most clients should not need to use this method.
        // REQUIRES: Finish(), Abandon() have not been called
        void Flush();

        // Return non-ok iff some error has been detected.
        Status status() const;

        // Finish building the table.  Stops using the file passed to the
        // constructor after this function returns.
        // REQUIRES: Finish(), Abandon() have not been called
        Status Finish();

        // Indicate that the contents of this builder should be abandoned.  Stops
        // using the file passed to the constructor after this function returns.
        // If the caller is not going to call Finish(), it must call Abandon()
        // before destroying this builder.
        // REQUIRES: Finish(), Abandon() have not been called
        void Abandon();

        // Number of calls to Add() so far.
        uint64_t NumEntries() const;

        uint64_t NumDataBlocks() const;

        // Size of the file generated so far.  If invoked after a successful
        // Finish() call, returns the size of the final generated file.
        uint64_t FileSize() const;

    private:
        bool ok() const { return status().ok(); }

        void WriteBlock(BlockBuilder *block, BlockHandle *handle);

        void
        WriteRawBlock(const Slice &data, CompressionType, BlockHandle *handle);

        struct Rep;
        Rep *rep_;
    };

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_H_

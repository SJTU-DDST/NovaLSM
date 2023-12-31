// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include <common/nova_console_logging.h>
#include <fmt/core.h>
#include <db/dbformat.h>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

    struct TableBuilder::Rep {
        Rep(const Options &opt, WritableFile *f)
                : options(opt),
                  index_block_options(opt),
                  file(f),
                  offset(0),
                  data_block(&options),
                  index_block(&index_block_options),
                  num_entries(0),
                  num_data_blocks(0),
                  closed(false),
                  filter_block(opt.filter_policy == nullptr
                               ? nullptr
                               : new FilterBlockBuilder(opt.filter_policy)), //一般不是null
                  pending_index_entry(false) {
            index_block_options.block_restart_interval = 1;
        }

        Options options;
        Options index_block_options;
        WritableFile *file;
        uint64_t offset;
        Status status;
        BlockBuilder data_block;
        BlockBuilder index_block;
        std::string last_key;
        int64_t num_entries;
        uint64_t num_data_blocks;
        bool closed;  // Either Finish() or Abandon() has been called.
        FilterBlockBuilder *filter_block;

        // We do not emit the index entry for a block until we have seen the
        // first key for the next data block.  This allows us to use shorter
        // keys in the index block.  For example, consider a block boundary
        // between the keys "the quick brown fox" and "the who".  We can use
        // "the r" as the key for the index block entry since it is >= all
        // entries in the first block and < all entries in subsequent
        // blocks.
        //
        // Invariant: r->pending_index_entry is true only if data_block is empty.
        bool pending_index_entry;
        BlockHandle pending_handle;  // Handle to add to index block

        std::string compressed_output;
    };

    TableBuilder::TableBuilder(const Options &options, WritableFile *file)
            : rep_(new Rep(options, file)) {
        if (rep_->filter_block != nullptr) {
            rep_->filter_block->StartBlock(0);
        }
    }

    TableBuilder::~TableBuilder() {
        assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
        delete rep_->filter_block;
        delete rep_;
    }

    Status TableBuilder::ChangeOptions(const Options &options) {
        // Note: if more fields are added to Options, update
        // this function to catch changes that should not be allowed to
        // change in the middle of building a Table.
        if (options.comparator != rep_->options.comparator) {
            return Status::InvalidArgument(
                    "changing comparator while building table");
        }

        // Note that any live BlockBuilders point to rep_->options and therefore
        // will automatically pick up the updated options.
        rep_->options = options;
        rep_->index_block_options = options;
        rep_->index_block_options.block_restart_interval = 1;
        return Status::OK();
    }

// 把key和value加入到这个文件中?
    bool TableBuilder::Add(const Slice &key, const Slice &value) {
        Rep *r = rep_;
        assert(!r->closed);
        if (!ok()) return false;
        if (r->num_entries > 0) {
            if (r->options.comparator->Compare(key, Slice(r->last_key)) <= //因为是按顺序家的 所以必然递增 这个情况发生说明有问题
                0) {
                ParsedInternalKey ik;
                ParseInternalKey(key, &ik);
                ParsedInternalKey lk;
                ParseInternalKey(r->last_key, &lk);
                return false;
            }
        }

        if (r->pending_index_entry) { // 基本都是false 有一个block写进去之后会变true
            assert(r->data_block.empty());
            r->options.comparator->FindShortestSeparator(&r->last_key, key);
            std::string handle_encoding;
            r->pending_handle.EncodeTo(&handle_encoding);
            r->index_block.Add(r->last_key, Slice(handle_encoding));
            r->num_data_blocks += 1;
            r->pending_index_entry = false;
        }

        if (r->filter_block != nullptr) { // 基本不是null
            r->filter_block->AddKey(key);
        }

        r->last_key.assign(key.data(), key.size());
        r->num_entries++;
        r->data_block.Add(key, value);

        const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
        if (estimated_block_size >= r->options.block_size) { // 如果当前的块大小超过config中的值了 或者说完成了一个datablock的编写 4096字节?
            Flush(); // 进行flush操作?作用:
        }
        return true;
    }

// 完成了1个block的编写
    void TableBuilder::Flush() {
        Rep *r = rep_;
        assert(!r->closed);
        if (!ok()) return;
        if (r->data_block.empty()) return;
        assert(!r->pending_index_entry);
        WriteBlock(&r->data_block, &r->pending_handle);
        if (ok()) {// 每写好一个block就改一下标识 并且
            r->pending_index_entry = true;
            r->status = r->file->Flush(); // 这里memwriteble 所以好像是什么都不做
        }
        if (r->filter_block != nullptr) {
            r->filter_block->StartBlock(r->offset); // 再开始一个 filter block
        }
    }

// 把当前这个block写入，并且开一个新block
    void TableBuilder::WriteBlock(BlockBuilder *block, BlockHandle *handle) {
        // File format contains a sequence of blocks where each block has:
        //    block_data: uint8[n]
        //    type: uint8
        //    crc: uint32
        assert(ok());
        Rep *r = rep_;
        Slice raw = block->Finish();

        Slice block_contents;
        CompressionType type = r->options.compression;
        // TODO(postrelease): Support more compression options: zlib?
        switch (type) {
            case kNoCompression: //一般都是这个
                block_contents = raw;
                break;

            case kSnappyCompression: {
                std::string *compressed = &r->compressed_output;
                if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
                    compressed->size() < raw.size() - (raw.size() / 8u)) {
                    block_contents = *compressed;
                } else {
                    // Snappy not supported, or compressed less than 12.5%, so just
                    // store uncompressed form
                    block_contents = raw;
                    type = kNoCompression;
                }
                break;
            }
        }
        WriteRawBlock(block_contents, type, handle);
        r->compressed_output.clear();
        block->Reset();
    }

// 把已经组织好的block(可以直接写入的)写入
    void TableBuilder::WriteRawBlock(const Slice &block_contents,
                                     CompressionType type,
                                     BlockHandle *handle) {
        Rep *r = rep_;
        // 这里是根据rep里面的记录来制作当前写入的block的
        handle->set_offset(r->offset);
        handle->set_size(block_contents.size());
        r->status = r->file->Append(block_contents); // 写入文件
        if (r->status.ok()) { // append之后为每个block加压缩类型和校验码
            char trailer[kBlockTrailerSize];
            trailer[0] = type;
            uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
            crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
            EncodeFixed32(trailer + 1, crc32c::Mask(crc));
            // Make sure the last byte is not 0.
            trailer[kBlockTrailerSize - 1] = '!';
            r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));// 将校验码也写入
            if (r->status.ok()) {
                r->offset += block_contents.size() + kBlockTrailerSize;
            }
        }
    }

    Status TableBuilder::status() const { return rep_->status; }

// 所有的kv都处理完了 写剩余的数据以及除了数据块的其他东西
    Status TableBuilder::Finish() {
        Rep *r = rep_;
        Flush(); // 首先处理剩下的数据块
        assert(!r->closed);
        r->closed = true;

        BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

        // Write filter block
        if (ok() && r->filter_block != nullptr) {
            WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                          &filter_block_handle);
            NOVA_LOG(rdmaio::DEBUG)
                << fmt::format("filter handle off:{} size:{}",
                               filter_block_handle.offset(),
                               filter_block_handle.size());
        }

        // Write metaindex block
        if (ok()) {
            BlockBuilder meta_index_block(&r->options);
            if (r->filter_block != nullptr) {
                // Add mapping from "filter.Name" to location of filter data
                std::string key = "filter.";
                key.append(r->options.filter_policy->Name());
                std::string handle_encoding;
                filter_block_handle.EncodeTo(&handle_encoding);
                meta_index_block.Add(key, handle_encoding);
            }

            // TODO(postrelease): Add stats and other meta blocks
            WriteBlock(&meta_index_block, &metaindex_block_handle);
        }

        // Write index block
        if (ok()) {
            if (r->pending_index_entry) { // 如果刚才结束了一个block的编写
                r->options.comparator->FindShortSuccessor(&r->last_key);
                std::string handle_encoding;
                r->pending_handle.EncodeTo(&handle_encoding);
                r->index_block.Add(r->last_key, Slice(handle_encoding));
                r->num_data_blocks += 1;
                r->pending_index_entry = false;
            }
            WriteBlock(&r->index_block, &index_block_handle);
        }

        // Write footer
        if (ok()) {
            Footer footer;
            footer.set_metaindex_handle(metaindex_block_handle);
            footer.set_index_handle(index_block_handle);
            std::string footer_encoding;
            footer.EncodeTo(&footer_encoding);
            r->status = r->file->Append(footer_encoding);
            if (r->status.ok()) {
                r->offset += footer_encoding.size();
            }
        }
        return r->status;
    }

    void TableBuilder::Abandon() {
        Rep *r = rep_;
        assert(!r->closed);
        r->closed = true;
    }

    uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

    uint64_t
    TableBuilder::NumDataBlocks() const { return rep_->num_data_blocks; }

    uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb

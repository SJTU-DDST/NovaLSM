
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#include "compaction.h"
#include "filename.h"

namespace leveldb {
    CompactionJob::CompactionJob(std::function<uint64_t(void)> &fn_generator,
                                 leveldb::Env *env, const std::string &dbname,
                                 const leveldb::Comparator *user_comparator,
                                 const leveldb::Options &options)
            : fn_generator_(fn_generator), env_(env), dbname_(dbname),
              user_comparator_(
                      user_comparator), options_(options) {
    }

    Status CompactionJob::OpenCompactionOutputFile(CompactionState *compact,
                                                   EnvBGThread *bg_thread) {
        assert(compact != nullptr);
        assert(compact->builder == nullptr);
        uint64_t file_number;
        {
            file_number = fn_generator_();
            CompactionState::Output out;
            out.number = file_number;
            out.smallest.Clear();
            out.largest.Clear();
            compact->outputs.push_back(out);
        }
        // Make the output file
        MemManager *mem_manager = bg_thread->mem_manager();
        std::string filename = TableFileName(dbname_, file_number);
        NovaCCMemFile *cc_file = new NovaCCMemFile(options_.env,
                                                   options_,
                                                   file_number,
                                                   mem_manager,
                                                   bg_thread->dc_client(),
                                                   dbname_,
                                                   bg_thread->thread_id(),
                                                   options_.max_dc_file_size,
                                                   bg_thread->rand_seed(),
                                                   filename);
        compact->outfile = new MemWritableFile(cc_file);
        compact->builder = new TableBuilder(options_, compact->outfile);
        compact->output_files.push_back(compact->outfile);
        return Status::OK();
    }

    Status
    CompactionJob::FinishCompactionOutputFile(const ParsedInternalKey &ik,
                                              CompactionState *compact,
                                              Iterator *input) {
        assert(compact != nullptr);
        assert(compact->outfile != nullptr);
        assert(compact->builder != nullptr);
        assert(!compact->output_files.empty());

        const uint64_t output_number = compact->current_output()->number;
        assert(output_number != 0);

        // Check for iterator errors
        Status s = input->status();
        if (s.ok()) {
            s = compact->builder->Finish();
        } else {
            compact->builder->Abandon();
        }
        const uint64_t current_entries = compact->builder->NumEntries();
        const uint64_t current_data_blocks = compact->builder->NumDataBlocks();
        const uint64_t current_bytes = compact->builder->FileSize();
        compact->current_output()->file_size = current_bytes;
        compact->total_bytes += current_bytes;
        delete compact->builder;
        compact->builder = nullptr;

        RDMA_LOG(rdmaio::INFO)
            << fmt::format("Close table-{} at {} bytes", output_number,
                           current_bytes);

        FileMetaData meta;
        meta.number = output_number;
        meta.file_size = current_bytes;
        meta.smallest = compact->current_output()->smallest;
        meta.largest = compact->current_output()->largest;
        // Set meta in order to flush to the corresponding DC node.
        NovaCCMemFile *mem_file = static_cast<NovaCCMemFile *>(compact->outfile->mem_file());
        mem_file->set_meta(meta);
        mem_file->set_num_data_blocks(current_data_blocks);

        // Finish and check for file errors
        RDMA_ASSERT(s.ok());
        s = compact->outfile->Sync();
        s = compact->outfile->Close();

        mem_file->WaitForPersistingDataBlocks();
        return s;
    }

    Status
    CompactionJob::CompactTables(CompactionState *compact,
                                 EnvBGThread *bg_thread,
                                 Iterator *input,
                                 CompactionStats *stats, bool drop_duplicates,
                                 CompactType type) {
        const uint64_t start_micros = env_->NowMicros();
        std::string output;
        if (type == CompactType::kCompactMemTables) {
            output = fmt::format(
                    "bg[{}] Flushing {} memtables",
                    bg_thread->thread_id(),
                    stats->input_source.num_files);
        } else {
            output = fmt::format(
                    "bg[{}] Major Compacting {}@{} + {}@{} files",
                    bg_thread->thread_id(),
                    stats->input_source.num_files,
                    stats->input_source.level,
                    stats->input_target.num_files,
                    stats->input_target.level);
        }
        Log(options_.info_log, "%s", output.c_str());
        RDMA_LOG(rdmaio::INFO) << output;

        assert(compact->builder == nullptr);
        assert(compact->outfile == nullptr);
        assert(compact->outputs.empty());

        input->SeekToFirst();
        Status status;
        ParsedInternalKey ikey;
        std::string current_user_key;
        bool has_current_user_key = false;
        SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
        while (input->Valid()) {
            Slice key = input->key();
            RDMA_ASSERT(ParseInternalKey(key, &ikey));

            if (compact->ShouldStopBefore(key, user_comparator_) &&
                compact->builder != nullptr &&
                compact->builder->NumEntries() > 0) {
                status = FinishCompactionOutputFile(ikey, compact, input);
                if (!status.ok()) {
                    break;
                }
            }

            // Handle key/value, add to state, etc.
            bool drop = false;
            if (!has_current_user_key ||
                user_comparator_->Compare(ikey.user_key,
                                          Slice(current_user_key)) !=
                0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(),
                                        ikey.user_key.size());
                has_current_user_key = true;
                last_sequence_for_key = kMaxSequenceNumber;
            }

            if (last_sequence_for_key <= compact->smallest_snapshot) {
                // Hidden by an newer entry for same user key
                drop = true;  // (A)
            }
            last_sequence_for_key = ikey.sequence;
#if 0
            Log(options_.info_log,
                "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
                "%d smallest_snapshot: %d",
                ikey.user_key.ToString().c_str(),
                (int)ikey.sequence, ikey.type, kTypeValue, drop,
                compact->compaction->IsBaseLevelForKey(ikey.user_key),
                (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif
            if (drop && drop_duplicates) {
                RDMA_LOG(rdmaio::DEBUG)
                    << fmt::format("drop key-{}", ikey.FullDebugString());
                input->Next();
                continue;
            } else {
                // Open output file if necessary
                if (compact->builder == nullptr) {
                    status = OpenCompactionOutputFile(compact, bg_thread);
                    if (!status.ok()) {
                        break;
                    }
                }
                if (compact->builder->NumEntries() == 0) {
                    compact->current_output()->smallest.DecodeFrom(key);
                }
                RDMA_LOG(rdmaio::DEBUG)
                    << fmt::format("add key-{}", ikey.FullDebugString());
                compact->current_output()->largest.DecodeFrom(key);
                compact->builder->Add(key, input->value());

                // Close output file if it is big enough
                if (compact->builder->FileSize() >= options_.max_file_size) {
                    status = FinishCompactionOutputFile(ikey, compact, input);
                    if (!status.ok()) {
                        break;
                    }
                }
            }
            input->Next();
        }

        if (status.ok() && compact->builder != nullptr) {
            status = FinishCompactionOutputFile(ikey, compact, input);
        }
        if (status.ok()) {
            status = input->status();
        }
        delete input;
        input = nullptr;

        stats->micros = env_->NowMicros() - start_micros;
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            stats->output.file_size += compact->outputs[i].file_size;
            stats->output.num_files += 1;
        }

        if (type == CompactType::kCompactMemTables) {
            output = fmt::format(
                    "bg[{}] Flushing {} memtables => {} files {} bytes",
                    bg_thread->thread_id(),
                    stats->input_source.num_files,
                    stats->output.num_files,
                    stats->output.file_size);
        } else {
            const int src_level = compact->compaction->level();
            const int dest_level = compact->compaction->target_level();
            output = fmt::format(
                    "bg[{}]: Major Compacted {}@{} + {}@{} files => {} bytes",
                    bg_thread->thread_id(),
                    compact->compaction->num_input_files(0),
                    src_level,
                    compact->compaction->num_input_files(1),
                    dest_level,
                    compact->total_bytes);
        }
        Log(options_.info_log, "%s", output.c_str());
        RDMA_LOG(rdmaio::INFO) << output;

        if (type == CompactType::kCompactMemTables) {
            output = fmt::format(
                    "Flushing memtables stats,{},{},{},{},{}",
                    stats->input_source.num_files +
                    stats->input_target.num_files,
                    stats->input_source.file_size +
                    stats->input_target.file_size,
                    stats->output.num_files, stats->output.file_size,
                    stats->micros);
        } else {
            output = fmt::format("Major compaction stats,{},{},{},{},{}",
                                 stats->input_source.num_files +
                                 stats->input_target.num_files,
                                 stats->input_source.file_size +
                                 stats->input_target.file_size,
                                 stats->output.num_files,
                                 stats->output.file_size,
                                 stats->micros);
        }
        Log(options_.info_log, "%s", output.c_str());
        RDMA_LOG(rdmaio::INFO) << output;

        // Now finalize all tables.
        for (int i = 0; i < compact->output_files.size(); i++) {
            CompactionState::Output &output = compact->outputs[i];
            MemWritableFile *out = compact->output_files[i];
            auto *mem_file = dynamic_cast<NovaCCMemFile *>(out->mem_file());
            output.converted_file_size = mem_file->Finalize();
            output.meta_block_handle = mem_file->meta_block_handle();
            output.data_block_group_handles = mem_file->rhs();

            delete mem_file;
            delete out;
            compact->output_files[i] = nullptr;
            mem_file = nullptr;
            out = nullptr;
        }

        if (compact->compaction) {
            sem_post(&compact->compaction->complete_signal_);
        }
        return status;
    }
}
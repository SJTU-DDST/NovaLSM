// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <stdio.h>

#include <algorithm>
#include <fmt/core.h>
#include <getopt.h>
#include <nova/nova_common.h>
#include <cc/nova_cc.h>
#include "nova/logging.hpp"

#include "db/filename.h"
#include "db/log_reader.h"
#include "leveldb/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

    static size_t TargetFileSize(const Options *options) {
        return options->max_file_size;
    }

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
    static int64_t MaxGrandParentOverlapBytes(const Options *options) {
        return 10 * TargetFileSize(options);
    }

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
    static int64_t ExpandedCompactionByteSizeLimit(const Options *options) {
        return 25 * TargetFileSize(options);
    }

    static double MaxBytesForLevel(const Options &options, int level) {
        // Note: the result for level zero is not really used since we set
        // the level-0 compaction threshold based on number of files.

        // Result for both level-0 and level-1
        double result = 10. * 1048576.0; // 10 MB.
        while (level > 1) {
            result *= 10;
            level--;
        }
        return result;
    }

    static uint64_t MaxFileSizeForLevel(const Options *options, int level) {
        // We could vary per level to reduce number of files?
        return TargetFileSize(options);
    }

    static int64_t TotalFileSize(const std::vector<FileMetaData *> &files) {
        int64_t sum = 0;
        for (size_t i = 0; i < files.size(); i++) {
            sum += files[i]->file_size;
        }
        return sum;
    }

    void Version::Destroy() {
        assert(refs_ == 0);

        // Remove from linked list
        prev_->next_ = next_;
        next_->prev_ = prev_;

        // Drop references to files
        for (int level = 0; level < config::kNumLevels; level++) {
            for (size_t i = 0; i < files_[level].size(); i++) {
                FileMetaData *f = files_[level][i];
                assert(f->refs > 0);
                f->refs--;
                if (f->refs <= 0) {
//                    delete f;
                }
            }
        }
    }

    Version::~Version() {

    }

    int FindFile(const InternalKeyComparator &icmp,
                 const std::vector<FileMetaData *> &files, const Slice &key) {
        uint32_t left = 0;
        uint32_t right = files.size();
        while (left < right) {
            uint32_t mid = (left + right) / 2;
            const FileMetaData *f = files[mid];
            if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) <
                0) {
                // Key at "mid.largest" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.largest" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    static bool AfterFile(const Comparator *ucmp, const Slice *user_key,
                          const FileMetaData *f) {
        // null user_key occurs before all keys and is therefore never after *f
        return (user_key != nullptr &&
                ucmp->Compare(*user_key, f->largest.user_key()) > 0);
    }

    static bool BeforeFile(const Comparator *ucmp, const Slice *user_key,
                           const FileMetaData *f) {
        // null user_key occurs after all keys and is therefore never before *f
        return (user_key != nullptr &&
                ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
    }

    bool SomeFileOverlapsRange(const InternalKeyComparator &icmp,
                               bool disjoint_sorted_files,
                               const std::vector<FileMetaData *> &files,
                               const Slice *smallest_user_key,
                               const Slice *largest_user_key) {
        const Comparator *ucmp = icmp.user_comparator();
        if (!disjoint_sorted_files) {
            // Need to check against all files
            for (size_t i = 0; i < files.size(); i++) {
                const FileMetaData *f = files[i];
                if (AfterFile(ucmp, smallest_user_key, f) ||
                    BeforeFile(ucmp, largest_user_key, f)) {
                    // No overlap
                } else {
                    return true;  // Overlap
                }
            }
            return false;
        }

        // Binary search over file list
        uint32_t index = 0;
        if (smallest_user_key != nullptr) {
            // Find the earliest possible internal key for smallest_user_key
            InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                                  kValueTypeForSeek);
            index = FindFile(icmp, files, small_key.Encode());
        }

        if (index >= files.size()) {
            // beginning of range is after all files, so no overlap.
            return false;
        }

        return !BeforeFile(ucmp, largest_user_key, files[index]);
    }

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
    class Version::LevelFileNumIterator : public Iterator {
    public:
        LevelFileNumIterator(const InternalKeyComparator &icmp,
                             const std::vector<FileMetaData *> *flist)
                : icmp_(icmp), flist_(flist),
                  index_(flist->size()) {  // Marks as invalid
        }

        bool Valid() const override { return index_ < flist_->size(); }

        void Seek(const Slice &target) override {
            index_ = FindFile(icmp_, *flist_, target);
        }

        void SeekToFirst() override { index_ = 0; }

        void SeekToLast() override {
            index_ = flist_->empty() ? 0 : flist_->size() - 1;
        }

        void Next() override {
            assert(Valid());
            index_++;
        }

        void Prev() override {
            assert(Valid());
            if (index_ == 0) {
                index_ = flist_->size();  // Marks as invalid
            } else {
                index_--;
            }
        }

        Slice key() const override {
            assert(Valid());
            return (*flist_)[index_]->largest.Encode();
        }

        Slice value() const override {
            assert(Valid());
            FileMetaData &meta = *(*flist_)[index_];
            EncodeFixed64(value_buf_, meta.number);
            return Slice(value_buf_, 8);
        }

        Status status() const override { return Status::OK(); }

    private:
        const InternalKeyComparator icmp_;
        const std::vector<FileMetaData *> *const flist_;
        uint32_t index_;

        // Backing store for value().  Holds the file number and size.
        mutable char value_buf_[8];
    };

    static Iterator *
    GetFileIterator(void *arg, BlockReadContext context,
                    const ReadOptions &options,
                    const Slice &file_value) {
        Version *v = reinterpret_cast<Version *>(arg);
        RDMA_ASSERT(v);
        RDMA_ASSERT(file_value.size() == 8);
        uint64_t fn = DecodeFixed64(file_value.data());
        FileMetaData *meta = v->file_meta(fn);
        RDMA_ASSERT(meta) << fn;
        return v->table_cache()->NewIterator(context.caller, options, *meta,
                                             meta->number,
                                             context.level,
                                             meta->converted_file_size);
    }

    TableCache *Version::table_cache() {
        return vset_->table_cache_;
    }

    FileMetaData *Version::file_meta(uint64_t fn) {
        return fn_files_[fn];
    }

    Iterator *Version::NewConcatenatingIterator(const ReadOptions &options,
                                                int level) const {
        BlockReadContext context = {
                .caller = AccessCaller::kUserIterator,
                .file_number = 0,
                .level = level,
        };
        return NewTwoLevelIterator(
                new LevelFileNumIterator(vset_->icmp_, &files_[level]),
                context,
                &GetFileIterator,
                (void *) this, options);
    }

    void Version::AddIterators(const ReadOptions &options,
                               std::vector<Iterator *> *iters) {
        // Merge all level zero files together since they may overlap
        for (size_t i = 0; i < files_[0].size(); i++) {
            iters->push_back(vset_->table_cache_->NewIterator(
                    AccessCaller::kUserIterator,
                    options, *files_[0][i], files_[0][i]->number, 0,
                    files_[0][i]->converted_file_size));
        }

        // For levels > 0, we can use a concatenating iterator that sequentially
        // walks through the non-overlapping files in the level, opening them
        // lazily.
        for (int level = 1; level < config::kNumLevels; level++) {
            if (!files_[level].empty()) {
                iters->push_back(NewConcatenatingIterator(options, level));
            }
        }
    }

// Callback from TableCache::Get()
    namespace {
        enum SaverState {
            kNotFound,
            kFound,
            kDeleted,
            kCorrupt,
        };
        struct Saver {
            SaverState state;
            const Comparator *ucmp;
            Slice user_key;
            std::string *value;
        };
    }  // namespace
    static void SaveValue(void *arg, const Slice &ikey, const Slice &v) {
        Saver *s = reinterpret_cast<Saver *>(arg);
        ParsedInternalKey parsed_key;
        if (!ParseInternalKey(ikey, &parsed_key)) {
            s->state = kCorrupt;
        } else {
            if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
                s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
                if (s->state == kFound) {
                    s->value->assign(v.data(), v.size());
                }
            }
        }
    }

    static bool NewestFirst(FileMetaData *a, FileMetaData *b) {
        return a->number > b->number;
    }

    void
    Version::ForEachOverlapping(Slice user_key, Slice internal_key, void *arg,
                                bool (*func)(void *, int, FileMetaData *),
                                bool search_all_l0) {
        const Comparator *ucmp = vset_->icmp_.user_comparator();

        // Search level-0 in order from newest to oldest.
        std::vector<FileMetaData *> tmp;
        tmp.reserve(files_[0].size());
        for (uint32_t i = 0; i < files_[0].size(); i++) {
            FileMetaData *f = files_[0][i];
            if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
                ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
                tmp.push_back(f);
            }
        }
        bool found_in_l0 = false;
        if (!tmp.empty()) {
            std::sort(tmp.begin(), tmp.end(), NewestFirst);
            for (uint32_t i = 0; i < tmp.size(); i++) {
                if (!(*func)(arg, 0, tmp[i])) {
                    found_in_l0 = true;
                    if (!search_all_l0) {
                        return;
                    }
                }
            }
        }

        if (found_in_l0) {
            return;
        }

        // Search other levels.
        for (int level = 1; level < config::kNumLevels; level++) {
            size_t num_files = files_[level].size();
            if (num_files == 0) continue;

            // Binary search to find earliest index whose largest key >= internal_key.
            uint32_t index = FindFile(vset_->icmp_, files_[level],
                                      internal_key);
            if (index < num_files) {
                FileMetaData *f = files_[level][index];
                if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
                    // All of "f" is past any data for user_key
                } else {
                    if (!(*func)(arg, level, f)) {
                        return;
                    }
                }
            }
        }
    }

    Status Version::Get(const leveldb::ReadOptions &options, uint64_t fn,
                        const leveldb::LookupKey &key, std::string *val) {
        RDMA_ASSERT(fn < MAX_LIVE_MEMTABLES);
        FileMetaData *file = fn_files_[fn];
        RDMA_ASSERT(file);
        RDMA_ASSERT(file->number == fn);
        Saver saver;
        saver.state = kNotFound;
        saver.ucmp = vset_->icmp_.user_comparator();
        saver.user_key = key.user_key();
        saver.value = val;
        Status s = vset_->table_cache_->Get(options,
                                            *file,
                                            file->number,
                                            file->converted_file_size,
                                            0,
                                            key.internal_key(),
                                            &saver,
                                            SaveValue);
        RDMA_ASSERT(saver.state == kFound);
        return s;
    }

    Status Version::Get(const ReadOptions &options, const LookupKey &k,
                        std::string *value, GetStats *stats,
                        bool search_all_l0) {
        stats->seek_file = nullptr;
        stats->seek_file_level = -1;

        struct State {
            Saver saver;
            GetStats *stats;
            const ReadOptions *options;
            Slice ikey;
            FileMetaData *last_file_read;
            int last_file_read_level;

            VersionSet *vset;
            Status s;
            bool found;

            static bool Match(void *arg, int level, FileMetaData *f) {
                State *state = reinterpret_cast<State *>(arg);
                if (state->stats->seek_file == nullptr &&
                    state->last_file_read != nullptr) {
                    // We have had more than one seek for this read.  Charge the 1st file.
                    state->stats->seek_file = state->last_file_read;
                    state->stats->seek_file_level = state->last_file_read_level;
                }

                state->last_file_read = f;
                state->last_file_read_level = level;
                state->s = state->vset->table_cache_->Get(*state->options,
                                                          *f,
                                                          f->number,
                                                          f->converted_file_size,
                                                          level,
                                                          state->ikey,
                                                          &state->saver,
                                                          SaveValue);

                if (!state->s.ok()) {
                    state->found = true;
                    return false;
                }
                switch (state->saver.state) {
                    case kNotFound:
                        return true;  // Keep searching in other files
                    case kFound:
                        state->found = true;
                        return false;
                    case kDeleted:
                        return false;
                    case kCorrupt:
                        state->s =
                                Status::Corruption("corrupted key for ",
                                                   state->saver.user_key);
                        state->found = true;
                        return false;
                }

                // Not reached. Added to avoid false compilation warnings of
                // "control reaches end of non-void function".
                return false;
            }
        };

        State state;
        state.found = false;
        state.stats = stats;
        state.last_file_read = nullptr;
        state.last_file_read_level = -1;

        state.options = &options;
        state.ikey = k.internal_key();
        state.vset = vset_;

        state.saver.state = kNotFound;
        state.saver.ucmp = vset_->icmp_.user_comparator();
        state.saver.user_key = k.user_key();
        state.saver.value = value;

        ForEachOverlapping(state.saver.user_key, state.ikey, &state,
                           &State::Match, search_all_l0);

        return state.found ? state.s : Status::NotFound(Slice());
    }

    bool Version::UpdateStats(const GetStats &stats) {
        FileMetaData *f = stats.seek_file;
        if (f != nullptr) {
            f->allowed_seeks--;
            if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
                file_to_compact_ = f;
                file_to_compact_level_ = stats.seek_file_level;
                return true;
            }
        }
        return false;
    }

//    bool Version::RecordReadSample(Slice internal_key) {
//        ParsedInternalKey ikey;
//        if (!ParseInternalKey(internal_key, &ikey)) {
//            return false;
//        }
//
//        struct State {
//            GetStats stats;  // Holds first matching file
//            int matches;
//
//            static bool Match(void *arg, int level, FileMetaData *f) {
//                State *state = reinterpret_cast<State *>(arg);
//                state->matches++;
//                if (state->matches == 1) {
//                    // Remember first match.
//                    state->stats.seek_file = f;
//                    state->stats.seek_file_level = level;
//                }
//                // We can stop iterating once we have a second match.
//                return state->matches < 2;
//            }
//        };
//
//        State state;
//        state.matches = 0;
//        ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);
//
//        // Must have at least two matches since we want to merge across
//        // files. But what if we have a single file that contains many
//        // overwrites and deletions?  Should we have another mechanism for
//        // finding such files?
//        if (state.matches >= 2) {
//            // 1MB cost is about 1 seek (see comment in Builder::Apply).
//            return UpdateStats(state.stats);
//        }
//        return false;
//    }

    void Version::Ref() {
        ++refs_;
    }

    uint32_t Version::Unref() {
        uint32_t refs = 0;
        assert(this != &vset_->dummy_versions_);
        assert(refs_ >= 1);
        --refs_;
        refs = refs_;
        return refs;
    }

    bool Version::OverlapInLevel(int level, const Slice *smallest_user_key,
                                 const Slice *largest_user_key) {
        return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                                     smallest_user_key, largest_user_key);
    }

    int Version::PickLevelForMemTableOutput(const Slice &smallest_user_key,
                                            const Slice &largest_user_key) {
        int level = 0;
        if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
            // Push to next level if there is no overlap in next level,
            // and the #bytes overlapping in the level after that are limited.
            InternalKey start(smallest_user_key, kMaxSequenceNumber,
                              kValueTypeForSeek);
            InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
            std::vector<FileMetaData *> overlaps;
            while (level < config::kMaxMemCompactLevel) {
                if (OverlapInLevel(level + 1, &smallest_user_key,
                                   &largest_user_key)) {
                    break;
                }
                if (level + 2 < config::kNumLevels) {
                    // Check that file does not overlap too many grandparent bytes.
                    GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
                    const int64_t sum = TotalFileSize(overlaps);
                    if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
                        break;
                    }
                }
                level++;
            }
        }
        return level;
    }

// Store in "*inputs" all files in "level" that overlap [begin,end]
    void Version::GetOverlappingInputs(int level, const InternalKey *begin,
                                       const InternalKey *end,
                                       std::vector<FileMetaData *> *inputs) {
        assert(level >= 0);
        assert(level < config::kNumLevels);
        inputs->clear();
        Slice user_begin, user_end;
        if (begin != nullptr) {
            user_begin = begin->user_key();
        }
        if (end != nullptr) {
            user_end = end->user_key();
        }
        const Comparator *user_cmp = vset_->icmp_.user_comparator();
        for (size_t i = 0; i < files_[level].size();) {
            FileMetaData *f = files_[level][i++];
            const Slice file_start = f->smallest.user_key();
            const Slice file_limit = f->largest.user_key();
            if (begin != nullptr &&
                user_cmp->Compare(file_limit, user_begin) < 0) {
                // "f" is completely before specified range; skip it
            } else if (end != nullptr &&
                       user_cmp->Compare(file_start, user_end) > 0) {
                // "f" is completely after specified range; skip it
            } else {
                inputs->push_back(f);
//                if (level == 0) {
//                    // Level-0 files may overlap each other.  So check if the newly
//                    // added file has expanded the range.  If so, restart search.
//                    if (begin != nullptr &&
//                        user_cmp->Compare(file_start, user_begin) < 0) {
//                        user_begin = file_start;
//                        inputs->clear();
//                        i = 0;
//                    } else if (end != nullptr &&
//                               user_cmp->Compare(file_limit, user_end) > 0) {
//                        user_end = file_limit;
//                        inputs->clear();
//                        i = 0;
//                    }
//                }
            }
        }
    }

    void Version::ComputeOverlappingFilesPerTable(
            std::map<uint64_t, leveldb::FileMetaData *> *files,
            std::vector<leveldb::OverlappingStats> *num_overlapping,
            const leveldb::Comparator *user_comparator) {
        for (auto pivot_it = files->begin();
             pivot_it != files->end(); pivot_it++) {
            Slice lower = pivot_it->second->smallest.user_key();
            Slice upper = pivot_it->second->largest.user_key();

            OverlappingStats stats = {};
            stats.num_overlapping_tables = 1;
            stats.total_size = pivot_it->second->file_size;
            nova::str_to_int(lower.data(), &stats.smallest, lower.size());
            nova::str_to_int(upper.data(), &stats.largest, upper.size());

            for (auto comp_it = files->begin();
                 comp_it != files->end(); comp_it++) {
                if (comp_it == pivot_it) {
                    continue;
                }
                if (user_comparator->Compare(lower,
                                             comp_it->second->largest.user_key()) >
                    0) {
                    continue;
                }
                if (user_comparator->Compare(upper,
                                             comp_it->second->smallest.user_key()) <
                    0) {
                    continue;
                }
                stats.num_overlapping_tables += 1;
                stats.total_size += comp_it->second->file_size;
            }
            num_overlapping->push_back(stats);
        }

        {
            auto comp = [&](const OverlappingStats &a,
                            const OverlappingStats &b) {
                return a.num_overlapping_tables > b.num_overlapping_tables;
            };
            std::sort(num_overlapping->begin(), num_overlapping->end(),
                      comp);
        }
    }

    bool Version::ComputeOverlappingFilesInRange(
            std::map<uint64_t, FileMetaData *> *files,
            int which,
            Compaction *compaction,
            const Slice &lower,
            const Slice &upper,
            Slice *new_lower,
            Slice *new_upper,
            const leveldb::Comparator *user_comparator) {
        bool found_new_tables = false;
        while (!files->empty()) {
            auto it = files->begin();

            // Find all overlapping L0 tables.
            while (it != files->end()) {
                auto *file = it->second;
                if (user_comparator->Compare(lower,
                                             file->largest.user_key()) >
                    0) {
                    it++;
                    continue;
                }
                if (user_comparator->Compare(upper,
                                             file->smallest.user_key()) <
                    0) {
                    it++;
                    continue;
                }
                // overlap.
                found_new_tables = true;
                compaction->inputs_[which].push_back(file);
                // Take the smallest user key.
                if (user_comparator->Compare(file->smallest.user_key(), lower) <
                    0) {
                    *new_lower = file->smallest.user_key();
                }
                if (user_comparator->Compare(file->largest.user_key(), upper) >
                    0) {
                    *new_upper = file->largest.user_key();
                }
                it = files->erase(it);
            }
        }
        return found_new_tables;
    }

    void Version::ComputeOverlappingFilesForRange(
            std::map<uint64_t, FileMetaData *> *l0files,
            std::map<uint64_t, FileMetaData *> *l1files,
            const Options *options,
            std::vector<Compaction *> *compactions,
            const leveldb::Comparator *user_comparator) {
        // Compute overlapping sstables at L0.
        while (!l0files->empty()) {
            Slice lower = l0files->begin()->second->smallest.user_key();
            Slice upper = l0files->begin()->second->largest.user_key();;
            Slice new_lower;
            Slice new_upper;

            Compaction *compaction = new Compaction(options, 0);
            while (true) {
                bool found_new_l0 = ComputeOverlappingFilesInRange(l0files, 0,
                                                                   compaction,
                                                                   lower, upper,
                                                                   &new_lower,
                                                                   &new_upper,
                                                                   user_comparator);
                bool found_new_l1 = ComputeOverlappingFilesInRange(l1files, 1,
                                                                   compaction,
                                                                   lower, upper,
                                                                   &new_lower,
                                                                   &new_upper,
                                                                   user_comparator);
                lower = new_lower;
                upper = new_upper;
                if (found_new_l0 || found_new_l1) {
                    continue;
                } else {
                    break;
                }
            }
            compactions->push_back(compaction);

            if (compactions->size() ==
                options->max_num_coordinated_compaction_nonoverlapping_sets) {
                break;
            }
        }
    }

    void Version::ComputeOverlappingFilesStats(
            std::map<uint64_t, leveldb::FileMetaData *> *files,
            std::vector<OverlappingStats> *num_overlapping,
            const Comparator *user_comparator) {
        // Compute overlapping sstables at L0.
        while (!files->empty()) {
            uint64_t pivot = files->begin()->first;
            Slice lower = files->begin()->second->smallest.user_key();
            Slice upper = files->begin()->second->largest.user_key();
            OverlappingStats stats = {};
            stats.num_overlapping_tables = 1;
            stats.total_size = files->begin()->second->file_size;

            files->erase(pivot);
            auto it = files->begin();

            // Find all overlapping tables.
            while (it != files->end()) {
                auto *file = it->second;
                if (user_comparator->Compare(lower, file->largest.user_key()) >
                    0) {
                    it++;
                    continue;
                }
                if (user_comparator->Compare(upper, file->smallest.user_key()) <
                    0) {
                    it++;
                    continue;
                }
                // overlap.
                stats.num_overlapping_tables++;
                stats.total_size += file->file_size;
                // Take the smallest user key.
                if (user_comparator->Compare(file->smallest.user_key(), lower) <
                    0) {
                    lower = file->smallest.user_key();
                }
                if (user_comparator->Compare(file->largest.user_key(), upper) >
                    0) {
                    upper = file->largest.user_key();
                }
                it = files->erase(it);
                it = files->begin();
            }
            nova::str_to_int(lower.data(), &stats.smallest, lower.size());
            nova::str_to_int(upper.data(), &stats.largest, upper.size());
            num_overlapping->push_back(stats);
        }

        {
            auto comp = [&](const OverlappingStats &a,
                            const OverlappingStats &b) {
                return a.smallest < b.smallest;
            };
            std::sort(num_overlapping->begin(), num_overlapping->end(),
                      comp);
        }
    }

    void Version::QueryStats(leveldb::DBStats *stats,
                             const Comparator *user_comparator) {
        std::map<uint64_t, FileMetaData *> all_fnfile;
        std::map<uint64_t, FileMetaData *> new_fnfile;

        for (int level = 0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData *> &files = files_[level];
            if (level == 0) {
                stats->nsstables += files.size();
            }
            for (size_t i = 0; i < files.size(); i++) {
                stats->dbsize += files[i]->file_size;
                stats->sstable_size_dist[files[i]->file_size / 1024 /
                                         1024] += 1;
                if (level == 0) {
                    all_fnfile[files[i]->number] = files[i];
                    if (last_fnfile.find(files[i]->number) ==
                        last_fnfile.end()) {
                        new_fnfile[files[i]->number] = files[i];
                        last_fnfile[files[i]->number] = files[i];
                    }
                }
            }
        }

        stats->new_l0_sstables_since_last_query = new_fnfile.size();
        ComputeOverlappingFilesPerTable(&all_fnfile,
                                        &stats->num_overlapping_sstables_per_table,
                                        user_comparator);
        ComputeOverlappingFilesStats(&all_fnfile,
                                     &stats->num_overlapping_sstables,
                                     user_comparator);
        RDMA_ASSERT(all_fnfile.empty());

        ComputeOverlappingFilesPerTable(&new_fnfile,
                                        &stats->num_overlapping_sstables_per_table_since_last_query,
                                        user_comparator);
        ComputeOverlappingFilesStats(&new_fnfile,
                                     &stats->num_overlapping_sstables_since_last_query,
                                     user_comparator);
        RDMA_ASSERT(new_fnfile.empty());
    }

    std::string Version::DebugString() const {
        std::string r;
        r.append("compaction-level: ");
        AppendNumberTo(&r, compaction_level_);

        r.append("compaction-score: ");
        r.append(std::to_string(compaction_score_));
        r.append("\n");

        for (int level = 0; level < config::kNumLevels; level++) {
            r.append("sstables,");
            AppendNumberTo(&r, level);
            r.append(",");
            AppendNumberTo(&r, files_[level].size());
            r.append(",");
            uint64_t size = 0;
            const std::vector<FileMetaData *> &files = files_[level];
            for (size_t i = 0; i < files.size(); i++) {
                size += files[i]->file_size;
            }
            AppendNumberTo(&r, size);
            r.append("\n");

            // E.g.,
            //   --- level 1 ---
            //   17:123['a' .. 'd']
            //   20:43['e' .. 'g']
            r.append("--- level ");
            AppendNumberTo(&r, level);
            r.append(" ---\n");
            for (size_t i = 0; i < files.size(); i++) {
                r.append(files[i]->DebugString());
                r.append("\n");
            }
        }
        return r;
    }

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
    class VersionSet::Builder {
    private:
        // Helper to sort by v->files_[file_number].smallest
        struct BySmallestKey {
            const InternalKeyComparator *internal_comparator;

            bool operator()(FileMetaData *f1, FileMetaData *f2) const {
                int r = internal_comparator->Compare(f1->smallest,
                                                     f2->smallest);
                if (r != 0) {
                    return (r < 0);
                } else {
                    // Break ties by file number
                    return (f1->number < f2->number);
                }
            }
        };

        typedef std::set<FileMetaData *, BySmallestKey> FileSet;
        struct LevelState {
            std::set<uint64_t> deleted_files;
            FileSet *added_files;
        };

        VersionSet *vset_;
        Version *base_;
        LevelState levels_[config::kNumLevels];

    public:
        // Initialize a builder with the files from *base and other info from *vset
        Builder(VersionSet *vset, Version *base) : vset_(vset), base_(base) {
            Version *v = vset->versions_[base->version_id_].Ref();
            RDMA_ASSERT(v == base);
            BySmallestKey cmp;
            cmp.internal_comparator = &vset_->icmp_;
            for (int level = 0; level < config::kNumLevels; level++) {
                levels_[level].added_files = new FileSet(cmp);
            }
        }

        ~Builder() {
            for (int level = 0; level < config::kNumLevels; level++) {
                const FileSet *added = levels_[level].added_files;
                std::vector<FileMetaData *> to_unref;
                to_unref.reserve(added->size());
                for (FileSet::const_iterator it = added->begin();
                     it != added->end();
                     ++it) {
                    to_unref.push_back(*it);
                }
                delete added;
                for (uint32_t i = 0; i < to_unref.size(); i++) {
                    FileMetaData *f = to_unref[i];
                    f->refs--;
                    if (f->refs <= 0) {
//                        delete f;
                    }
                }
            }
            vset_->versions_[base_->version_id_].Unref(vset_->dbname_);
        }

        // Apply all of the edits in *edit to the current state.
        void Apply(VersionEdit *edit) {
            // Update compaction pointers
//            for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
//                const int level = edit->compact_pointers_[i].first;
//                vset_->compact_pointer_[level] =
//                        edit->compact_pointers_[i].second.Encode().ToString();
//            }

            // Delete files
            for (const auto &deleted_file_set_kvp : edit->deleted_files_) {
                const int level = deleted_file_set_kvp.first;
                const uint64_t number = deleted_file_set_kvp.second.fnumber;
                levels_[level].deleted_files.insert(number);
            }

            // Add new files
            for (size_t i = 0; i < edit->new_files_.size(); i++) {
                const int level = edit->new_files_[i].first;
                FileMetaData *f = new FileMetaData(edit->new_files_[i].second);
                f->refs = 1;

                // We arrange to automatically compact this file after
                // a certain number of seeks.  Let's assume:
                //   (1) One seek costs 10ms
                //   (2) Writing or reading 1MB costs 10ms (100MB/s)
                //   (3) A compaction of 1MB does 25MB of IO:
                //         1MB read from this level
                //         10-12MB read from next level (boundaries may be misaligned)
                //         10-12MB written to next level
                // This implies that 25 seeks cost the same as the compaction
                // of 1MB of data.  I.e., one seek costs approximately the
                // same as the compaction of 40KB of data.  We are a little
                // conservative and allow approximately one seek for every 16KB
                // of data before triggering a compaction.
                f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
                if (f->allowed_seeks < 100) f->allowed_seeks = 100;

                levels_[level].deleted_files.erase(f->number);
                levels_[level].added_files->insert(f);
            }
        }

        // Save the current state in *v.
        void SaveTo(Version *v) {
            BySmallestKey cmp;
            cmp.internal_comparator = &vset_->icmp_;
            for (int level = 0; level < config::kNumLevels; level++) {
                // Merge the set of added files with the set of pre-existing files.
                // Drop any deleted files.  Store the result in *v.
                const std::vector<FileMetaData *> &base_files = base_->files_[level];
                std::vector<FileMetaData *>::const_iterator base_iter = base_files.begin();
                std::vector<FileMetaData *>::const_iterator base_end = base_files.end();
                const FileSet *added_files = levels_[level].added_files;
                v->files_[level].reserve(
                        base_files.size() + added_files->size());
                for (const auto &added_file : *added_files) {
                    // Add all smaller files listed in base_
                    for (std::vector<FileMetaData *>::const_iterator bpos =
                            std::upper_bound(base_iter, base_end, added_file,
                                             cmp);
                         base_iter != bpos; ++base_iter) {
                        MaybeAddFile(v, level, *base_iter);
                    }
                    RDMA_ASSERT(added_file->compaction_status !=
                                FileCompactionStatus::COMPACTING)
                        << fmt::format("{}@{}", added_file->number, level);
                    MaybeAddFile(v, level, added_file);
                }

                // Add remaining base files
                for (; base_iter != base_end; ++base_iter) {
                    MaybeAddFile(v, level, *base_iter);
                }

#ifndef NDEBUG
                // Make sure there is no overlap in levels > 0
                if (level > 0) {
                    for (uint32_t i = 1; i < v->files_[level].size(); i++) {
                        const InternalKey &prev_end = v->files_[level][i -
                                                                       1]->largest;
                        const InternalKey &this_begin = v->files_[level][i]->smallest;
                        if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
                            fprintf(stderr,
                                    "overlapping ranges in same level %s vs. %s\n",
                                    prev_end.DebugString().c_str(),
                                    this_begin.DebugString().c_str());
                            abort();
                        }
                    }
                }
#endif
            }
        }

        void MaybeAddFile(Version *v, int level, FileMetaData *f) {
            if (levels_[level].deleted_files.count(f->number) > 0) {
                // File is deleted: do nothing
            } else {
                std::vector<FileMetaData *> *files = &v->files_[level];
                if (level > 0 && !files->empty()) {
                    // Must not overlap
                    RDMA_ASSERT(vset_->icmp_.Compare(
                            (*files)[files->size() - 1]->largest,
                            f->smallest) < 0)
                        << fmt::format("level:{} fn:{} v:{}", level, f->number,
                                       v->DebugString());
                }
                f->refs++;
                files->push_back(f);
                RDMA_ASSERT(f->number < MAX_LIVE_MEMTABLES);
//                RDMA_ASSERT(v->fn_files_[f->number] == nullptr)
//                    << fmt::format("{}@{}", f->number, level);
                v->fn_files_[f->number] = f;
            }
        }
    };

    VersionSet::VersionSet(const std::string &dbname, const Options *options,
                           TableCache *table_cache,
                           const InternalKeyComparator *cmp)
            : env_(options->env),
              dbname_(dbname),
              options_(options),
              table_cache_(table_cache),
              icmp_(*cmp),
              next_file_number_(2),
              last_sequence_(0),
              descriptor_file_(nullptr),
              descriptor_log_(nullptr),
              version_id_seq_(0),
              dummy_versions_(this, version_id_seq_++),
              current_(nullptr) {
        AppendVersion(new Version(this, version_id_seq_++));
    }

    VersionSet::~VersionSet() {
        versions_[current_->version_id_].Unref(dbname_);
        assert(dummy_versions_.next_ ==
               &dummy_versions_);  // List must be empty
        delete descriptor_log_;
        delete descriptor_file_;
    }

    void VersionSet::AppendVersion(Version *v) {
        // Make "v" current
        assert(v->refs_ == 0);
        assert(v != current_);
        if (current_ != nullptr) {
            versions_[current_->version_id_].Unref(dbname_);
        }
        current_ = v;

        // Append to linked list
        v->prev_ = dummy_versions_.prev_;
        v->next_ = &dummy_versions_;
        v->prev_->next_ = v;
        v->next_->prev_ = v;

        versions_[v->version_id_].SetVersion(v);
        current_version_id_.store(v->version_id_);
    }

    void VersionSet::AppendChangesToManifest(leveldb::VersionEdit *edit,
                                             NovaCCMemFile *manifest_file,
                                             uint32_t stoc_id) {
        if (!manifest_file) {
            return;
        }

        manifest_lock_.lock();
        edit->SetNextFile(next_file_number_);
        edit->SetLastSequence(last_sequence_);
        char *edit_str = manifest_file->Buf();
        uint32_t msg_size = edit->EncodeTo(edit_str);

        if (RDMA_LOG_LEVEL == rdmaio::DEBUG) {
            VersionEdit decode;
            RDMA_ASSERT(decode.DecodeFrom(Slice(edit_str, msg_size)).ok());
            RDMA_LOG(rdmaio::DEBUG) << decode.DebugString();
        }

        RDMA_ASSERT(manifest_file->SyncAppend(Slice(edit_str, msg_size),
                                              stoc_id).ok());
        manifest_lock_.unlock();
    }

    Status VersionSet::LogAndApply(VersionEdit *edit, Version *v) {
        {
            Builder builder(this, current_);
            builder.Apply(edit);
            builder.SaveTo(v);
        }
        Finalize(v);
        AppendVersion(v);
        RDMA_LOG(rdmaio::DEBUG)
            << fmt::format("New version: {}", v->DebugString());
        return Status::OK();
    }

    Status VersionSet::Recover(Slice record,
                               std::vector<VersionSubRange> *subrange_edits) {
        uint64_t next_file = 0;
        uint64_t last_sequence = 0;
        Builder builder(this, current_);
        Slice input = record;
        Slice next;
        {
            while (true) {
                VersionEdit edit;

                Status s = edit.DecodeFrom(input, &next);
                input = next;
                std::set<uint32_t> subrange_ids;
                if (!s.ok()) {
                    break;
                }
                RDMA_LOG(rdmaio::INFO)
                    << fmt::format("stats:{} edit:{}", s.ToString(),
                                   edit.DebugString());

                builder.Apply(&edit);
                if (edit.has_next_file_number_) {
                    next_file = std::max(next_file, edit.next_file_number_);
                }

                if (edit.has_last_sequence_) {
                    last_sequence = std::max(last_sequence,
                                             edit.last_sequence_);
                }

                // Only retain the latest subrange.
                for (int i = edit.new_subranges_.size() - 1; i >= 0; i--) {
                    if (subrange_edits->empty()) {
                        subrange_edits->resize(
                                options_->num_memtable_partitions);
                    }

                    VersionSubRange &sr = edit.new_subranges_[i];
                    if (subrange_ids.find(sr.subrange_id) !=
                        subrange_ids.end()) {
                        continue;
                    }
                    subrange_ids.insert(sr.subrange_id);
                    (*subrange_edits)[sr.subrange_id] = sr;
                }
            }
        }

        Version *v = new Version(this, version_id_seq_++);
        builder.SaveTo(v);
        // Install recovered version
        Finalize(v);
        AppendVersion(v);
        next_file_number_ = next_file + 1;
        last_sequence_ = last_sequence;
        return Status::OK();
    }

    bool VersionSet::ReuseManifest(const std::string &dscname,
                                   const std::string &dscbase) {
        if (!options_->reuse_logs) {
            return false;
        }
        FileType manifest_type;
        uint64_t manifest_number;
        uint64_t manifest_size;
        if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
            manifest_type != kDescriptorFile ||
            !env_->GetFileSize(dscname, &manifest_size).ok() ||
            // Make new compacted MANIFEST if old one is too big
            manifest_size >= TargetFileSize(options_)) {
            return false;
        }

        assert(descriptor_file_ == nullptr);
        assert(descriptor_log_ == nullptr);
        Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
        if (!r.ok()) {
            Log(options_->info_log, "Reuse MANIFEST: %s\n",
                r.ToString().c_str());
            assert(descriptor_file_ == nullptr);
            return false;
        }

        Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
        descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
        return true;
    }

    void VersionSet::MarkFileNumberUsed(uint64_t number) {
        if (next_file_number_ <= number) {
            next_file_number_ = number + 1;
        }
    }

    void VersionSet::Finalize(Version *v) {
        // Precomputed best level for next compaction
        int best_level = -1;
        double best_score = -1;

        for (int level = 0; level < config::kNumLevels - 1; level++) {
            double score;
            if (level == 0) {
                // We treat level-0 specially by bounding the number of files
                // instead of number of bytes for two reasons:
                //
                // (1) With larger write-buffer sizes, it is nice not to do too
                // many level-0 compactions.
                //
                // (2) The files in level-0 are merged on every read and
                // therefore we wish to avoid too many files when the individual
                // file size is small (perhaps because of a small write-buffer
                // setting, or very high compression ratios, or lots of
                // overwrites/deletions).
                score = v->files_[level].size() /
                        static_cast<double>(config::kL0_CompactionTrigger);
            } else {
                // Compute the ratio of current size to size limit.
                const uint64_t level_bytes = TotalFileSize(v->files_[level]);
                score =
                        static_cast<double>(level_bytes) /
                        MaxBytesForLevel(*options_, level);
            }

            if (score > best_score) {
                best_level = level;
                best_score = score;
            }
        }

        v->compaction_level_ = best_level;
        v->compaction_score_ = best_score;
    }

    Status VersionSet::WriteSnapshot(log::Writer *log) {
        // TODO: Break up into multiple records to reduce memory usage on recovery?

        // Save metadata
        VersionEdit edit;
        edit.SetComparatorName(icmp_.user_comparator()->Name());

        // Save compaction pointers
        for (int level = 0; level < config::kNumLevels; level++) {
            if (!compact_pointer_[level].empty()) {
                InternalKey key;
                key.DecodeFrom(compact_pointer_[level]);
                edit.SetCompactPointer(level, key);
            }
        }

        // Save files
        for (int level = 0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData *> &files = current_->files_[level];
            for (size_t i = 0; i < files.size(); i++) {
                const FileMetaData *f = files[i];
                edit.AddFile(level, 0, f->number, f->file_size,
                             f->converted_file_size,
                             f->flush_timestamp,
                             f->smallest,
                             f->largest,
                             f->meta_block_handle,
                             f->data_block_group_handles);
            }
        }

        std::string record;
//        edit.EncodeTo(&record);
        return log->AddRecord(record);
    }

    int VersionSet::NumLevelFiles(int level) const {
        assert(level >= 0);
        assert(level < config::kNumLevels);
        return current_->files_[level].size();
    }

    const char *VersionSet::LevelSummary(uint32_t thread_id) const {
        // Update code if kNumLevels changes
//        std::string summary = "files[ ";
//        for (int i = 0; i < 7; i++) {
//            summary += std::to_string(current_->files_[i].size()) + " ";
//        }
//        summary += "]";
//        Log(options_->info_log, "bg[%d]: compacted to: %s", thread_id,
//            summary.c_str());
        return nullptr;
    }

    uint64_t
    VersionSet::ApproximateOffsetOf(Version *v, const InternalKey &ikey) {
        uint64_t result = 0;
        for (int level = 0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData *> &files = v->files_[level];
            for (size_t i = 0; i < files.size(); i++) {
                if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
                    // Entire file is before "ikey", so just add the file size
                    result += files[i]->file_size;
                } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
                    // Entire file is after "ikey", so ignore
                    if (level > 0) {
                        // Files other than level 0 are sorted by meta->smallest, so
                        // no further files in this level will contain data for
                        // "ikey".
                        break;
                    }
                } else {
                    // "ikey" falls in the range for this table.  Add the
                    // approximate offset of "ikey" within the table.
                    // TODO: Unsupported.
//                    Table *tableptr;
//                    Iterator *iter = table_cache_->NewIterator(
//                            AccessCaller::kApproximateSize,
//                            ReadOptions(), files[i]->number, level,
//                            files[i]->file_size, &tableptr);
//                    if (tableptr != nullptr) {
//                        result += tableptr->ApproximateOffsetOf(ikey.Encode());
//                    }
//                    delete iter;
                }
            }
        }
        return result;
    }

    void VersionSet::DeleteObsoleteVersions() {
        Version *v = dummy_versions_.next_;
        while (v != &dummy_versions_) {
            Version *next = v->next_;
            if (versions_[v->version_id()].deleted) {
                v->Destroy();
                delete v;
                v = nullptr;
            }
            v = next;
        }

    }

    void VersionSet::AddLiveFiles(std::set<uint64_t> *live) {
        for (Version *v = dummy_versions_.next_; v != &dummy_versions_;
             v = v->next_) {
            for (int level = 0; level < config::kNumLevels; level++) {
                const std::vector<FileMetaData *> &files = v->files_[level];
                for (size_t i = 0; i < files.size(); i++) {
                    live->insert(files[i]->number);
                }
            }
        }
    }

    int64_t VersionSet::NumLevelBytes(int level) const {
        assert(level >= 0);
        assert(level < config::kNumLevels);
        return TotalFileSize(current_->files_[level]);
    }

    int64_t VersionSet::MaxNextLevelOverlappingBytes() {
        int64_t result = 0;
        std::vector<FileMetaData *> overlaps;
        for (int level = 1; level < config::kNumLevels - 1; level++) {
            for (size_t i = 0; i < current_->files_[level].size(); i++) {
                const FileMetaData *f = current_->files_[level][i];
                current_->GetOverlappingInputs(level + 1, &f->smallest,
                                               &f->largest,
                                               &overlaps);
                const int64_t sum = TotalFileSize(overlaps);
                if (sum > result) {
                    result = sum;
                }
            }
        }
        return result;
    }

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
    void VersionSet::GetRange(const std::vector<FileMetaData *> &inputs,
                              InternalKey *smallest, InternalKey *largest) {
        assert(!inputs.empty());
        smallest->Clear();
        largest->Clear();
        for (size_t i = 0; i < inputs.size(); i++) {
            FileMetaData *f = inputs[i];
            if (i == 0) {
                *smallest = f->smallest;
                *largest = f->largest;
            } else {
                if (icmp_.Compare(f->smallest, *smallest) < 0) {
                    *smallest = f->smallest;
                }
                if (icmp_.Compare(f->largest, *largest) > 0) {
                    *largest = f->largest;
                }
            }
        }
    }

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
    void VersionSet::GetRange2(const std::vector<FileMetaData *> &inputs1,
                               const std::vector<FileMetaData *> &inputs2,
                               InternalKey *smallest, InternalKey *largest) {
        std::vector<FileMetaData *> all = inputs1;
        all.insert(all.end(), inputs2.begin(), inputs2.end());
        GetRange(all, smallest, largest);
    }

    void VersionSet::AddCompactedInputs(leveldb::Compaction *c,
                                        std::map<uint64_t, leveldb::FileMetaData> *map) {
        int num = 0;
        for (int which = 0; which < 2; which++) {
            if (!c->inputs_[which].empty()) {
                const std::vector<FileMetaData *> &files = c->inputs_[which];
                for (size_t i = 0; i < files.size(); i++) {
                    (*map)[files[i]->number] = *files[i];
                }
            }
        }
    }

    Iterator *
    VersionSet::MakeInputIterator(Compaction *c, EnvBGThread *bg_thread) {
        ReadOptions options;
        options.verify_checksums = options_->paranoid_checks;
        options.fill_cache = false;
        options.thread_id = bg_thread->thread_id();
        options.mem_manager = bg_thread->mem_manager();
        options.dc_client = bg_thread->dc_client();

        // Level-0 files have to be merged together.  For other levels,
        // we will make a concatenating iterator per level.
        // TODO(opt): use concatenating iterator for level-0 if there is no overlap
        const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
        Iterator **list = new Iterator *[space];
        int num = 0;
        for (int which = 0; which < 2; which++) {
            if (!c->inputs_[which].empty()) {
                if (c->level() + which == 0) {
                    const std::vector<FileMetaData *> &files = c->inputs_[which];
                    for (size_t i = 0; i < files.size(); i++) {
                        list[num++] = table_cache_->NewIterator(
                                AccessCaller::kCompaction, options,
                                *files[i],
                                files[i]->number,
                                c->level() +
                                which,
                                files[i]->converted_file_size);
                    }
                } else {
                    // Create concatenating iterator for the files from this level
                    BlockReadContext context = {
                            .caller = AccessCaller::kCompaction,
                            .file_number = 0,
                            .level = c->level() + which,
                    };
                    list[num++] = NewTwoLevelIterator(
                            new Version::LevelFileNumIterator(icmp_,
                                                              &c->inputs_[which]),
                            context,
                            &GetFileIterator, c->input_version_, options);
                }
            }
        }
        assert(num <= space);
        Iterator *result = NewMergingIterator(&icmp_, list, num);
        delete[] list;
        return result;
    }

    void VersionSet::ComputeNonOverlappingSet(
            std::vector<leveldb::Compaction *> *compactions) {
        Version *current = versions_[current_->version_id()].Ref();
        std::map<uint64_t, FileMetaData *> l0files;
        std::map<uint64_t, FileMetaData *> l1files;
        current->ComputeOverlappingFilesForRange(&l0files, &l1files, options_,
                                                 compactions,
                                                 icmp_.user_comparator());

        // Remove files in a set so that its number of files does not exceed max.
        for (int i = 0; i < compactions->size(); i++) {
            auto compaction = (*compactions)[i];
            while (true) {
                int l0files = compaction->num_input_files(0);
                int l1files = compaction->num_input_files(1);
                if (l0files + l1files <=
                    options_->max_num_sstables_in_nonoverlapping_set) {
                    break;
                }
                // TODO: If one L0 file overlap with all L1 files, should we reinsert its entries back to memtables?
                // cut off l0 sstables.
                int new_l0files =
                        options_->max_num_sstables_in_nonoverlapping_set -
                        l1files;
                compaction->inputs_[0].resize(new_l0files);
                compaction->inputs_[1].clear();
                // recompute the overlapping tables at L1.
                InternalKey *smallest = nullptr;
                InternalKey *largest = nullptr;

                for (int i = 0; i < compaction->inputs_[0].size(); i++) {
                    auto meta = compaction->inputs_[0][i];
                    if (smallest == nullptr ||
                        icmp_.user_comparator()->Compare(
                                meta->smallest.user_key(),
                                smallest->user_key()) <
                        0) {
                        smallest = &meta->smallest;
                    }
                    if (largest == nullptr ||
                        icmp_.user_comparator()->Compare(
                                meta->largest.user_key(),
                                largest->user_key()) <
                        0) {
                        largest = &meta->largest;
                    }
                }
                current_->GetOverlappingInputs(1, smallest, largest,
                                               &compaction->inputs_[1]);
                int new_l1files = compaction->num_input_files(1);
                RDMA_ASSERT(new_l1files + new_l0files <=
                            options_->max_num_sstables_in_nonoverlapping_set);
                break;
            }
        }
    }

    Compaction *VersionSet::PickCompaction(uint32_t thread_id) {
        Compaction *c;
        int level = current_->compaction_level_;
        assert(level == 0);
        assert(level + 1 < config::kNumLevels);
        c = new Compaction(options_, level);

        // Pick the first file that comes after compact_pointer_[level]
        for (size_t i = 0; i < current_->files_[level].size(); i++) {
            FileMetaData *f = current_->files_[level][i];
            if (compact_pointer_[level].empty() ||
                icmp_.Compare(f->largest.Encode(),
                              compact_pointer_[level]) > 0) {
                c->inputs_[0].push_back(f);
                break;
            }
        }
        if (c->inputs_[0].empty()) {
            // Wrap-around to the beginning of the key space
            c->inputs_[0].push_back(current_->files_[level][0]);
        }

        c->input_version_ = current_;
        versions_[current_->version_id()].Ref();

        // Files in level 0 may overlap each other, so pick up all overlapping ones
        {
            InternalKey &smallest = c->inputs_[0][0]->smallest;
            InternalKey &largest = c->inputs_[0][0]->largest;
            current_->GetOverlappingInputs(0, &smallest, &largest,
                                           &c->inputs_[0]);
            assert(!c->inputs_[0].empty());
        }

        InternalKey smallest, largest;
        GetRange(c->inputs_[0], &smallest, &largest);
        current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                       &c->inputs_[1]);
        compact_pointer_[level] = largest.Encode().ToString();

        std::string files;
        uint32_t nfiles = 0;
        for (int which = 0; which < 2; which++) {
            for (int i = 0; i < c->inputs_[which].size(); i++) {
                auto f = c->inputs_[which][i];
                nfiles += 1;
                files += fmt::format("{}@{}-{}-{} ", f->number,
                                     c->level_ + which,
                                     f->smallest.DebugString(),
                                     f->largest.DebugString());
            }
        }

        if (nfiles <= 1) {
            delete c;
            return nullptr;
        }

        std::string output = fmt::format("bg[{}]: Compaction picks files {}",
                                         thread_id, files);
        Log(options_->info_log, "%s", output.c_str());
        RDMA_LOG(rdmaio::INFO) << output;
        return c;
    }


// Finds the largest key in a vector of files. Returns true if files it not
// empty.
    bool FindLargestKey(const InternalKeyComparator &icmp,
                        const std::vector<FileMetaData *> &files,
                        InternalKey *largest_key) {
        if (files.empty()) {
            return false;
        }
        *largest_key = files[0]->largest;
        for (size_t i = 1; i < files.size(); ++i) {
            FileMetaData *f = files[i];
            if (icmp.Compare(f->largest, *largest_key) > 0) {
                *largest_key = f->largest;
            }
        }
        return true;
    }

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
    FileMetaData *FindSmallestBoundaryFile(
            const InternalKeyComparator &icmp,
            const std::vector<FileMetaData *> &level_files,
            const InternalKey &largest_key) {
        const Comparator *user_cmp = icmp.user_comparator();
        FileMetaData *smallest_boundary_file = nullptr;
        for (size_t i = 0; i < level_files.size(); ++i) {
            FileMetaData *f = level_files[i];
            if (icmp.Compare(f->smallest, largest_key) > 0 &&
                user_cmp->Compare(f->smallest.user_key(),
                                  largest_key.user_key()) ==
                0) {
                if (smallest_boundary_file == nullptr ||
                    icmp.Compare(f->smallest,
                                 smallest_boundary_file->smallest) < 0) {
                    smallest_boundary_file = f;
                }
            }
        }
        return smallest_boundary_file;
    }

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
    void AddBoundaryInputs(const InternalKeyComparator &icmp,
                           const std::vector<FileMetaData *> &level_files,
                           std::vector<FileMetaData *> *compaction_files) {
        InternalKey largest_key;

        // Quick return if compaction_files is empty.
        if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
            return;
        }

        bool continue_searching = true;
        while (continue_searching) {
            FileMetaData *smallest_boundary_file =
                    FindSmallestBoundaryFile(icmp, level_files, largest_key);

            // If a boundary file was found advance largest_key, otherwise we're done.
            if (smallest_boundary_file != NULL) {
                compaction_files->push_back(smallest_boundary_file);
                largest_key = smallest_boundary_file->largest;
            } else {
                continue_searching = false;
            }
        }
    }

    void VersionSet::SetupOtherInputs(Compaction *c) {
        const int level = c->level();
        assert(level >= 0);
        InternalKey smallest, largest;

        AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
        GetRange(c->inputs_[0], &smallest, &largest);

        current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                       &c->inputs_[1]);

        // Get entire range covered by compaction
        InternalKey all_start, all_limit;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

        // See if we can grow the number of inputs in "level" without
        // changing the number of "level+1" files we pick up.
        if (!c->inputs_[1].empty()) {
            std::vector<FileMetaData *> expanded0;
            current_->GetOverlappingInputs(level, &all_start, &all_limit,
                                           &expanded0);
            AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
            const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
            const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
            const int64_t expanded0_size = TotalFileSize(expanded0);
            if (expanded0.size() > c->inputs_[0].size() &&
                inputs1_size + expanded0_size <
                ExpandedCompactionByteSizeLimit(options_)) {
                InternalKey new_start, new_limit;
                GetRange(expanded0, &new_start, &new_limit);
                std::vector<FileMetaData *> expanded1;
                current_->GetOverlappingInputs(level + 1, &new_start,
                                               &new_limit,
                                               &expanded1);
                if (expanded1.size() == c->inputs_[1].size()) {
                    Log(options_->info_log,
                        "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
                        level, int(c->inputs_[0].size()),
                        int(c->inputs_[1].size()),
                        long(inputs0_size), long(inputs1_size),
                        int(expanded0.size()),
                        int(expanded1.size()), long(expanded0_size),
                        long(inputs1_size));
                    smallest = new_start;
                    largest = new_limit;
                    c->inputs_[0] = expanded0;
                    c->inputs_[1] = expanded1;
                    GetRange2(c->inputs_[0], c->inputs_[1], &all_start,
                              &all_limit);
                }
            }
        }

        // Compute the set of grandparent files that overlap this compaction
        // (parent == level+1; grandparent == level+2)
        if (level + 2 < config::kNumLevels) {
            current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                           &c->grandparents_);
        }

        // Update the place where we will do the next compaction for this level.
        // We update this immediately instead of waiting for the VersionEdit
        // to be applied so that if the compaction fails, we will try a different
        // key range next time.
        compact_pointer_[level] = largest.Encode().ToString();
        c->edit_.SetCompactPointer(level, largest);
    }

    Compaction *VersionSet::CompactRange(int level, const InternalKey *begin,
                                         const InternalKey *end) {
        std::vector<FileMetaData *> inputs;
        current_->GetOverlappingInputs(level, begin, end, &inputs);
        if (inputs.empty()) {
            return nullptr;
        }

        // Avoid compacting too much in one shot in case the range is large.
        // But we cannot do this for level-0 since level-0 files can overlap
        // and we must not pick one file and drop another older file if the
        // two files overlap.
        if (level > 0) {
            const uint64_t limit = MaxFileSizeForLevel(options_, level);
            uint64_t total = 0;
            for (size_t i = 0; i < inputs.size(); i++) {
                uint64_t s = inputs[i]->file_size;
                total += s;
                if (total >= limit) {
                    inputs.resize(i + 1);
                    break;
                }
            }
        }

        Compaction *c = new Compaction(options_, level);
        c->input_version_ = current_;
//        c->input_version_->Ref();
        c->inputs_[0] = inputs;
        SetupOtherInputs(c);
        return c;
    }

    Compaction::Compaction(const Options *options, int level)
            : level_(level),
              max_output_file_size_(MaxFileSizeForLevel(options, level)),
              input_version_(nullptr),
              grandparent_index_(0),
              seen_key_(false),
              overlapped_bytes_(0) {
        for (int i = 0; i < config::kNumLevels; i++) {
            level_ptrs_[i] = 0;
        }
    }

    Compaction::~Compaction() {
        if (input_version_ != nullptr) {
//            input_version_->Unref();
        }
    }

    bool Compaction::IsTrivialMove() const {
        const VersionSet *vset = input_version_->vset_;
        // Avoid a move if there is lots of overlapping grandparent data.
        // Otherwise, the move could create a parent file that will require
        // a very expensive merge later on.
        return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
                TotalFileSize(grandparents_) <=
                MaxGrandParentOverlapBytes(vset->options_));
    }

    void Compaction::AddInputDeletions(VersionEdit *edit) {
        for (int which = 0; which < 2; which++) {
            for (size_t i = 0; i < inputs_[which].size(); i++) {
                int delete_level = level_ + which;
                auto *f = inputs_[which][i];
                edit->DeleteFile(delete_level, f->memtable_id, f->number);
            }
        }
    }

    bool Compaction::IsBaseLevelForKey(const Slice &user_key) {
        // Maybe use binary search to find right entry instead of linear search?
        const Comparator *user_cmp = input_version_->vset_->icmp_.user_comparator();
        for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
            const std::vector<FileMetaData *> &files = input_version_->files_[lvl];
            while (level_ptrs_[lvl] < files.size()) {
                FileMetaData *f = files[level_ptrs_[lvl]];
                if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
                    // We've advanced far enough
                    if (user_cmp->Compare(user_key, f->smallest.user_key()) >=
                        0) {
                        // Key falls in this file's range, so definitely not base level
                        return false;
                    }
                    break;
                }
                level_ptrs_[lvl]++;
            }
        }
        return true;
    }

    bool Compaction::ShouldStopBefore(const Slice &internal_key) {
        const VersionSet *vset = input_version_->vset_;
        // Scan to find earliest grandparent file that contains key.
        const InternalKeyComparator *icmp = &vset->icmp_;
        while (grandparent_index_ < grandparents_.size() &&
               icmp->Compare(internal_key,
                             grandparents_[grandparent_index_]->largest.Encode()) >
               0) {
            if (seen_key_) {
                overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
            }
            grandparent_index_++;
        }
        seen_key_ = true;

        if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
            // Too much overlap for current output; start new output
            overlapped_bytes_ = 0;
            return true;
        } else {
            return false;
        }
    }

    void AtomicVersion::SetVersion(leveldb::Version *v) {
        mutex.lock();
        RDMA_ASSERT(!version);
        version = v;
        v->Ref();
        mutex.unlock();
    }

    Version *AtomicVersion::Ref() {
        Version *v = nullptr;
        mutex.lock();
        if (version != nullptr) {
            v = version;
            v->Ref();
        }
        mutex.unlock();
        return v;
    }

    void AtomicVersion::Unref(const std::string &dbname) {
        mutex.lock();
        if (version) {
            uint32_t vid = version->version_id();
            uint32_t refs = version->Unref();
            if (refs <= 0) {
//                RDMA_LOG(rdmaio::INFO)
//                    << fmt::format("delete vid-{}", vid);
//                delete version;
//                version = nullptr;
                deleted = true;
            }
        }
        mutex.unlock();
    }

}  // namespace leveldb
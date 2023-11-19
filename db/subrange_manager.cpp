
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//
#include <cmath>
#include "subrange_manager.h"
#include "memtable.h"

//这个是用来管理什么的呢????
namespace leveldb {
    SubRangeManager::SubRangeManager(leveldb::StoCWritableFileClient *manifest_file,
                                     FlushOrder *flush_order,
                                     const std::string &dbname,
                                     uint32_t dbindex,
                                     leveldb::VersionSet *versions,
                                     const leveldb::Options &options,
                                     const InternalKeyComparator *internal_comparator,
                                     const leveldb::Comparator *user_comparator,
                                     std::atomic_int_fast32_t *memtable_id_seq,
                                     std::vector<leveldb::MemTablePartition *> *partitioned_active_memtables,
                                     std::vector<uint32_t> *partitioned_imms)
            : manifest_file_(manifest_file), flush_order_(flush_order), dbname_(dbname), dbindex_(dbindex),
              versions_(versions), options_(options),
              internal_comparator_(internal_comparator),
              user_comparator_(user_comparator),
              memtable_id_seq_(memtable_id_seq),
              partitioned_active_memtables_(partitioned_active_memtables),
              partitioned_imms_(partitioned_imms) {
        lower_bound_ = options.lower_key;
        upper_bound_ = options.upper_key;
        auto sr = new SubRanges;

//这里是1个fragment对应的那个dbindex一个subranges，也就是1个range????
        uint32_t cfgid = nova::NovaConfig::config->current_cfg_id;
        auto range = nova::NovaConfig::config->cfgs[cfgid]->fragments[dbindex_];
        if (range->range.key_end - range->range.key_start <= options_.num_memtable_partitions) { // 如果这个key的范围小于active memtable的数量， 开始的时候基本不会发生，也许后面会有duplicate的情况??
            int nkeys = range->range.key_end - range->range.key_start;
            int num_duplicates = options_.num_memtable_partitions / nkeys;
            int cdup = 0;
            int lower = range->range.key_start;
            int upper = range->range.key_start + 1;

            for (int i = 0; i < options_.num_memtable_partitions; i++) {
                SubRange nsr;
                Range r;
                r.lower = std::to_string(lower);
                r.upper = std::to_string(upper);
                if (num_duplicates == 1) { //基本不可能是这个分支，全是0(开始的时候)
                    r.num_duplicates = 0;
                    nsr.num_duplicates = 0;
                } else {
                    r.num_duplicates = num_duplicates;
                    nsr.num_duplicates = num_duplicates;
                }
                nsr.tiny_ranges.push_back(r);
                sr->subranges.push_back(nsr);
                cdup++;
                if (cdup == num_duplicates) {
                    lower = upper;
                    upper = lower + 1;
                    cdup = 0;
                }
                if (upper > range->range.key_end) {
                    break;
                }
            }
        } else { // 把大的range捏成一个
            // Construct one subrange.
            SubRange nsr;
            Range r;
            r.lower = std::to_string(lower_bound_);
            r.upper = std::to_string(upper_bound_);
            nsr.tiny_ranges.push_back(r);
            sr->subranges.push_back(nsr);
        }
        sr->AssertSubrangeBoundary(user_comparator);
        ComputeCompactionThreadsAssignment(sr);
        latest_subranges_.store(sr);
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("keys:{},{}", lower_bound_, upper_bound_);
    }

// put的时候先找对应的subrange，然后再put
    int SubRangeManager::SearchSubranges(const leveldb::WriteOptions &options,
                                         const leveldb::Slice &key,
                                         const leveldb::Slice &val,
                                         leveldb::SubRange **subrange) {
        SubRanges *ref = latest_subranges_;
        int subrange_id = -1;
        // 如果已经分裂到了稳定的状态 而且不开reorganize这个选项
        if (ref->subranges.size() == options_.num_memtable_partitions || !options_.enable_subrange_reorg) { 
            // steady state.
            bool found = ref->BinarySearchWithDuplicate(key,
                                                        options.rand_seed,
                                                        &subrange_id,
                                                        user_comparator_);
            if (found) {
                NOVA_ASSERT(subrange_id != -1);
                *subrange = &ref->subranges[subrange_id];
                return subrange_id;
            }
        }

        range_lock_.Lock();
        SubRanges *new_subranges = nullptr;
        while (true) {
            ref = latest_subranges_;
            bool found = ref->BinarySearchWithDuplicate(key,
                                                        options.rand_seed,
                                                        &subrange_id,
                                                        user_comparator_);
            // 如果找到 并且已经分裂到了稳定状态
            if (found &&
                ref->subranges.size() == options_.num_memtable_partitions) { // subrange size和 num memtable partitioon有什么关系呢????
                NOVA_ASSERT(subrange_id != -1);
                break;
            }
            new_subranges = new SubRanges(*ref); // 左值！

            // 找到 但是还没分裂到稳定状态
            if (found &&
                new_subranges->subranges.size() <
                options_.num_memtable_partitions) {
                NOVA_ASSERT(subrange_id != -1);
                SubRange &sr = new_subranges->subranges[subrange_id];

                // 这里如果要分裂的话 就是 [lower, key, upper) -> [lower, key) [key, upper) 所以key = lower的时候不能分裂
                // 
                if (sr.first().lower_inclusive &&
                    user_comparator_->Compare(sr.first().lower, key) == 0) {
                    // Cannot split this subrange since it will create a new subrange with no keys.
                    break;
                }

                // find the key but not enough subranges. Split this subrange.
                SubRange new_sr = {};
                int new_subrange_index = 0;
                if (sr.tiny_ranges.size() == 1) { // 初始情况的话 分裂subrange和tinyrange
                    // split the tiny range.
                    // new_sr = [k, sr.upper)
                    Range new_range = {};
                    new_range.lower.assign(key.ToString());
                    new_range.upper.assign(sr.last().upper);
                    // Update the last tiny range of sr to [tr.lower, k)
                    Range &last = sr.last();
                    last.upper.assign(key.ToString());
                    // update stats.
                    last.ninserts /= 2;
                    new_range.ninserts = last.ninserts;
                    // insert new subrange.
                    new_sr.tiny_ranges.push_back(std::move(new_range));
                    new_subrange_index = subrange_id + 1;
                } else {
                    int moves = sr.tiny_ranges.size() / 2;
                    new_sr.tiny_ranges.insert(new_sr.tiny_ranges.begin(),
                                              sr.tiny_ranges.begin() + moves,
                                              sr.tiny_ranges.end());
                    sr.tiny_ranges.resize(moves);
                    if (sr.IsGreaterThanUpper(key, user_comparator_)) {
                        new_subrange_index = subrange_id + 1;
                    } else {
                        new_subrange_index = subrange_id;
                    }
                }
                new_sr.UpdateStats(0);
                sr.UpdateStats(0);
                subrange_id += 1;
                new_subranges->subranges.insert(
                        new_subranges->subranges.begin() + subrange_id,
                        std::move(new_sr));
                subrange_id = new_subrange_index;
                break;
            }

            // 还可以没有找到???  没找到，但是已经分裂到稳定状态了 说明要么在最大的右面 要么在最小的左面(实际上不可能发生?)
            if (!found &&
                new_subranges->subranges.size() ==
                options_.num_memtable_partitions) {
                NOVA_ASSERT(subrange_id == -1);
                // didn't find the key but we have enough subranges.
                // Extend the lower boundary of the next subrange to include this key.
                if (new_subranges->first().IsSmallerThanLower(
                        key,
                        user_comparator_)) {
                    SubRange &sr = new_subranges->first();
                    Range &first = sr.first();
                    first.lower.assign(key.ToString());
                    for (int i = 1; i < sr.num_duplicates; i++) {
                        SubRange &dup = new_subranges->subranges[i];
                        Range &dup_first = dup.tiny_ranges[0];
                        dup_first.lower.assign(key.ToString());
                    }
                    subrange_id = 0;
                } else {
                    NOVA_ASSERT(
                            new_subranges->last().IsGreaterThanUpper(
                                    key, user_comparator_));
                    SubRange &sr = new_subranges->last();
                    Range &last = sr.last();
                    uint64_t k;
                    nova::str_to_int(key.data(), &k, key.size());
                    last.upper.assign(std::to_string(k + 1));
                    for (int i = 1; i < sr.num_duplicates; i++) {
                        SubRange &dup = new_subranges->subranges[
                                new_subranges->subranges.size() - i - 1];
                        Range &dup_last = dup.last();
                        dup_last.upper.assign(std::to_string(k + 1));
                    }
                    subrange_id = new_subranges->subranges.size() - 1;
                }
                break;
            }

            // 没有找到 而且 subranges是空的 做一个range(只包含1个key)放进去
            // not found and not enough subranges.
            // no subranges. construct a point.
            if (new_subranges->subranges.empty()) {
                SubRange sr = {};
                Range new_range = {};
                new_range.lower.assign(key.ToString());
                new_range.upper.assign(
                        std::to_string(new_range.lower_int() + 1));
                sr.tiny_ranges.push_back(std::move(new_range));
                new_subranges->subranges.push_back(std::move(sr));
                subrange_id = 0;
                break;
            }

            // 没找到 而且subranges只有一个 而且是一个point
            if (new_subranges->subranges.size() == 1 &&
                new_subranges->first().IsAPoint(user_comparator_)) {
                if (new_subranges->first().IsSmallerThanLower(key,
                                                              user_comparator_)) {
                    Range &first = new_subranges->first().first();
                    first.lower.assign(key.ToString());
                } else {
                    Range &last = new_subranges->first().last();
                    uint64_t u = 0;
                    nova::str_to_int(key.data(), &u, key.size());
                    last.upper.assign(std::to_string(u + 1));
                }
                subrange_id = 0;
                break;
            }

            // 没找到 subrange有多个但是还没完全稳定/subrange有1个但是 不是point 扩张
            // key is less than the smallest user key.
            if (new_subranges->first().IsSmallerThanLower(key,
                                                          user_comparator_)) {
                SubRange sr = {};
                Range new_range = {};
                new_range.lower.assign(key.ToString());
                new_range.upper.assign(new_subranges->first().first().lower);
                sr.tiny_ranges.push_back(std::move(new_range));
                new_subranges->subranges.insert(
                        new_subranges->subranges.begin(), std::move(sr));
                subrange_id = 0;
                break;
            }

            // key is greater than the largest user key.
            if (new_subranges->last().IsGreaterThanUpper(key,
                                                         user_comparator_)) {
                SubRange sr = {};
                Range new_range = {};
                new_range.lower.assign(new_subranges->last().last().upper);
                uint64_t u = 0;
                nova::str_to_int(key.data(), &u, key.size());
                new_range.upper.assign(std::to_string(u + 1));
                sr.tiny_ranges.push_back(std::move(new_range));
                new_subranges->subranges.push_back(std::move(sr));
                subrange_id = new_subranges->subranges.size() - 1;
                break;
            }
            NOVA_ASSERT(false);
        }

        if (new_subranges) {
            if (options_.enable_detailed_stats) {
                NOVA_LOG(rdmaio::INFO)
                    << fmt::format("Expand subranges for key {} ",
                                   key.ToString());
            }
            new_subranges->AssertSubrangeBoundary(user_comparator_);
            latest_subranges_.store(new_subranges);
            ref = new_subranges;
        }
        range_lock_.Unlock();
        *subrange = &ref->subranges[subrange_id]; //ref会自己析构么??? 应该是会的 我猜
        return subrange_id;
    }

// major reorganization 核心调用
    bool SubRangeManager::MajorReorg() {
        std::vector<double> insertion_rates;
        std::vector<SubRange> &subranges = latest_->subranges;
        // Perform major reorg.
        std::vector<std::vector<AtomicMemTable *>> subrange_imms; // 每个subrange的memtable
        uint32_t nslots = options_.num_memtables / options_.num_memtable_partitions; // 应该是重新计算各种分配??
        uint32_t remainder = options_.num_memtables % options_.num_memtable_partitions;
        uint32_t slot_id = 0;
        for (int i = 0; i < options_.num_memtable_partitions; i++) { //处理每个partition
            std::vector<AtomicMemTable *> memtables;
            (*partitioned_active_memtables_)[i]->mutex.Lock();
            MemTable *m = (*partitioned_active_memtables_)[i]->active_memtable;
            if (m) {
                NOVA_ASSERT(versions_->mid_table_mapping_[m->memtableid()]->RefMemTable());
                memtables.push_back(versions_->mid_table_mapping_[m->memtableid()]);
            }
            uint32_t slots = nslots;
            if (remainder > 0) {
                slots += 1;
                remainder--;
            }
            for (int j = 0; j < slots; j++) { //找到自己partition对应的memtable 以及 immutable memtable??
                uint32_t imm_id = (*partitioned_imms_)[slot_id + j];
                if (imm_id == 0) {
                    continue;
                }
                auto *imm = versions_->mid_table_mapping_[imm_id]->RefMemTable();
                if (imm) {
                    memtables.push_back(versions_->mid_table_mapping_[imm_id]);
                }
            }
            slot_id += slots;
            (*partitioned_active_memtables_)[i]->mutex.Unlock();
            subrange_imms.push_back(memtables);
        }

        // We have all memtables now.
        // Determine the number of samples to retrieve from each subrange.
        uint32_t sample_size_per_subrange = UINT32_MAX;
        std::vector<uint32_t> subrange_nputs; // 每个subrange对应的记录数量
        std::vector<std::vector<uint32_t>> subrange_mem_nputs; // 每个subrange对应的memtable的记录数量
        for (int i = 0; i < subrange_imms.size(); i++) { // 每一个partition / subrange
            uint32_t nputs = 0;
            std::vector<uint32_t> ns;
            for (int j = 0; j < subrange_imms[i].size(); j++) { // subrange种每一个memtable中的记录数量
                uint32_t n = subrange_imms[i][j]->nentries_.load(std::memory_order_relaxed);
                ns.push_back(n);
                nputs += n;
            }
            subrange_mem_nputs.push_back(ns);
            subrange_nputs.push_back(nputs);
            if (nputs > 100) {
                sample_size_per_subrange = std::min(sample_size_per_subrange, nputs);
            }
        }
        // 这里是为了决定采样的数量?????????

        // Sample from each memtable.
        sample_size_per_subrange = (double) (sample_size_per_subrange) * options_.subrange_reorg_sampling_ratio;
        std::map<uint64_t, double> userkey_rate; // 某个key对应的insertion_ratio的和 emmm 表征了key的插入频次
        double total_rate = 0; // keyrate中所有项频次之和
        for (int i = 0; i < subrange_imms.size(); i++) { // 每一个subrange/partition
            SubRange &sr = subranges[i];
            uint32_t total_puts = subrange_nputs[i];
            double insertion_ratio = subranges[i].insertion_ratio;

            for (int j = 0; j < subrange_imms[i].size(); j++) { // subrange种每一个memtable 
                AtomicMemTable *mem = subrange_imms[i][j];
                uint32_t sample_size = 0;
                if (total_puts <= sample_size_per_subrange) { // 如果当前subrange的总记录数还没有要sample的数量多
                    // sample everything.
                    sample_size = sample_size_per_subrange;
                } else {
                    sample_size = ((double) subrange_mem_nputs[i][j] / (double) total_puts) * sample_size_per_subrange; // 当前的memtable要sample的数量
                }
                uint32_t samples = 0;
                leveldb::Iterator *it = mem->memtable_->NewIterator(
                        TraceType::MEMTABLE, AccessCaller::kUncategorized, sample_size);
                it->SeekToFirst();
                while (it->Valid() && samples < sample_size) {
                    Slice userkey = ExtractUserKey(it->key());
                    uint64_t k = 0;
                    nova::str_to_int(userkey.data(), &k, userkey.size());
                    userkey_rate[k] += insertion_ratio;
                    total_rate += insertion_ratio;
                    samples += 1;
                    it->Next();
                }
                if (options_.enable_detailed_stats) {
                    NOVA_LOG(rdmaio::INFO)
                        << fmt::format(
                                "Sample {} {} {} {} from mid-{} subrange-{}",
                                samples, sample_size,
                                subrange_mem_nputs[i][j], total_puts,
                                mem->memtable_->memtableid(), i);
                }
                delete it;
            }
        }

        // 如果采样出来的key的总数小于partition个数的2倍 就不进行major reorganization了 这里的做法是为什么
        if (userkey_rate.size() <= options_.num_memtable_partitions * 2) {
            num_skipped_major_reorgs++;
            // Unref all immutable memtables.
            for (int i = 0; i < subrange_imms.size(); i++) {
                for (int j = 0; j < subrange_imms[i].size(); j++) {
                    subrange_imms[i][j]->Unref(dbname_);
                }
            }
            delete latest_; // ???? 删了旧的 subrange..?????????????
            latest_ = nullptr;
            return false;
        }

        num_major_reorgs++; // 开始进行major reorganization
        subranges.clear(); // 基本还原了

        // First, construct subranges with each subrange containing one tiny
        // range.
        std::vector<Range> tmp_subranges;
        ConstructRanges(userkey_rate, total_rate, lower_bound_, upper_bound_, // key对应的插入频次， 总的插入频次， 上下限，  
                        options_.num_memtable_partitions, true, &tmp_subranges);
        // Second, break each subrange that contains more than one value into
        // alpha tiny ranges.
        for (int i = 0; i < tmp_subranges.size(); i++) {
            std::map<uint64_t, double> sub_userkey_rate; // 1个subrange内key和频次
            uint64_t lower = tmp_subranges[i].lower_int();
            uint64_t upper = tmp_subranges[i].upper_int();
            SubRange sr = {};
            if (upper - lower > 1) { // 如果当前的subrange有多于1个的key
                double sub_total_share = 0; // 1个
                for (auto it : userkey_rate) {
                    if (it.first < lower) {
                        continue;
                    }
                    if (it.first >= upper) {
                        continue;
                    }
                    sub_total_share += it.second;
                    sub_userkey_rate[it.first] = it.second;
                }
                ConstructRanges(sub_userkey_rate, sub_total_share,
                                lower, upper,
                                options_.num_tiny_ranges_per_subrange, // 一般是10
                                false, &sr.tiny_ranges); // 用同样的方法做tiny ranges
            } else {
                sr.tiny_ranges.push_back(std::move(tmp_subranges[i])); // 只有1个key就直接放进去就好
            }
            sr.num_duplicates = sr.first().num_duplicates;
            subranges.push_back(std::move(sr));
        }
        // Unref all immutable memtables.
        for (int i = 0; i < subrange_imms.size(); i++) {
            for (int j = 0; j < subrange_imms[i].size(); j++) {
                subrange_imms[i][j]->Unref(dbname_);
            }
        }
        if (options_.enable_detailed_stats) {
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("major with {} keys: {}",
                               userkey_rate.size(), latest_->DebugString());
        }
        return true;
    }

// 找到duplicate的subrange
    void SubRangeManager::MoveShareForDuplicateSubRange(int index) {
        SubRange &sr = latest_->subranges[index];
        uint64_t lower = sr.tiny_ranges[0].lower_int();
        int remaining_num_duplicates = 0;
        int start = -1;
        int end = -1;
        for (int i = 0; i < latest_->subranges.size(); i++) { // 找到index对应的范围的起始subrange和结束subrange
            SubRange &r = latest_->subranges[i];
            if (r.num_duplicates == 0) {
                continue;
            }
            NOVA_ASSERT(r.tiny_ranges.size() == 1);
            if (r.tiny_ranges[0].lower_int() != lower) {
                continue;
            }
            end = i;
            if (start == -1) {
                start = i;
            }
        }

        remaining_num_duplicates = end - start; // 这个duplicate数量等于 这个range的subrange数量-1
        NOVA_ASSERT(sr.num_duplicates == remaining_num_duplicates + 1);
        double share = sr.ninserts / remaining_num_duplicates; // 把index对应的subrange的东西分给别的duplicate
        for (int i = start; i <= end; i++) {
            SubRange &r = latest_->subranges[i];
            r.tiny_ranges[0].ninserts += share;
            r.num_duplicates -= 1;
            r.tiny_ranges[0].num_duplicates -= 1;
            r.UpdateStats(total_num_inserts_since_last_major_);

            if (r.num_duplicates == 1) {
                r.num_duplicates = 0;
                r.tiny_ranges[0].num_duplicates = 0;
            }
        }
    }

// major reorganization重新做subrange
    void SubRangeManager::ConstructRanges(
            const std::map<uint64_t, double> &userkey_rate, double total_rate,
            uint64_t lower, uint64_t upper, uint32_t num_ranges_to_construct, // 一般就是partition的数量
            bool is_constructing_subranges,
            std::vector<leveldb::Range> *ranges) {
        NOVA_ASSERT(upper - lower > 1);
        NOVA_ASSERT(num_ranges_to_construct > 1);
        double share_per_range = total_rate / (double) num_ranges_to_construct; // 每个subrange对应的比例 (应该动态计算 这个看起来是针对range
        double fair_rate = total_rate / (double) num_ranges_to_construct; // 一个key平均应该对应的比例 这个用于计算key应该dupliate几个
        double total = total_rate;

        uint32_t current_lower = lower;
        uint32_t current_upper = 0;
        double current_rate = 0.0;
        for (auto it : userkey_rate) { // 遍历这个抽样产生的key和频次
            NOVA_ASSERT(it.first >= lower);
            NOVA_ASSERT(it.first < upper);
            double rate = it.second; // key对应的插入频次
            if (rate >= fair_rate && is_constructing_subranges) { // 如果这个key的rate高于正常的rate 而且 正在做?
                if (options_.enable_detailed_stats) {
                    NOVA_LOG(rdmaio::INFO)
                        << fmt::format("hot key {}:{}:{}", it.first,
                                       rate / total,
                                       fair_rate / total);
                }
                // close the current subrange.
                if (current_lower < it.first) { // 如果当前在做的这个subrange 的lower低于key， 那么就直接先放一个subrange进去?? (这样是不包括这个key的)
                    current_upper = it.first;
                    Range r = {};
                    r.lower = std::to_string(current_lower);
                    r.upper = std::to_string((current_upper));
                    r.insertion_ratio = current_rate / total;
                    (*ranges).push_back(std::move(r));
                }

                int num_duplicates = (int) std::ceil(rate / fair_rate); // 看看需要放几个这个range
                for (int i = 0; i < num_duplicates; i++) {
                    Range r = {};
                    r.lower = std::to_string(it.first);
                    r.upper = std::to_string(it.first + 1);
                    r.num_duplicates = num_duplicates;
                    r.insertion_ratio = rate / num_duplicates;
                    (*ranges).push_back(std::move(r));
                }
                current_lower = it.first + 1; // 为
                total_rate -= it.second; // 
                current_rate = 0;
                share_per_range =
                        total_rate / (num_ranges_to_construct - ranges->size()); // 更新以下剩下的range应该分配到的比例??
                continue;
            }

            if (current_rate + rate > share_per_range) { // 如果当前积累的比例可以做一个subrange了
                if (current_lower == it.first) { // 当前的lower就是这个key
                    current_upper = it.first + 1;
                    Range r = {};
                    r.lower = std::to_string(current_lower);
                    r.upper = std::to_string(current_upper);
                    r.insertion_ratio = current_rate / total;
                    (*ranges).push_back(std::move(r));

                    current_lower = it.first + 1;
                    if (ranges->size() + 1 == num_ranges_to_construct) { // 如果就剩1个subrange的容量了 
                        break;
                    }
                    current_rate = 0;
                    total_rate -= rate;
                    share_per_range = total_rate / (num_ranges_to_construct -
                                                    ranges->size());
                    continue;
                } else {// ~
                    current_upper = it.first;
                    Range r = {};
                    r.lower = std::to_string(current_lower);
                    r.upper = std::to_string(current_upper);
                    r.insertion_ratio = current_rate / total;
                    (*ranges).push_back(std::move(r));

                    current_lower = it.first;
                    if (ranges->size() + 1 == num_ranges_to_construct) {
                        break;
                    }
                    current_rate = 0;
                    share_per_range = total_rate / (num_ranges_to_construct -
                                                    ranges->size());
                }
            }
            current_rate += rate;
            total_rate -= rate;
        }

        if (is_constructing_subranges) { // major reorganization的时候为真
            Range r = {};
            r.lower = std::to_string(current_lower); // ????????????????
            ranges->push_back(std::move(r));
            NOVA_ASSERT(ranges->size() == num_ranges_to_construct);
        } else {
            if (current_lower < upper) {
                Range r = {};
                r.lower = std::to_string(current_lower);
                ranges->push_back(std::move(r));
            }
            NOVA_ASSERT(ranges->size() <= num_ranges_to_construct);
        }

        (*ranges)[0].lower = std::to_string(lower);
        (*ranges->rbegin()).upper = std::to_string(upper);
    }
// 这里的处理好低效

// 销毁目前subrange中的所有duplicate，以便更好地进行划分?
    bool
    SubRangeManager::DestroyDuplicates(int subrange_id, bool force) {
        SubRange &sr = latest_->subranges[subrange_id];
        double fair_ratio = 1.0 / (double) (options_.num_memtable_partitions); // 每个partition应该承担的访问比例
        if (sr.num_duplicates == 0) { //如果没有duplicate 就不用distroy了
            return false;
        }
        if (force) {
            MoveShareForDuplicateSubRange(subrange_id);
            latest_->subranges.erase(latest_->subranges.begin() + subrange_id);
            return true;
        }

        double percent = sr.insertion_ratio / fair_ratio;
        if (percent >= 0.5) { // 如果占据了正常比例的50%以上 就不用改动它以及它的duplicate?
            return false;
        }
        num_minor_reorgs_for_dup++;
        if (options_.enable_detailed_stats) {
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("Destroy subrange {}", subrange_id);
        }
        // destroy this duplicate.
        MoveShareForDuplicateSubRange(subrange_id);
        latest_->subranges.erase(latest_->subranges.begin() + subrange_id); // 把当前的subrange删除掉
        latest_->AssertSubrangeBoundary(user_comparator_);
        return true;
    }

// 查看是否需要对目前subrange进行分裂
    bool SubRangeManager::CreateDuplicates(int subrange_id) {
        SubRange &sr = latest_->subranges[subrange_id];
        if (!sr.IsAPoint(user_comparator_)) { // 不是point就不需要分裂? 查看是不是point
            return false;
        }
        if (sr.insertion_ratio <= 1.5 * fair_ratio_) { // 如果访问的比例小于1.5倍的平均比例
            return false;
        }
        int new_num_duplicates =
                (int) std::floor(sr.insertion_ratio / fair_ratio_) - 1; // 看看应该分为几个subrange
        if (new_num_duplicates == 0) {
            return false;
        }
        num_minor_reorgs_for_dup++;
        uint64_t lower = sr.tiny_ranges[0].lower_int();
        uint64_t upper = sr.tiny_ranges[0].upper_int();
        // Update num duplicates.
        uint32_t total_num_dups = sr.num_duplicates + new_num_duplicates + 1;
        if (sr.num_duplicates == 0) { // 这里duplicate计算有点emmmm ..
            total_num_dups = sr.num_duplicates + new_num_duplicates + 1;
        } else {
            total_num_dups = sr.num_duplicates + new_num_duplicates;
        }
        if (sr.num_duplicates > 0) { // 如果原来这个subrange是有duplicate的 
            int start = -1;
            int end = -1;
            for (int i = 0; i < latest_->subranges.size(); i++) {
                SubRange &r = latest_->subranges[i];
                if (r.num_duplicates == 0) {
                    continue;
                }
                if (r.tiny_ranges.size() != 1) {
                    continue;
                }
                if (r.tiny_ranges[0].lower_int() != lower) {
                    continue;
                }
                if (r.tiny_ranges[0].upper_int() != upper) {
                    continue;
                }
                end = i;
                if (start == -1) {
                    start = i;
                }
            }// 找到这个subrange对应的开始点和结束点
            NOVA_ASSERT(start != -1 && end != -1);
            for (int i = start; i <= end; i++) { // 更新新的duplicate数量
                SubRange &r = latest_->subranges[i];
                r.tiny_ranges[0].num_duplicates = total_num_dups;
                r.num_duplicates = total_num_dups;
            }
        }

        // Create new duplicate subranges.
        double total_inserts = sr.ninserts;
        double remaining_sum = total_inserts;
        for (int i = 0; i < new_num_duplicates; i++) {
            SubRange new_sr = {};
            Range tinyrange = {};
            tinyrange.lower = std::to_string(lower);
            tinyrange.upper = std::to_string(upper);
            tinyrange.ninserts = total_inserts / (new_num_duplicates + 1);
            tinyrange.insertion_ratio =
                    tinyrange.ninserts / total_num_inserts_since_last_major_;
            tinyrange.num_duplicates = total_num_dups;
            new_sr.ninserts = tinyrange.ninserts;
            new_sr.insertion_ratio = tinyrange.insertion_ratio;
            new_sr.num_duplicates = total_num_dups;
            remaining_sum -= tinyrange.ninserts;
            new_sr.tiny_ranges.push_back(std::move(tinyrange));
            latest_->subranges.insert(
                    latest_->subranges.begin() + subrange_id + 1, new_sr);
        } // 将新加的subrange加入到对应的subrange后面
        {
            SubRange &sr = latest_->subranges[subrange_id];
            sr.num_duplicates = total_num_dups;
            sr.tiny_ranges[0].num_duplicates = total_num_dups;
            sr.tiny_ranges[0].ninserts = remaining_sum;
            sr.ninserts = remaining_sum;
            sr.tiny_ranges[0].insertion_ratio =
                    remaining_sum / total_num_inserts_since_last_major_;
            sr.insertion_ratio =
                    remaining_sum / total_num_inserts_since_last_major_;
        }


        // Remove subranges if the number of subranges exceeds max.
        // For each removed subrange, move its tiny ranges to its neighboring
        // subranges.
        while (latest_->subranges.size() > options_.num_memtable_partitions) { // 由于新增了subrange 所以要检查
            // remove the subrange that has the lowest insertion rate.
            double min_ratio = 1.0;
            int min_range_id = -1;
            for (int i = 0; i < latest_->subranges.size(); i++) {
                SubRange &min_sr = latest_->subranges[i];
                // Skip the new subranges.
                if (min_sr.tiny_ranges.size() == 1) {
                    if (min_sr.tiny_ranges[0].lower_int() == lower) {
                        continue;
                    }
                }
                if (min_sr.insertion_ratio < min_ratio) { // 找到操作最小的那个subrange
                    min_ratio = min_sr.insertion_ratio;
                    min_range_id = i;
                }
            }
            NOVA_ASSERT(min_range_id != -1);
            if (latest_->subranges[min_range_id].num_duplicates > 0) { //如果这个range有duplicate 对这个range做处理
                DestroyDuplicates(min_range_id, true);
                continue;
            }

            int left = min_range_id - 1; // 这个subtange 不是duplicate的 这个subrange操作很少 或许可以合并
            int right = min_range_id + 1;
            bool merge_left = true;
            if (left >= 0 && right < latest_->subranges.size()) {
                if (latest_->subranges[left].insertion_ratio <
                    latest_->subranges[right].insertion_ratio) {
                    merge_left = true;
                } else {
                    merge_left = false;
                }
            } else if (left >= 0) {
                merge_left = true;
            } else {
                merge_left = false;
            } // 看看可不可以与左边的合并
            if (merge_left && latest_->subranges[left].num_duplicates > 0) { // 如果左边是duplicate的 先压缩
                DestroyDuplicates(left, true);
            } else if (!merge_left &&
                       latest_->subranges[right].num_duplicates > 0) { // 如果右边是 先压缩
                DestroyDuplicates(right, true);
            } else { // 这里想合并的部分
                bool dummy;
                int nranges = latest_->subranges[min_range_id].tiny_ranges.size();
                int pushed_ranges = PushTinyRanges(min_range_id, false, &dummy);
                NOVA_ASSERT(pushed_ranges <= nranges);
            }
        }
        if (options_.enable_detailed_stats) {
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("minor duplicate subrange {} new d:{}",
                               subrange_id,
                               new_num_duplicates);
        }
        latest_->AssertSubrangeBoundary(user_comparator_);
        return true;
    }

// 将目前的subrange push到邻居的subrange
    int
    SubRangeManager::PushTinyRanges(int subrangeId, bool stopWhenBelowFair,
                                    bool *updated_prior) { 
        // Push its tiny ranges to its neighbors.
        int left = subrangeId - 1;
        int right = subrangeId + 1;

        SubRange *left_sr = nullptr;
        SubRange *right_sr = nullptr;
        SubRange *min_sr = &latest_->subranges[subrangeId];

        // 选要合并的subrange

        if (left >= 0 && latest_->subranges[left].num_duplicates == 0) { 
            left_sr = &latest_->subranges[left];
        }
        if (right < latest_->subranges.size() &&
            latest_->subranges[right].num_duplicates == 0) {
            right_sr = &latest_->subranges[right];
        }

        int moved = 0;
        // move to left
        // move the remaining
        double left_ratio = 1.0;
        double right_ratio = 1.0;
        if (left_sr) {
            left_ratio = latest_->subranges[left].insertion_ratio;
        }
        if (right_sr) {
            right_ratio = right_sr->insertion_ratio;
        }
        if (left_ratio != 1.0 || right_ratio != 1.0) { // 如果两侧有一侧存在
            int direction = rand() % 2;
            while (!min_sr->tiny_ranges.empty()) { // 将当前的subrange种tinyrange
                Range &first = min_sr->first();
                Range &last = min_sr->last();
                bool push_left = false;

                if (direction != -1) {
                    if (direction == 0) {
                        push_left = true;
                    } else {
                        push_left = false;
                    }
                    direction = -1; //第一次push的时候选个随机的..??
                } else {
                    if (left_ratio + first.insertion_ratio <
                        right_ratio + last.insertion_ratio) {
                        push_left = true;
                    }
                }

                if (push_left) {
                    if (!left_sr) {
                        push_left = false;
                    }
                } else {
                    // push to right.
                    if (!right_sr) {
                        push_left = true;
                    }
                }

                if (stopWhenBelowFair) {
                    if (push_left) {
                        if (min_sr->insertion_ratio
                            - first.insertion_ratio < fair_ratio_ &&
                            moved > 0) {
                            return moved;
                        }
                    } else {
                        if (min_sr->insertion_ratio - last.insertion_ratio <
                            fair_ratio_
                            && moved > 0) {
                            return moved;
                        }
                    }
                }
                // 到这里终于决定了 push到哪里
                if (push_left) {
                    // move to left.
                    left_ratio += first.insertion_ratio;
                    left_sr->insertion_ratio += first.insertion_ratio;
                    left_sr->ninserts += first.ninserts;
                    min_sr->insertion_ratio -= first.insertion_ratio;
                    min_sr->ninserts -= first.ninserts;

                    left_sr->tiny_ranges.push_back(std::move(first));
                    min_sr->tiny_ranges.erase(min_sr->tiny_ranges.begin());
                } else {
                    // move to right.
                    right_ratio += last.insertion_ratio;
                    right_sr->insertion_ratio += last.insertion_ratio;
                    right_sr->ninserts += last.ninserts;
                    min_sr->insertion_ratio -= last.insertion_ratio;
                    min_sr->ninserts -= last.ninserts;
                    right_sr->tiny_ranges.insert(right_sr->tiny_ranges.begin(), std::move(last));
                    min_sr->tiny_ranges.resize(min_sr->tiny_ranges.size() - 1);
                }
                moved++;
            }
        }
        if (min_sr->tiny_ranges.empty()) {
            latest_->subranges.erase(latest_->subranges.begin() + subrangeId);
        }
        return moved;
    }

// minor reorganization的sample?
    std::vector<AtomicMemTable *>
    SubRangeManager::MinorSampling(int subrange_id) {
        SubRange &sr = latest_->subranges[subrange_id];
        double fair = 1.0 / sr.tiny_ranges.size(); // 每个tiny range应该分得的份额
        uint32_t unfair_ranges = 0;
        for (int i = 0; i < sr.tiny_ranges.size(); i++) { // 计算下不平衡的tinyrange数量
            Range &r = sr.tiny_ranges[i];
            double diff = (r.insertion_ratio - fair) * 100.0 / fair;
            if (std::abs(diff) > SUBRANGE_REORG_DIFF_FROM_FAIR_THRESHOLD) {
                unfair_ranges += 1;
            }
        }
        std::vector<AtomicMemTable *> subrange_imms;
        if ((double) unfair_ranges / (double) sr.tiny_ranges.size() >
            SUBRANGE_MAJOR_REORG_THRESHOLD) { // 如果不平衡的tiny range数量很多
            // higher share.
            // Perform major reorg.
            uint32_t nslots = options_.num_memtables / options_.num_memtable_partitions;
            uint32_t remainder = options_.num_memtables % options_.num_memtable_partitions;
            uint32_t slot_id = 0;
            uint32_t slots = 0;
            for (int i = 0; i < subrange_id; i++) {
                slots = nslots;
                if (remainder > 0) {
                    slots += 1;
                    remainder--;
                }
                slot_id += slots;
            }
            slots = nslots;
            if (remainder > 0) {
                slots += 1;
            }
            (*partitioned_active_memtables_)[subrange_id]->mutex.Lock();
            MemTable *m = (*partitioned_active_memtables_)[subrange_id]->active_memtable;
            if (m) {
                NOVA_ASSERT(versions_->mid_table_mapping_[m->memtableid()]->RefMemTable()) << m->memtableid();
                subrange_imms.push_back(versions_->mid_table_mapping_[m->memtableid()]);
            }
            for (int j = 0; j < slots; j++) {
                NOVA_ASSERT(slot_id + j < (*partitioned_imms_).size());
                uint32_t imm_id = (*partitioned_imms_)[slot_id + j];
                if (imm_id == 0) {
                    continue;
                }
                auto *imm = versions_->mid_table_mapping_[imm_id]->RefMemTable();
                if (imm) {
                    subrange_imms.push_back(versions_->mid_table_mapping_[imm_id]);
                }
            }
            (*partitioned_active_memtables_)[subrange_id]->mutex.Unlock();
            // We have all memtables now.
            std::map<uint64_t, double> userkey_freq;
            double total_accesses = 0;
            for (int i = 0; i < subrange_imms.size(); i++) {
                AtomicMemTable *mem = subrange_imms[i];
                Iterator *it = mem->memtable_->NewIterator(MEMTABLE, kUncategorized, 0);
                it->SeekToFirst();
                while (it->Valid()) {
                    Slice uk = ExtractUserKey(it->key());
                    if (sr.IsSmallerThanLower(uk, user_comparator_)) {
                        it->Next();
                        continue;
                    }
                    if (sr.IsGreaterThanUpper(uk, user_comparator_)) {
                        it->Next();
                        continue;
                    }

                    uint64_t k = 0;
                    nova::str_to_int(uk.data(), &k, uk.size());
                    userkey_freq[k] += 1;
                    total_accesses += 1;
                    it->Next();
                }
                delete it;
            }
            num_minor_reorgs_samples += 1;
            if (userkey_freq.size() <=
                options_.num_tiny_ranges_per_subrange * 2 ||
                total_accesses <= 100) {
                num_skipped_minor_reorgs++;
            } else {
                std::vector<Range> ranges;
                ConstructRanges(userkey_freq, total_accesses,
                                sr.first().lower_int(),
                                sr.last().upper_int(),
                                options_.num_tiny_ranges_per_subrange, false, &ranges);
                for (auto &range : ranges) {
                    range.ninserts = range.insertion_ratio * sr.ninserts;
                }
                sr.tiny_ranges.clear();
                sr.tiny_ranges = ranges;
                sr.UpdateStats(total_num_inserts_since_last_major_);
                NOVA_LOG(rdmaio::INFO)
                    << fmt::format("Minor sampling {} {}", total_accesses, sr.DebugString());
            }
        }
        return subrange_imms;
    }

// 属于minor reorganization? 用于处理最不平衡的那个subrange 没到major reorganize的标准
    bool
    SubRangeManager::MinorRebalancePush(int subrange_id, bool *updated_prior) {
        SubRange &sr = latest_->subranges[subrange_id];
        double totalRemoveInserts = (sr.insertion_ratio - fair_ratio_) * total_num_inserts_since_last_major_; // 需要移动的记录/inserts数量

        if (totalRemoveInserts >= sr.ninserts) { // ??????
            return false;
        }

        if (sr.tiny_ranges.size() == 1) { // 只有1个tiny range的话就拆不了
            return false;
        }
        NOVA_ASSERT(sr.insertion_ratio > fair_ratio_);
        if (options_.enable_detailed_stats) {
            NOVA_LOG(rdmaio::INFO)
                << fmt::format("{} push minor before {}", subrange_id, sr.DebugString());
        }
        std::vector<AtomicMemTable *> subrange_imms = MinorSampling(subrange_id);
        // Distribute the load across adjacent subranges.
        bool success = PushTinyRanges(subrange_id, true, updated_prior) > 0;
        // push tiny ranges.
        if (success) {
            num_minor_reorgs++;
            if (options_.enable_detailed_stats) {
                NOVA_LOG(rdmaio::INFO)
                    << fmt::format("{} push minor after {}", subrange_id, sr.DebugString());
            }
        }
        // Unref all immutable memtables.
        for (int j = 0; j < subrange_imms.size(); j++) {
            subrange_imms[j]->Unref(dbname_);
        }
        return success;
    }

// 开启reorganize 核心调用 这里既包括了minor也包括了major
    void
    SubRangeManager::ReorganizeSubranges() {
        uint32_t cfgid = nova::NovaConfig::config->current_cfg_id;
        auto range = nova::NovaConfig::config->cfgs[cfgid]->fragments[dbindex_];
        if (range->range.key_end - range->range.key_start <= options_.num_memtable_partitions) { // 如果range本身key很少了 那就不用
            return;
        }

        if (options_.enable_detailed_stats) {
            NOVA_LOG(rdmaio::INFO) << "Perform subrange reorg";
        }
        uint64_t latest_seq_number = versions_->last_sequence_;
        SubRanges *ref = latest_subranges_;
        // Make a copy.
        range_lock_.Lock();
        latest_ = new SubRanges(*ref);
        range_lock_.Unlock();
        total_num_inserts_since_last_major_ = 0;
        fair_ratio_ = 1.0 / (double) (options_.num_memtable_partitions); // 正常情况下每个partition应该分得的访问比例
        edit_.Clear();

        std::vector<SubRange> &subranges = latest_->subranges;
        double unfair_subranges = 0;
        bool subrange_reorged = false;
        bool update_latest_subrange = false;

// 计算各个subrange的总insert数量和比例
        for (int i = 0; i < subranges.size(); i++) {
            SubRange &sr = subranges[i];
            sr.ninserts = 0;
            for (int j = 0; j < sr.tiny_ranges.size(); j++) {
                sr.ninserts += sr.tiny_ranges[j].ninserts;
            }
            total_num_inserts_since_last_major_ += sr.ninserts;
        }
        for (int i = 0; i < subranges.size(); i++) {
            SubRange &sr = subranges[i];
            for (int j = 0; j < sr.tiny_ranges.size(); j++) {
                sr.tiny_ranges[j].insertion_ratio = sr.tiny_ranges[j].ninserts / total_num_inserts_since_last_major_;
            }
            sr.insertion_ratio = sr.ninserts / total_num_inserts_since_last_major_;
        }

// 如果到了interval 一般要么是到了interval 要么是到了warmup 如果现在的seq number与上次minor reorganization 的 interval到了
        if (latest_seq_number - last_minor_reorg_seq_ > SUBRANGE_MINOR_REORG_INTERVAL) {
            int pivot = 0;
            bool success = false;
            while (pivot < subranges.size()) {
                SubRange &sr = subranges[pivot];
                if (DestroyDuplicates(pivot, false)) { // true删了 false 没删 如果删了那么pivot就是下一个要处理的subrange 删掉duplicate中share过小的部分
                    // No need to update pivot since it already points to the
                    // next range.
                    success = true;
                } else if (CreateDuplicates(pivot)) {
                    // Go to the next subrange.
                    for (int i = 0; i < subranges.size(); i++) {
                        SubRange &other = subranges[i];
                        if (other.Equals(sr, user_comparator_)) {
                            pivot = i;
                        }
                    }
                    pivot++;
                    success = true;
                } else {
                    pivot++;
                }
            }
            if (success) {
                subrange_reorged = true;
                update_latest_subrange = true;
                uint64_t now = versions_->last_sequence_;
                last_minor_reorg_seq_ = now;
            }
        }
        // 以上完成了 minor reorganization
        // 以下应该是 major reorganization 

        double most_unfair = 0;
        int most_unfair_subrange = -1;
        for (int i = 0; i < subranges.size(); i++) {
            SubRange &sr = subranges[i];
            double diff = (sr.insertion_ratio - fair_ratio_) * 100.0 / fair_ratio_; // 在20%的话 就是一个unfair sburange
            if (std::abs(diff) > SUBRANGE_REORG_DIFF_FROM_FAIR_THRESHOLD &&
                !sr.IsAPoint(user_comparator_)) {
                unfair_subranges += 1;
            }
            bool eligble_for_minor =
                    sr.ninserts > 100 && latest_seq_number - last_minor_reorg_seq_ > SUBRANGE_MINOR_REORG_INTERVAL; //如果足够的insert之后还是这个结果 说明不能忽略这个分布不均匀
            if (eligble_for_minor) {
                if (diff > SUBRANGE_REORG_DIFF_FROM_FAIR_THRESHOLD &&
                    diff > most_unfair) { // 记录下目前最不平衡的subrange
                    most_unfair = diff;
                    most_unfair_subrange = i;
                }
            }
        }

        ImpactedDranges impacted_dranges;
        impacted_dranges.generation_id = flush_order_ ? flush_order_->latest_generation_id.load() + 1 : 0;
        if (unfair_subranges / (double) subranges.size() > SUBRANGE_MAJOR_REORG_THRESHOLD && // 如果不平衡的subrange达到了30%
            latest_seq_number - last_major_reorg_seq_ > SUBRANGE_MAJOR_REORG_INTERVAL) { // 且已经到了major reorganization的次数
            subrange_reorged = MajorReorg(); // 完成major reorganization 如果是false的话也会清空??
            update_latest_subrange = subrange_reorged;
            impacted_dranges.lower_drange_index = 0;
            impacted_dranges.upper_drange_index = partitioned_active_memtables_->size() - 1;
            if (subrange_reorged) { // reorganize成功 / 也可能没有进行reorganization
                uint64_t now = versions_->last_sequence_;
                last_major_reorg_seq_ = now;
                last_minor_reorg_seq_ = now;
            }
        } else if (most_unfair != 0) { // 不然的话还是再做一次minor reorganization就好
            // Perform minor.
            bool updated_prior = false;
            subrange_reorged = MinorRebalancePush(most_unfair_subrange, &updated_prior);
            if (subrange_reorged || updated_prior) {
                update_latest_subrange = true;
            }
            if (subrange_reorged) {
                uint64_t now = versions_->last_sequence_;
                last_minor_reorg_seq_ = now;
            }
        }

        if (subrange_reorged) { // 如果经过了minor reorganization或者major reorganization
            ComputeCompactionThreadsAssignment(latest_);
            for (int i = 0; i < latest_->subranges.size(); i++) {
                edit_.UpdateSubRange(i, latest_->subranges[i].tiny_ranges, latest_->subranges[i].num_duplicates);
            }
            versions_->AppendChangesToManifest(&edit_, manifest_file_, options_.manifest_stoc_ids);
        }
        if (update_latest_subrange) { // 如果更新了subranges
            if (ref->subranges.size() != latest_->subranges.size() ||
                ref->subranges.size() < options_.num_memtable_partitions) {
                impacted_dranges.lower_drange_index = 0;
                impacted_dranges.upper_drange_index = options_.num_memtable_partitions - 1;
            } else {
                for (int i = 0; i < ref->subranges.size(); i++) {
                    const auto &current = ref->subranges[i];
                    const auto &updated = latest_->subranges[i];
                    if (!current.RangeEquals(updated, user_comparator_)) {
                        impacted_dranges.lower_drange_index = i;
                        break;
                    }
                }

                for (int i = ref->subranges.size() - 1; i >= 0; i--) {
                    const auto &current = ref->subranges[i];
                    const auto &updated = latest_->subranges[i];
                    if (!current.RangeEquals(updated, user_comparator_)) {
                        impacted_dranges.upper_drange_index = i;
                        break;
                    }
                }
            }

            if (flush_order_ && impacted_dranges.upper_drange_index > 0) {
                // Mark all active memtable of impacted dranges as immutable.
                for (uint32_t drange_id = impacted_dranges.lower_drange_index;
                     drange_id <= impacted_dranges.upper_drange_index; drange_id++) {
                    MemTablePartition *partition = (*partitioned_active_memtables_)[drange_id];
                    uint32_t next_imm_slot = -1;
                    partition->mutex.Lock();
                    MemTable *table = partition->active_memtable;
                    if (table) {
                        auto atomic_table = versions_->mid_table_mapping_[table->memtableid()];
                        if (atomic_table->nentries_ == 0) {
                            atomic_table->generation_id_.store(impacted_dranges.generation_id);
                        } else {
                            if (!partition->available_slots.empty()) {
                                next_imm_slot = partition->available_slots.front();
                                partition->available_slots.pop();
                            }
                            if (next_imm_slot != -1) {
                                // Create a new table.
                                NOVA_ASSERT((*partitioned_imms_)[next_imm_slot] == 0);
                                (*partitioned_imms_)[next_imm_slot] = table->memtableid();
                                partition->slot_imm_id[next_imm_slot] = table->memtableid();
                                uint32_t memtable_id = memtable_id_seq_->fetch_add(1);
                                partition->immutable_memtable_ids.push_back(table->memtableid());
                                table = new MemTable(*internal_comparator_, memtable_id, nullptr, true);
                                auto new_atomic_table = versions_->mid_table_mapping_[table->memtableid()];
                                NOVA_ASSERT(memtable_id < MAX_LIVE_MEMTABLES);
                                new_atomic_table->SetMemTable(impacted_dranges.generation_id, table);
                                partition->active_memtable = table;
                                partition->AddMemTable(new_atomic_table->generation_id_, table->memtableid());
                            } else {
                                atomic_table->generation_id_.store(impacted_dranges.generation_id);
                            }
                        }
                    }
                    partition->mutex.Unlock();
                }
                flush_order_->UpdateImpactedDranges(impacted_dranges);
            }

            range_lock_.Lock();
            latest_->AssertSubrangeBoundary(user_comparator_);
            latest_subranges_.store(latest_);
            range_lock_.Unlock();
        } else {
            delete latest_;
            latest_ = nullptr;
        }
    }

// recover的时候用uniform分布预先处理subtrange
    void SubRangeManager::ConstructSubrangesWithUniform(const Comparator *user_comparator) {
        auto sr = new SubRanges;
        uint32_t cfgid = nova::NovaConfig::config->current_cfg_id;
        auto range = nova::NovaConfig::config->cfgs[cfgid]->fragments[dbindex_];
        int nkeys = range->range.key_end - range->range.key_start;

        // 如果整个数据库的key都没有 要分的分区多的话 (基本不可能出现这种情况)
        if (nkeys < options_.num_memtable_partitions) {
            int num_duplicates = options_.num_memtable_partitions / nkeys;
            int cdup = 0;
            int lower = range->range.key_start;
            int upper = range->range.key_start + 1;

            for (int i = 0; i < options_.num_memtable_partitions; i++) {
                SubRange nsr;
                Range r;
                r.lower = std::to_string(lower);
                r.upper = std::to_string(upper);
                if (num_duplicates == 1) {
                    r.num_duplicates = 0;
                    nsr.num_duplicates = 0;
                } else {
                    r.num_duplicates = num_duplicates;
                    nsr.num_duplicates = num_duplicates;
                }
                nsr.tiny_ranges.push_back(r);
                sr->subranges.push_back(nsr);
                cdup++;
                if (cdup == num_duplicates) {
                    lower = upper;
                    upper = lower + 1;
                    cdup = 0;
                }
                if (upper > range->range.key_end) {
                    break;
                }
            }
        // 正常情况(
        } else {
            int nkeys_per_range = nkeys / options_.num_memtable_partitions;
            int lower = range->range.key_start;
            int upper = range->range.key_start + nkeys_per_range;

            for (int i = 0; i < options_.num_memtable_partitions; i++) {
                if (i == options_.num_memtable_partitions - 1) {
                    upper = range->range.key_end;
                }
                SubRange nsr;
                Range r;
                r.lower = std::to_string(lower);
                r.upper = std::to_string(upper);
                nsr.tiny_ranges.push_back(r);
                sr->subranges.push_back(nsr);
                lower = upper;
                upper = lower + nkeys_per_range;
            }
        }
        /*
                    db
                    |
                    |
                ----------------
                |   ...  | ....
            subrange
                |   ...        
            tinyrange
        */
        sr->AssertSubrangeBoundary(user_comparator);
        NOVA_ASSERT(sr->first().first().lower_int() == range->range.key_start) << sr->DebugString();
        NOVA_ASSERT(sr->last().last().upper_int() == range->range.key_end) << sr->DebugString();
        ComputeCompactionThreadsAssignment(sr);
        latest_subranges_.store(sr);
        NOVA_LOG(rdmaio::INFO)
            << fmt::format("keys:{},{}", lower_bound_, upper_bound_);
    }

// 计算每一个subtrange对应的范围的compaction应该给谁做
    void
    SubRangeManager::ComputeCompactionThreadsAssignment(SubRanges *subranges) {
        if (options_.subrange_no_flush_num_keys == 0 || // config基本100
            !options_.enable_flush_multiple_memtables) { // config基本是true
            // MemTables of a subrange maybe assigned to any compaction thread.
            for (SubRange &subrange : subranges->subranges) {
                subrange.start_tid = 0;
                subrange.end_tid = options_.num_compaction_threads - 1;
            }
            return;
        }

// 正常情况
        NOVA_ASSERT(
                options_.num_compaction_threads >= subranges->subranges.size()); // 至少要做到一个subrange有一个cmopaction thread
        int thread_id = 0;
        // MemTables of a subrange is assigned to only one thread. key少的range线程分配少一点
        for (SubRange &subrange : subranges->subranges) {
            if (subrange.keys() <= options_.subrange_no_flush_num_keys) { // no flush num keys代表最少的开启写的key的数量??
                subrange.merge_memtables_without_flushing = true;
                subrange.start_tid = thread_id;
                subrange.end_tid = thread_id;
                thread_id = thread_id + 1;
            }
        }
        for (SubRange &subrange : subranges->subranges) { 
            if (subrange.keys() > options_.subrange_no_flush_num_keys) { // key多的range线程分配多一点
                subrange.merge_memtables_without_flushing = false; 
                subrange.start_tid = thread_id;
                subrange.end_tid = options_.num_compaction_threads - 1;
            }
        }
    }

    void SubRangeManager::ComputeLoadImbalance(const std::vector<double> &loads,
                                               leveldb::DBStats *db_stats) {
        double fair = 1.0 / (double) loads.size();
        double sum = 0.0;
        double highest_load = 0.0;
        double stdev = 0.0;
        for (int i = 0; i < loads.size(); i++) {
            double load = loads[i];
            sum += load;
            highest_load = std::max(highest_load, load);

            double diff = (load - fair) * 100.0;
            stdev += std::pow(diff, 2);
        }
        db_stats->load_imbalance.maximum_load_imbalance = (highest_load - fair) * 100.0 / fair;
        db_stats->load_imbalance.stdev = std::sqrt(stdev / (double) loads.size()) / 100.0;
    }

    void SubRangeManager::QueryDBStats(leveldb::DBStats *db_stats) {
        SubRanges *ref = latest_subranges_;
        db_stats->num_major_reorgs = num_major_reorgs;
        db_stats->num_minor_reorgs = num_minor_reorgs;
        db_stats->num_skipped_major_reorgs = num_skipped_major_reorgs;
        db_stats->num_skipped_minor_reorgs = num_skipped_minor_reorgs;
        db_stats->num_minor_reorgs_for_dup = num_minor_reorgs_for_dup;
        db_stats->num_minor_reorgs_samples = num_minor_reorgs_samples;
        std::vector<double> loads;
        uint64_t totalkeys = upper_bound_ - lower_bound_;
        if (nova::NovaConfig::config->client_access_pattern == "uniform") {
            for (int i = 0; i < ref->subranges.size(); i++) {
                SubRange &sr = ref->subranges[i];
                uint64_t lower = sr.first().lower_int();
                uint64_t upper = sr.last().upper_int();
                uint64_t keys = upper - lower;
                loads.push_back((double) keys / (double) totalkeys);
            }
        } else {
            // Zipfian.
            std::map<uint64_t, uint64_t> duplicated_keys;
            for (int i = 0; i < ref->subranges.size(); i++) {
                SubRange &sr = ref->subranges[i];
                if (sr.num_duplicates == 0) {
                    continue;
                }
                uint64_t lower = sr.first().lower_int();
                uint64_t upper = sr.first().upper_int();
                for (uint64_t k = lower; k < upper; k++) {
                    duplicated_keys[k] += 1;
                }
            }
            for (int i = 0; i < ref->subranges.size(); i++) {
                uint64_t accesses = 0;
                SubRange &sr = ref->subranges[i];
                uint64_t lower = sr.first().lower_int();
                uint64_t upper = sr.last().upper_int();

                for (uint64_t key = lower; key < upper; key++) {
                    if (duplicated_keys.find(key) !=
                        duplicated_keys.end()) {
                        accesses += nova::NovaConfig::config->zipfian_dist.accesses[key] / duplicated_keys[key];
                    } else {
                        accesses += nova::NovaConfig::config->zipfian_dist.accesses[key];
                    }
                }
                loads.push_back((double) (accesses) / (double) (nova::NovaConfig::config->zipfian_dist.sum));
            }
        }
        if (loads.size() != options_.num_memtable_partitions) {
            return;
        }
        ComputeLoadImbalance(loads, db_stats);
    }
}
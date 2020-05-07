
//
// Created by Haoyu Huang on 5/4/20.
// Copyright (c) 2020 University of Southern California. All rights reserved.
//

#ifndef LEVELDB_SUBRANGE_MANAGER_H
#define LEVELDB_SUBRANGE_MANAGER_H

#include "subrange.h"
#include "memtable.h"
#include "version_set.h"

#define SUBRANGE_WARMUP_NPUTS 1000000
#define SUBRANGE_MAJOR_REORG_INTERVAL 1000000
#define SUBRANGE_MINOR_REORG_INTERVAL 100000
#define SUBRANGE_REORG_INTERVAL 100000

#define SUBRANGE_REORG_DIFF_FROM_FAIR_THRESHOLD 20
#define SUBRANGE_MAJOR_REORG_THRESHOLD 0.3

namespace leveldb {
    class SubRangeManager {
    public:
        SubRangeManager(NovaCCMemFile *manifest_file,
                      const std::string &dbname,
                      VersionSet *versions,
                      const Options &options,
                      const Comparator *user_comparator,
                      std::vector<MemTablePartition *> *partitioned_active_memtables,
                      std::vector<uint32_t> *partitioned_imms);

        void PerformSubRangeReorganization(double processed_writes);

        int SearchSubranges(const leveldb::WriteOptions &options,
                            const leveldb::Slice &key,
                            const leveldb::Slice &val,
                            SubRange **subrange);

        uint64_t last_major_reorg_seq_ = 0;
        uint64_t last_minor_reorg_seq_ = 0;

        uint32_t num_major_reorgs = 0;
        uint32_t num_skipped_major_reorgs = 0;
        uint32_t num_minor_reorgs = 0;
        uint32_t num_minor_reorgs_for_dup = 0;
        uint32_t num_skipped_minor_reorgs = 0;

        std::atomic<SubRanges *> latest_subranges_;

    private:
        void PerformSubrangeMajorReorg(SubRanges *latest,
                                       double processed_writes);

        void MoveShareDuplicateSubranges(SubRanges *latest, int index);

        bool
        PerformSubrangeMinorReorgDuplicate(int subrange_id, SubRanges *latest,
                                           double total_inserts);

        bool MinorReorgDestroyDuplicates(SubRanges *latest, int subrange_id);

        void PerformSubrangeMinorReorg(int subrange_id, SubRanges *latest,
                                       double total_inserts);

        port::Mutex range_lock_;
        NovaCCMemFile *manifest_file_ = nullptr;
        std::string dbname_;
        VersionSet *versions_;
        const Options options_;
        const Comparator *user_comparator_;
        std::vector<MemTablePartition *> *partitioned_active_memtables_;
        std::vector<uint32_t> *partitioned_imms_;
    };
}


#endif //LEVELDB_SUBRANGE_MANAGER_H
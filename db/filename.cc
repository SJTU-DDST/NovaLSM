// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/filename.h"

#include <ctype.h>
#include <stdio.h>

#include "db/dbformat.h"
#include "leveldb/env.h"
#include "util/logging.h"

namespace leveldb {

// A utility routine: write "data" to the named file and Sync() it.
    Status WriteStringToFileSync(Env *env, const Slice &data,
                                 const std::string &fname);

// dbname + number + replica_id done
    static std::string MakeFileName(const std::string &dbname, uint64_t number,
                                    uint32_t replica_id,
                                    const char *suffix) {
        char buf[100];
        snprintf(buf, sizeof(buf), "/%06llu-%06llu.%s",
                 static_cast<unsigned long long>(number),
                 static_cast<unsigned long long>(replica_id), suffix);
        return dbname + buf;
    }

// dbname + number + 0 + log 本质上是wal 预设 done
    std::string LogFileName(const std::string &dbname, uint64_t number) {
        assert(number > 0);
        return MakeFileName(dbname, number, 0, "log");
    }

// 根据level来判断这个放置于哪里 done
    std::string TableFileName(const std::string &dbname, const std::string &pmname,
                              uint64_t number, int level, int levels_in_pm, 
                              FileInternalType internal_type, uint32_t replica_id) {
        assert(number > 0);
        if (level <= levels_in_pm){ // 放到pm上
            if (internal_type == FileInternalType::kFileMetadata) {
                return MakeFileName(pmname, number, replica_id, "ldb-meta");
            } else if (internal_type == FileInternalType::kFileParity) {
                return MakeFileName(pmname, number, replica_id, "ldb-parity");
            }
            return MakeFileName(pmname, number, replica_id, "ldb");
        }else{
            if (internal_type == FileInternalType::kFileMetadata) {
                return MakeFileName(dbname, number, replica_id, "ldb-meta");
            } else if (internal_type == FileInternalType::kFileParity) {
                return MakeFileName(dbname, number, replica_id, "ldb-parity");
            }
            return MakeFileName(dbname, number, replica_id, "ldb");
        }
    }

// 预先准备好是pm还是磁盘 done
    std::string DescriptorFileName(const std::string &dbname, uint64_t number,
                                   uint32_t replica_id) {
        char buf[100];
        snprintf(buf, sizeof(buf), "/MANIFEST-%06llu-%06llu",
                 static_cast<unsigned long long>(number),
                 static_cast<unsigned long long>(replica_id));
        return dbname + buf;
    }
// 预先准备好是pm还是磁盘 done
    std::string CurrentFileName(const std::string &dbname) {
        return dbname + "/CURRENT";
    }
// 预先准备好是pm还是磁盘 done
    std::string LockFileName(const std::string &dbname) {
        return dbname + "/LOCK";
    }
// 预先准备好是pm还是磁盘 done
    std::string TempFileName(const std::string &dbname, uint64_t number) {
        assert(number > 0);
        return MakeFileName(dbname, number, 0, "dbtmp");
    }
// 预先准备好是pm还是磁盘 done
    std::string InfoLogFileName(const std::string &dbname) {
        return dbname + "/LOG";
    }
// 预先准备好是pm还是磁盘 done
// Return the name of the old info log file for "dbname".
    std::string OldInfoLogFileName(const std::string &dbname) {
        return dbname + "/LOG.old";
    }

// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
    bool ParseFileName(const std::string &filename, uint64_t *number,
                       FileType *type) {
        Slice rest(filename);
        if (rest == "CURRENT") {
            *number = 0;
            *type = kCurrentFile;
        } else if (rest == "LOCK") {
            *number = 0;
            *type = kDBLockFile;
        } else if (rest == "LOG" || rest == "LOG.old") {
            *number = 0;
            *type = kInfoLogFile;
        } else if (rest.starts_with("MANIFEST-")) {
            rest.remove_prefix(strlen("MANIFEST-"));
            uint64_t num;
            uint64_t replica_id;
            if (!ConsumeDecimalNumber(&rest, &num)) {
                return false;
            }
            if (!ConsumeDecimalNumber(&rest, &replica_id)) {
                return false;
            }
            if (!rest.empty()) {
                return false;
            }
            *type = kDescriptorFile;
            *number = num;
        } else {
            // Avoid strtoull() to keep filename format independent of the
            // current locale
            uint64_t num;
            uint64_t replica_id;
            if (!ConsumeDecimalNumber(&rest, &num)) {
                return false;
            }
            if (!ConsumeDecimalNumber(&rest, &replica_id)) {
                return false;
            }
            Slice suffix = rest;
            if (suffix == Slice(".log")) {
                *type = kLogFile;
            } else if (suffix == Slice(".sst") || suffix == Slice(".ldb") || suffix == Slice(".ldb-parity") ||
                       suffix == Slice(".ldb-meta")) {
                *type = kTableFile;
            } else if (suffix == Slice(".dbtmp")) {
                *type = kTempFile;
            } else {
                return false;
            }
            *number = num;
        }
        return true;
    }

// 查看文件的类型
    bool ParseFileName(const std::string &filename,
                       FileType *type) {
        if (filename.rfind("CURRENT") != std::string::npos) {
            *type = kCurrentFile;
        } else if (filename.rfind("LOCK") != std::string::npos) {
            *type = kDBLockFile;
        } else if (filename.rfind("LOG") != std::string::npos) {
            *type = kInfoLogFile;
        } else if (filename.rfind("MANIFEST") != std::string::npos) {
            *type = kDescriptorFile;
        } else {
            if (filename.rfind("log") != std::string::npos) {
                *type = kLogFile;
            } else if (filename.rfind("ldb") != std::string::npos) {
                *type = kTableFile;
            } else if (filename.rfind("tmp") != std::string::npos) {
                *type = kTempFile;
            } else {
                return false;
            }
        }
        return true;
    }

//    Status SetCurrentFile(Env *env, const std::string &dbname,
//                          uint64_t descriptor_number) {
//        // Remove leading "dbname/" and add newline to manifest file name
//        std::string manifest = DescriptorFileName(dbname, descriptor_number);
//        Slice contents = manifest;
//        assert(contents.starts_with(dbname + "/"));
//        contents.remove_prefix(dbname.size() + 1);
//        std::string tmp = TempFileName(dbname, descriptor_number);
//        Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp);
//        if (s.ok()) {
//            s = env->RenameFile(tmp, CurrentFileName(dbname));
//        }
//        if (!s.ok()) {
//            env->DeleteFile(tmp);
//        }
//        return s;
//    }

// manifest也放进pm里面
    bool IsPMfile(const std::string &filename){
        size_t found = filename.find("pm");
        size_t foundm = filename.find("MANIFEST");
        if (found != std::string::npos || foundm != std::string::npos) {
            return true;
        } else {
            return false;
        }
    }

// tbd
    bool ParseDBindexFromFilename(const std::string &filename, int *index){
        *index = 0;
        return true;
    }

}  // namespace leveldb

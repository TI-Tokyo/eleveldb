// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2015 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#ifndef WORKITEMS_H
#define WORKITEMS_H

#include <stdint.h>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "extractor.h"
#include "filter.h"
#include "Encoding.h"

#define LEVELDB_PLATFORM_POSIX
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/thread_tasks.h"

#include "work_result.hpp"
#include "atoms.h"
#include "refobjects.h"

namespace eleveldb {

/* Type returned from a work task: */
typedef basho::async_nif::work_result work_result;


/**
 * Virtual base class for async NIF work items:
 */
class WorkTask : public leveldb::ThreadTask {
protected:
        ReferencePtr<DbObject> m_DbPtr;   //!< access to database, and holds reference

        ErlNifEnv*     local_env_;
        ERL_NIF_TERM   caller_ref_term;
        ERL_NIF_TERM   caller_pid_term;
        bool           terms_set;

        ErlNifPid local_pid;   // maintain for task lifetime (JFW)

public:
        WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref);
        WorkTask(ErlNifEnv *caller_env, ERL_NIF_TERM& caller_ref, DbObjectPtr_t& DbPtr);

        virtual ~WorkTask();

        // this is the method called from the thread pool's worker thread; it
        // calls DoWork(), implemented in the subclass, and returns the result
        // of the work to the caller
        virtual void operator()();

        virtual ErlNifEnv *local_env()         { return local_env_; }

        // call local_env() since the virtual creates the data in MoveTask
        const ERL_NIF_TERM& caller_ref()       { local_env(); return caller_ref_term; }
        const ERL_NIF_TERM& pid()              { local_env(); return caller_pid_term; }

protected:
        // this is the method that does the real work for this task
        virtual work_result DoWork() = 0;

private:
        WorkTask();
        WorkTask(const WorkTask &);
        WorkTask & operator=(const WorkTask &);
};


/**
 * Background object for async open of a leveldb instance
 */

class OpenTask : public WorkTask {
protected:
        std::string db_name;
        leveldb::Options* open_options;  // associated with db handle, we don't free it

public:
        OpenTask(ErlNifEnv* caller_env, ERL_NIF_TERM& _caller_ref,
                 const std::string& db_name_, leveldb::Options *open_options_)
                : WorkTask(caller_env, _caller_ref),
                  db_name(db_name_), open_options(open_options_)
                {}

        virtual ~OpenTask() {}

protected:
        virtual work_result DoWork();

private:
        OpenTask();
        OpenTask(const OpenTask &);
        OpenTask & operator=(const OpenTask &);
};



/**
 * Background object for async write
 */

class WriteTask : public WorkTask {
protected:
        leveldb::WriteBatch*    batch;
        leveldb::WriteOptions*  options;

public:
        WriteTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
                  DbObjectPtr_t & _db_handle,
                  leveldb::WriteBatch* _batch,
                  leveldb::WriteOptions* _options)
                : WorkTask(_owner_env, _caller_ref, _db_handle),
                  batch(_batch),
                  options(_options)
                {}

        virtual ~WriteTask()
                {
                        delete batch;
                        delete options;
                }

protected:
        virtual work_result DoWork()
                {
                        leveldb::Status status = m_DbPtr->m_Db->Write(*options, batch);
                        return (status.ok()
                                ? work_result(ATOM_OK)
                                : work_result(local_env(), ATOM_ERROR_DB_WRITE, status));
                }

private:
        WriteTask();
        WriteTask(const WriteTask &);
        WriteTask & operator=(const WriteTask &);
};


/**
 * Alternate object for retrieving data out of leveldb.
 *  Reduces one memcpy operation.
 */
class BinaryValue : public leveldb::Value {
private:
        ErlNifEnv* m_env;
        ERL_NIF_TERM& m_value_bin;

        BinaryValue(const BinaryValue&);
        void operator=(const BinaryValue&);

public:
        BinaryValue(ErlNifEnv* env, ERL_NIF_TERM& value_bin)
                : m_env(env), m_value_bin(value_bin)
                {}

        virtual ~BinaryValue() {}

        BinaryValue& assign(const char* data, size_t size)
                {
                        unsigned char* v = enif_make_new_binary(m_env, size, &m_value_bin);
                        memcpy(v, data, size);
                        return *this;
                }
};


/**
 * Background object for async get,
 *  using new BinaryValue object
 */

class GetTask : public WorkTask {
protected:
        std::string m_Key;
        leveldb::ReadOptions options;

public:
        GetTask(ErlNifEnv *_caller_env,
                ERL_NIF_TERM _caller_ref,
                DbObjectPtr_t & _db_handle,
                ERL_NIF_TERM _key_term,
                leveldb::ReadOptions &_options)
                : WorkTask(_caller_env, _caller_ref, _db_handle),
                  options(_options)
                {
                        ErlNifBinary key;

                        enif_inspect_binary(_caller_env, _key_term, &key);
                        m_Key.assign((const char*)key.data, key.size);
                }

        virtual ~GetTask() {}
        work_result DoWork();
};



/**
 * Background object to open/start an iteration
 */

class IterTask : public WorkTask {
protected:
    const bool keys_only;
    leveldb::ReadOptions options;

public:
    IterTask(ErlNifEnv *_caller_env,
             ERL_NIF_TERM _caller_ref,
             DbObjectPtr_t & _db_handle,
             const bool _keys_only,
             leveldb::ReadOptions &_options)
            : WorkTask(_caller_env, _caller_ref, _db_handle),
              keys_only(_keys_only), options(_options)
                {}

        virtual ~IterTask() {}

        work_result DoWork();
};


class MoveTask : public WorkTask {
public:
        typedef enum { FIRST, LAST, NEXT, PREV, SEEK, PREFETCH, PREFETCH_STOP } action_t;

protected:
        ItrObjectPtr_t m_Itr;

public:
        action_t action;
        std::string seek_target;

public:
        // No seek target:
        MoveTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
                 ItrObjectPtr_t& Iter, action_t& _action);
        // With seek target:
        MoveTask(ErlNifEnv *_caller_env, ERL_NIF_TERM _caller_ref,
                 ItrObjectPtr_t& Iter, action_t& _action,
                 std::string& _seek_target);

        virtual ~MoveTask() {}

        ErlNifEnv *local_env();

        void recycle();

protected:
        work_result DoWork();
};


/**
 * Background object for async database close
 */

class CloseTask : public WorkTask {
public:
        CloseTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
                  DbObjectPtr_t& _db_handle)
                : WorkTask(_owner_env, _caller_ref, _db_handle)
                {}

        virtual ~CloseTask() {}

        work_result DoWork();
};


/**
 * Background object for async iterator close
 */

class ItrCloseTask : public WorkTask {
protected:
        ReferencePtr<ItrObject> m_ItrPtr;

public:
        ItrCloseTask(ErlNifEnv* _owner_env, ERL_NIF_TERM _caller_ref,
                     ItrObjectPtr_t& _itr_handle)
                : WorkTask(_owner_env, _caller_ref),
                  m_ItrPtr(_itr_handle)
                {}

        virtual ~ItrCloseTask() {}

        work_result DoWork();
};


/**
 * Background object for async open of a leveldb instance
 */

class DestroyTask : public WorkTask {
protected:
        std::string db_name;
        leveldb::Options* open_options;  // associated with db handle, we don't free it

public:
        DestroyTask(ErlNifEnv* caller_env, ERL_NIF_TERM& _caller_ref,
                    const std::string& db_name_, leveldb::Options *open_options_)
                : WorkTask(caller_env, _caller_ref),
                  db_name(db_name_), open_options(open_options_)
                {}

        virtual ~DestroyTask() {}

protected:
        work_result DoWork();

private:
        DestroyTask();
        DestroyTask(const DestroyTask&);
        DestroyTask & operator=(const DestroyTask&);

};

//=======================================================================
// An object for storing range_scan options
//=======================================================================

struct RangeScanOptions {
        // Byte-level controls for batching/ack
        size_t max_unacked_bytes = 10 * 1024 * 1024;
        size_t low_bytes         = 2 * 1024 * 1024;
        size_t max_batch_bytes   = 1 * 1024 * 1024;

        // Max number of items to return. Zero means unlimited.
        size_t limit = 0;

        // Include the start key in streaming iteration?
        bool start_inclusive = true;

        // Include the end key in streaming iteration?
        bool end_inclusive = false;

        // Read options
        bool fill_cache = false;
        bool verify_checksums = true;

        // Filter options
        ERL_NIF_TERM rangeFilterSpec_;
        ERL_NIF_TERM fieldTypes_;

        ErlNifEnv* env_ = nullptr;
        bool useRangeFilter_ = false;
};

class RangeScanTask : public WorkTask {
public:
        // Used to coordinate production and consumption of batches of data.
        // Producers acknowledge each batch received. Consumers block when the
        // unacked limit has been reached and need to be woken up by the consumer.
        // When consumers die, the ref count is decremented and that will signal
        // the producer to go away too.

        class SyncObject : public RefObject {
        public:
                explicit SyncObject(const RangeScanOptions& opts);
                ~SyncObject();

                // True if only one side (producer or consumer) alive.
                inline bool SingleOwned()
                        {
                                return 1 == GetRefCount();
                        }

                // Adds number of bytes sent to count.
                // Will block if count exceeds max waiting for the other
                // side to ack some and take it under the limit or for the other
                // side to shut down.
                void AddBytes(uint32_t n);

                void AckBytes(uint32_t n);
                bool AckBytesRet(uint32_t n);

                // Should be called when the Erlang handle is garbage collected
                // so no process is there to consume the output.
                void MarkConsumerDead();

                bool IsConsumerDead() const;

        private:
                const uint32_t max_bytes_;
                const uint32_t low_bytes_;
                volatile uint32_t num_bytes_;
                volatile bool producer_sleeping_;

                // Set if producer filled up but consumer acked before
                // producer went to sleep. Producer should abort going to
                // sleep upon seeing this set.

                volatile bool pending_signal_;
                volatile bool consumer_dead_;
                volatile bool crossed_under_max_;

                ErlNifMutex* mutex_;
                ErlNifCond*  cond_;
        };

        struct SyncHandle {
                SyncObject* sync_obj_;
        };

        RangeScanTask(ErlNifEnv* caller_env,
                      ERL_NIF_TERM caller_ref,
                      DbObjectPtr_t & _db_handle,
                      const std::string& start_key,
                      const std::string* end_key,
                      RangeScanOptions&  options,
                      SyncObject* sync_obj);

        virtual ~RangeScanTask();

        static void CreateSyncHandleType(ErlNifEnv* env);
        static SyncHandle* CreateSyncHandle(const RangeScanOptions & options);
        static SyncHandle* RetrieveSyncHandle(ErlNifEnv* env, ERL_NIF_TERM term);
        static void SyncHandleResourceCleanup(ErlNifEnv* env, void* arg);

        void sendMsg(ErlNifEnv* msg_env, ERL_NIF_TERM atom, ErlNifPid pid);
        void sendMsg(ErlNifEnv* msg_env, ERL_NIF_TERM atom, ErlNifPid pid, std::string msg);

        int VarintLength(uint64_t v);
        char* EncodeVarint64(char* dst, uint64_t v);
        void send_streaming_batch(ErlNifPid* pid, ErlNifEnv* msg_env, ERL_NIF_TERM ref_term,
                                  ErlNifBinary* bin);

protected:
        work_result DoWork();

        RangeScanOptions options_;
        std::string start_key_;
        std::string end_key_;
        bool has_end_key_;
        SyncObject* sync_obj_;
        ExpressionNode<bool>* range_filter_;

        ExtractorMsgpack extractor;

private:

        static ErlNifResourceType* sync_handle_resource_;
};


} // namespace eleveldb

#endif

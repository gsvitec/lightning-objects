//
// Created by chris on 10/7/15.
//

#include "lmdb_kvstore.h"
#include "liblmdb/lmdb++.h"
#include <algorithm>
#include <sys/stat.h>

namespace flexis {
namespace persistence {
namespace lmdb {

using namespace std;

static const char * CLASSDATA = "classdata";
static const char * CLASSMETA = "classmeta";

namespace ps = flexis::persistence;

static const unsigned ObjectId_off = ClassId_sz;
static const unsigned PropertyId_off = ClassId_sz + ObjectId_sz;

#define SK_CONSTR(nm, c, o, p) byte_t nm[StorageKey::byteSize]; *(ClassId *)nm = c; *(ObjectId *)(nm+ObjectId_off) = o; \
*(PropertyId *)(nm+PropertyId_off) = p

#define SK_RET(nm, k) nm.classId = *(ClassId *)k; nm.objectId = *(ObjectId *)(k+ObjectId_off); \
nm.propertyId = *(PropertyId *)(k+PropertyId_off)

#define SK_CLASSID(k) *(ClassId *)k
#define SK_OBJID(k) *(ObjectId *)(k+ObjectId_off)
#define SK_PROPID(k) *(PropertyId *)(k+PropertyId_off)

int key_compare(const MDB_val *a, const MDB_val *b)
{
  byte_t *k1 = (byte_t *)a->mv_data;
  byte_t *k2 = (byte_t *)b->mv_data;

  int c = *(ClassId *)k1 - *(ClassId *)k2;
  if(c == 0) {
    k1 += ClassId_sz;
    k2 += ClassId_sz;

    c = *(ObjectId *)k1 - *(ObjectId *)k2;
    if(c == 0) {
      k1 += ObjectId_sz;
      k2 += ObjectId_sz;
      c = *(PropertyId *)k1 - *(PropertyId *)k2 ;
    }
  }
  return c;
}

/**
 * class cursor backend. Iterates over all instances of a given class
 */
class FlexisPersistence_EXPORT ClassCursorHelper : public flexis::persistence::kv::CursorHelper
{
  ::lmdb::txn &m_txn;
  ::lmdb::dbi &m_dbi;

  ::lmdb::cursor m_cursor;
  ::lmdb::val m_keyval;

  ClassId m_classId;

protected:
  bool start() override
  {
    SK_CONSTR(sk, m_classId, 0, 0);
    m_keyval.assign(sk, sizeof(sk));

    if(m_cursor.get(m_keyval, MDB_SET_RANGE)) {
      return SK_CLASSID(m_keyval.data<byte_t>()) == m_classId;
    }
    return false;
  }

  ObjectId key() override
  {
    m_cursor.get(m_keyval, MDB_GET_CURRENT);
    return SK_OBJID(m_keyval.data<byte_t>());
  }

  bool next() override
  {
    while(m_cursor.get(m_keyval, MDB_NEXT)) {
      if(SK_CLASSID(m_keyval.data<byte_t>()) != m_classId) {
        //not anymore the requested class
        return false;
      }
      else if(SK_PROPID(m_keyval.data<byte_t>()) == 0) {
        //its a complex property. Skip
        return true;
      }
    }
    return false;
  }

  void erase() override
  {
    m_cursor.del();
  }

  virtual void close() override {
    m_cursor.close();
  }

  void get(StorageKey &key, ReadBuf &rb) override
  {
    ::lmdb::val dataval{};
    if(m_cursor.get(m_keyval, dataval, MDB_GET_CURRENT)) {
      SK_RET(key, m_keyval.data<byte_t>());
      rb.start(dataval.data<byte_t>(), dataval.size());
    }
  }

  const byte_t *getObjectData() override {
    ::lmdb::val dataval{};
    if(m_cursor.get(m_keyval, dataval, MDB_GET_CURRENT)) {
      return dataval.data<byte_t>();
    }
    return nullptr;
  }

public:
  ClassCursorHelper(::lmdb::txn &txn, ::lmdb::dbi &dbi, ClassId classId)
      : m_txn(txn), m_dbi(dbi), m_cursor(::lmdb::cursor::open(m_txn, m_dbi)), m_classId(classId)
  {}
  ~ClassCursorHelper() {m_cursor.close();}
};

/**
 * cursor over collection chunks
 */
class ChunkCursorImpl : public flexis::persistence::kv::ChunkCursor
{
  const ClassId m_classId;
  const ObjectId m_objectId;

  ::lmdb::txn &m_txn;
  ::lmdb::dbi &m_dbi;
  ::lmdb::val keyval;
  ::lmdb::val dataval;
  ::lmdb::cursor m_cursor;

public:
  ChunkCursorImpl(::lmdb::txn &txn, ::lmdb::dbi &dbi, ClassId classId, ObjectId objectId, bool toEnd=false)
      : m_txn(txn), m_dbi(dbi), m_cursor(::lmdb::cursor::open(txn, dbi)), m_classId(classId), m_objectId(objectId)
  {
    if(toEnd) {
      SK_CONSTR(k, classId, objectId, 0xFFFF);
      keyval.assign(k, sizeof(k));

      auto cursor = ::lmdb::cursor::open(m_txn, m_dbi);

      bool ok;
      if(cursor.get(keyval, nullptr, MDB_SET_RANGE))
        ok = cursor.get(keyval, dataval, MDB_PREV);
      else
        ok = cursor.get(keyval, dataval, MDB_LAST);

      m_atEnd = !(ok && SK_CLASSID(keyval.data<byte_t>()) == classId && SK_OBJID(keyval.data<byte_t>()) == objectId);
    }
    else {
      SK_CONSTR(k, classId, objectId, 1);
      keyval.assign(k, sizeof(k));

      m_atEnd = !m_cursor.get(keyval, dataval, MDB_SET);
    }
  }

  PropertyId chunkId() override {
    return SK_PROPID(keyval.data<byte_t>());
  }

  bool next() override {
    m_atEnd = !m_cursor.get(keyval, dataval, MDB_NEXT);
    if(!m_atEnd)
      m_atEnd = SK_CLASSID(keyval.data<byte_t>()) != m_classId || SK_OBJID(keyval.data<byte_t>()) != m_objectId;
    return !m_atEnd;
  }

  void get(ReadBuf &rb) override {
    rb.start(dataval.data<byte_t>(), dataval.size());
  }

  void close() override {
    m_cursor.close();
  }
};


/**
 * collection cursor backend. Iterates over all elements in a top-level collection
 */
class FlexisPersistence_EXPORT CollectionCursorHelper : public flexis::persistence::kv::CursorHelper
{
  ::lmdb::txn &m_txn;
  ::lmdb::dbi &m_dbi;

  const ClassId m_classId;
  const ObjectId m_collectionId;

  ReadBuf m_readBuf;
  size_t m_chunkSize, m_chunkIndex;
  const byte_t *m_data = nullptr;
  ChunkCursorImpl *m_chunkCursor = nullptr;

  bool prepare_chunk() {
    m_chunkSize = m_chunkIndex=0;
    if(!m_chunkCursor->atEnd()) {
      m_chunkCursor->get(m_readBuf);
      m_chunkSize = m_readBuf.readInteger<size_t>(4);
      m_readBuf.readInteger<ObjectId>(ObjectId_sz); //throw away
      m_data = m_readBuf.cur();
      return true;
    }
    return false;
  }

protected:
  bool start() {
    m_chunkCursor = new ChunkCursorImpl(m_txn, m_dbi, m_classId, m_collectionId);
    return prepare_chunk();
  }

  bool next()
  {
    if(++m_chunkIndex >= m_chunkSize) {
      m_chunkCursor->next();
      if(!prepare_chunk()) return false;
    }
    else
      m_data = m_readBuf.cur() + m_chunkIndex * StorageKey::byteSize;
    return true;
  }

  void erase() {
    throw persistence_error("not implemented");
  }

  ObjectId key() {
    return SK_OBJID(m_data);
  }

  void close() {
  }

  void get(StorageKey &key, ReadBuf &rb)
  {
    key.classId = SK_CLASSID(m_data);
    key.objectId = SK_OBJID(m_data);
    key.propertyId = SK_PROPID(m_data);

    ::lmdb::val keyval {m_data, StorageKey::byteSize};
    ::lmdb::val dataval;

    if(m_dbi.get(m_txn, keyval, dataval))
      rb.start(dataval.data<byte_t>(), dataval.size());
  }

  const byte_t *getObjectData()
  {
    ::lmdb::val keyval {m_data, StorageKey::byteSize};
    ::lmdb::val dataval;

    return m_dbi.get(m_txn, keyval, dataval) ? dataval.data<byte_t>() : nullptr;
  }

public:
  CollectionCursorHelper(::lmdb::txn &txn, ::lmdb::dbi &dbi, ClassId classId, ObjectId collectionId)
  : m_txn(txn), m_dbi(dbi), m_classId(classId), m_collectionId(collectionId)
  {}
  ~CollectionCursorHelper() {
    if(m_chunkCursor) delete m_chunkCursor;
  }
};

/**
 * LMDB-based class cursor backend. Iterates over all elements in a vector (member vaŕiable)
 */
class FlexisPersistence_EXPORT VectorCursorHelper : public flexis::persistence::kv::CursorHelper
{
  ::lmdb::txn &m_txn;
  ::lmdb::dbi &m_dbi;

  ::lmdb::val m_vectordata;
  size_t m_index, m_size;

  const ClassId m_classId;
  const ObjectId m_objectId;
  const PropertyId m_propertyId;

protected:
  bool start() override
  {
    m_index = m_size = 0;

    ::lmdb::val keyval;
    SK_CONSTR(sk, m_classId, m_objectId, m_propertyId);
    keyval.assign(sk, sizeof(sk));

    if(m_dbi.get(m_txn, keyval, m_vectordata)) {
      m_size = m_vectordata.size() / StorageKey::byteSize;
      return true;
    }
    return false;
  }

  ObjectId key() override
  {
    return SK_OBJID(m_vectordata.data<byte_t>() + m_index * StorageKey::byteSize);
  }

  bool next() override
  {
    return ++m_index < m_size;
  }

  void erase() override
  {
    const byte_t *keydata = m_vectordata.data<byte_t>() + m_index * StorageKey::byteSize;
    ::lmdb::val keyval;
    keyval.assign(keydata, StorageKey::byteSize);
    m_dbi.del(m_txn, keyval);
  }

  virtual void close() override {
    m_index = m_size = 0;
  }

  void get(StorageKey &key, ReadBuf &rb) override
  {
    if(m_index < m_size) {
      const byte_t *keydata = m_vectordata.data<byte_t>() + m_index * StorageKey::byteSize;

      ::lmdb::val keyval;
      keyval.assign(keydata, StorageKey::byteSize);
      ::lmdb::val dataval;

      if(m_dbi.get(m_txn, keyval, dataval)) {
        SK_RET(key, keydata);
        rb.start(dataval.data<byte_t>(), dataval.size());
      }
      else {
        throw new persistence_error("corrupted vector: item not found");
      }
    }
  }

  const byte_t *getObjectData() override {
    if(m_index < m_size) {
      const byte_t *keydata = m_vectordata.data<byte_t>() + m_index * StorageKey::byteSize;
      ::lmdb::val keyval;
      keyval.assign(keydata, StorageKey::byteSize);
      ::lmdb::val dataval;

      if(m_dbi.get(m_txn, keyval, dataval)) {
        return dataval.data<byte_t>();
      }
      else {
        throw new persistence_error("corrupted vector: item not found");
      }
    }
    return nullptr;
  }

public:
  VectorCursorHelper(::lmdb::txn &txn, ::lmdb::dbi &dbi, ClassId classId, ObjectId objectId, PropertyId propertyId)
      : m_txn(txn), m_dbi(dbi), m_classId(classId), m_objectId(objectId), m_propertyId(propertyId)
  {}
  ~VectorCursorHelper() {}
};

/**
 * LMDB-based Transaction
 */
class FlexisPersistence_EXPORT Transaction
    : public flexis::persistence::kv::WriteTransaction,
      public flexis::persistence::kv::ExclusiveReadTransaction
{
public:
  enum class Mode {read, append, write};

private:
  const ::lmdb::env &m_env;

  ::lmdb::txn m_txn;
  ::lmdb::dbi &m_dbi;

  Mode m_mode;
  bool m_closed = false;

protected:
  bool putData(ClassId classId, ObjectId objectId, PropertyId propertyId, WriteBuf &buf) override;
  bool allocData(ClassId classId, ObjectId objectId, PropertyId propertyId, size_t size, byte_t **data) override;
  void getData(ReadBuf &buf, ClassId classId, ObjectId objectId, PropertyId propertyId) override;
  bool remove(ClassId classId, ObjectId objectId, PropertyId propertyid) override;

  ClassCursorHelper * _openCursor(ClassId classId) override;
  CollectionCursorHelper * _openCursor(ClassId classId, ObjectId collectionId) override;
  VectorCursorHelper * _openCursor(ClassId classId, ObjectId objectId, PropertyId propertyId) override;

  bool _getCollectionData(CollectionInfo *info, size_t startIndex, size_t length, size_t elementSize,
                          void **data, bool *owned) override;

  PropertyId getMaxPropertyId(ClassId classId, ObjectId objectId) override;
  bool getNextChunkInfo(ObjectId collectionId, PropertyId *propertyId, size_t *startIndex) override;
  bool lastChunk(ObjectId collectionId, PropertyId &chunkId, ::lmdb::val &data);
  ChunkCursor::Ptr _openChunkCursor(ClassId classId, ObjectId objectId, bool atEnd) override;

public:
  Transaction(ps::KeyValueStore &store, Mode mode, ::lmdb::env &env, ::lmdb::dbi &dbi, bool blockWrites=false)
      : flexis::persistence::kv::ReadTransaction(store),
        flexis::persistence::kv::WriteTransaction(store, mode == Mode::append),
        flexis::persistence::kv::ExclusiveReadTransaction(store),
        m_mode(mode),
        m_env(env),
        m_dbi(dbi),
        m_txn(::lmdb::txn::begin(env, nullptr, mode == Mode::read ? MDB_RDONLY : 0))
  {
    setBlockWrites(blockWrites);
  }

  bool isClosed() {return m_closed;}

  void doCommit() override;
  void doAbort() override;
  void doReset() override;
  void doRenew() override;
};

/**
 * LMDB-based KeyValueStore implementation
 */
class KeyValueStoreImpl : public KeyValueStore
{
  ::lmdb::env m_env;
  ::lmdb::dbi m_dbi_meta = 0;
  ::lmdb::dbi m_dbi_data = 0;

  unsigned m_flags;
  weak_ptr<Transaction> writeTxn;
  string m_dbpath;
  Options m_options;

  size_t m_curMapSize;
  unsigned m_pageSize;
  unsigned m_writeBlocks = 0;

  static int meta_dup_compare(const MDB_val *a, const MDB_val *b);

  PropertyMetaInfoPtr make_metainfo(MDB_val *mdbVal);
  MDB_val make_val(unsigned id, const PropertyAccessBase *prop);
  ObjectId findMaxObjectId(::lmdb::txn &txn, ClassId classId);

protected:
  void loadSaveClassMeta(
      AbstractClassInfo *classInfo,
      PropertyAccessBase * currentProps[],
      unsigned numProps,
      std::vector<PropertyMetaInfoPtr> &propertyInfos) override;

  void checkAvailableSpace(unsigned needsKBs);

public:
  KeyValueStoreImpl(std::string location, std::string name, Options options);
  ~KeyValueStoreImpl();

  ps::ReadTransactionPtr beginRead() override;
  ps::ExclusiveReadTransactionPtr beginExclusiveRead() override;
  ps::WriteTransactionPtr beginWrite(bool append, unsigned needsKBs) override;

  void transactionCompleted(Transaction::Mode mode, bool blockWrites);
};

//Start KeyValueStoreImpl implementation

KeyValueStore::Factory::operator flexis::persistence::KeyValueStore *() const
{
  try {
    return new KeyValueStoreImpl(location, name, options);
  }
  catch(::lmdb::error &err) {
    throw persistence_error(err.what());
  }
}

#ifdef _WIN32
    static const char separator_char = '\\';
#else
    static const char separator_char = '/';
#endif

int KeyValueStoreImpl::meta_dup_compare(const MDB_val *a, const MDB_val *b)
{
  PropertyId id1 = read_integer<PropertyId>((byte_t *)a->mv_data, 2);
  PropertyId id2 = read_integer<PropertyId>((byte_t *)b->mv_data, 2);

  return id1 - id2;
}

KeyValueStoreImpl::KeyValueStoreImpl(string location, string name, Options options)
    : m_env(::lmdb::env::create()), m_options(options), m_curMapSize(options.initialMapSizeMB * size_t(1024) * size_t(1024))
{
  m_dbpath = location;
  if(m_dbpath.back() != separator_char) m_dbpath += separator_char;
  m_dbpath += (name.empty() ? "kvdata" : name);

  //don't need to worry for existing files. LMDB will increase to committed size if neeed
  m_env.set_mapsize(m_curMapSize);

  //classmeta + classdata db
  m_env.set_max_dbs(2);
  m_flags = MDB_NOSUBDIR;

  if(!m_options.lockFile) m_flags |= MDB_NOLOCK;
  if(m_options.writeMap) m_flags |= MDB_WRITEMAP;

  try {
    m_env.open(m_dbpath.c_str(), m_flags, 0664);
  }
  catch(::lmdb::runtime_error e) {
    throw persistence_error("error opening database", e.what());
  }

  MDB_stat envstat;
  mdb_env_stat(m_env, &envstat);
  m_pageSize = envstat.ms_psize;

  //make sure we've got room to grow
  checkAvailableSpace(0);

  m_reuseChunkspace = options.writeMap;

  auto txn = ::lmdb::txn::begin(m_env, nullptr);

  //open/create the classmeta database
  m_dbi_meta = ::lmdb::dbi::open(txn, CLASSMETA, MDB_DUPSORT | MDB_CREATE);
  m_dbi_meta.set_dupsort(txn, meta_dup_compare);

  //find the maximum classId
  ::lmdb::val key, val;

  key.assign((byte_t *)0, 0);
  val.assign((byte_t *)0, 0);
  auto cursor = ::lmdb::cursor::open(txn, m_dbi_meta);
  while (cursor.get(key, val, MDB_NEXT_NODUP)) {
    ClassId cid = read_integer<ClassId>(val.data<byte_t>()+2, 2);
    if(cid > m_maxClassId) m_maxClassId = cid;
  }
  cursor.close();

  //open/create the classdata database
  m_dbi_data = ::lmdb::dbi::open(txn, CLASSDATA, MDB_CREATE);
  m_dbi_data.set_compare(txn, key_compare);

  m_maxCollectionId = findMaxObjectId(txn, COLLECTION_CLSID);

  txn.commit();
}

KeyValueStoreImpl::~KeyValueStoreImpl()
{
  MDB_envinfo envinfo;
  mdb_env_info(m_env, &envinfo);

  //truncate the file
  size_t datasize = m_pageSize * (envinfo.me_last_pgno + 1);
  m_env.set_mapsize(datasize);

  m_env.close();
}

//check available space and increase map if needed. Only callable when no transactions are active
void KeyValueStoreImpl::checkAvailableSpace(unsigned needsKBs)
{
  MDB_envinfo envinfo;
  mdb_env_info(m_env, &envinfo);

  if(!needsKBs || needsKBs < m_options.minTransactionSpaceKB) needsKBs = m_options.minTransactionSpaceKB;

  size_t cursize = m_pageSize * (envinfo.me_last_pgno + 1);
  if(cursize + needsKBs * 1024 > m_curMapSize) {
    m_curMapSize += m_options.increaseMapSizeKB * 1024;
    m_env.set_mapsize(m_curMapSize);
  }
}

void KeyValueStoreImpl::transactionCompleted(Transaction::Mode mode, bool blockWrites)
{
  if(blockWrites) m_writeBlocks--;
}

ps::ReadTransactionPtr KeyValueStoreImpl::beginRead()
{
  return ps::ReadTransactionPtr(new Transaction(*this, Transaction::Mode::read, m_env, m_dbi_data, false));
}

ps::ExclusiveReadTransactionPtr KeyValueStoreImpl::beginExclusiveRead()
{
  shared_ptr<Transaction> wtr = writeTxn.lock();
  if(wtr && !wtr->isClosed()) throw invalid_argument("a write transaction is already running");
  m_writeBlocks++;

  return ps::ExclusiveReadTransactionPtr(new Transaction(*this, Transaction::Mode::read, m_env, m_dbi_data, true));
}

ps::WriteTransactionPtr KeyValueStoreImpl::beginWrite(bool append, unsigned needsKBs)
{
  if(m_writeBlocks)
    throw invalid_argument("write operations are blocked by a running transaction");

  shared_ptr<Transaction> wtr = writeTxn.lock();
  if(wtr && !wtr->isClosed()) throw invalid_argument("a write transaction is already running");

  checkAvailableSpace(needsKBs);
  Transaction::Mode mode = append ? Transaction::Mode::append : Transaction::Mode::write;
  auto tptr = shared_ptr<Transaction>(new Transaction(*this, mode, m_env, m_dbi_data));
  writeTxn = tptr;

  return tptr;
}

void Transaction::doCommit()
{
  m_txn.commit();
  m_closed = true;
  ((KeyValueStoreImpl *)&store)->transactionCompleted(m_mode, m_blockWrites);
}

void Transaction::doAbort()
{
  m_txn.abort();
  m_closed = true;
  ((KeyValueStoreImpl *)&store)->transactionCompleted(m_mode, m_blockWrites);
}

void Transaction::doReset()
{
  m_txn.reset();
}

void Transaction::doRenew()
{
  m_txn.renew();
}

bool Transaction::putData(ClassId classId, ObjectId objectId, PropertyId propertyId, WriteBuf &buf)
{
  SK_CONSTR(kv, classId, objectId, propertyId);
  ::lmdb::val k{kv, sizeof(kv)};
  ::lmdb::val v{buf.data(), buf.size()};

  return ::lmdb::dbi_put(m_txn, m_dbi.handle(), k, v, m_append ? MDB_APPEND : 0);
}

bool Transaction::allocData(ClassId classId, ObjectId objectId, PropertyId propertyId, size_t size, byte_t **data)
{
  SK_CONSTR(kv, classId, objectId, propertyId);
  ::lmdb::val k{kv, sizeof(kv)};
  ::lmdb::val v{nullptr, size};

  if(::lmdb::dbi_put(m_txn, m_dbi.handle(), k, v, MDB_RESERVE)) {
    *data = v.data<byte_t>();
    return true;
  }
  return false;
}

void Transaction::getData(ReadBuf &buf, ClassId classId, ObjectId objectId, PropertyId propertyId)
{
  SK_CONSTR(kv, classId, objectId, propertyId);
  ::lmdb::val k{kv, sizeof(kv)};
  ::lmdb::val v{};
  if(::lmdb::dbi_get(m_txn, m_dbi.handle(), k, v))
    buf.start(v.data<byte_t>(), v.size());
}

bool Transaction::remove(ClassId classId, ObjectId objectId, PropertyId propertyId)
{
  SK_CONSTR(kv, classId, objectId, propertyId);
  ::lmdb::val k{kv, sizeof(kv)};
  ::lmdb::val v{};
  return ::lmdb::dbi_del(m_txn, m_dbi.handle(), k, v);
}

ChunkCursor::Ptr Transaction::_openChunkCursor(ClassId classId, ObjectId objectId, bool atEnd)
{
  return ChunkCursor::Ptr(new ChunkCursorImpl(m_txn, m_dbi, classId, objectId, atEnd));
}

bool Transaction::getNextChunkInfo(ObjectId collectionId, PropertyId *chunkId, size_t *startIndex)
{
  ::lmdb::val data;
  PropertyId last;

  bool ok = lastChunk(collectionId, last, data);

  if(ok) {
    *chunkId = last + PropertyId(1); //next higher chunkid
    size_t start, count;
    readChunkHeader(data.data<byte_t>(), 0, &start, &count);
    *startIndex = start + count; //next higher element index
    return true;
  }
  return false;
}

bool Transaction::lastChunk(ObjectId collectionId, PropertyId &chunkId, ::lmdb::val &data)
{
  SK_CONSTR(k, COLLECTION_CLSID, collectionId, 0xFFFF);
  ::lmdb::val key {k, sizeof(k)};

  auto cursor = ::lmdb::cursor::open(m_txn, m_dbi);

  bool ok;
  if(cursor.get(key, nullptr, MDB_SET_RANGE))
    ok = cursor.get(key, data, MDB_PREV);
  else
    ok = cursor.get(key, data, MDB_LAST);

  if(ok && SK_CLASSID(key.data<byte_t>()) == COLLECTION_CLSID && SK_OBJID(key.data<byte_t>()) == collectionId) {
    chunkId = SK_PROPID(key.data<byte_t>());
    return true;
  }
  return false;
}

bool check_chunkinfo(const ChunkInfo &run, const ChunkInfo &ref)
{
  return run.startIndex + run.elementCount < ref.startIndex;
}

bool Transaction::_getCollectionData(CollectionInfo *info, size_t startIndex, size_t length,
                                     size_t elementSize, void **data, bool *owned)
{
  ChunkInfo chunk(0, startIndex);
  auto findStart = lower_bound(info->chunkInfos.cbegin(), info->chunkInfos.cend(), chunk, check_chunkinfo);
  if(findStart != info->chunkInfos.cend()) {
    chunk.startIndex += length;
    auto findEnd = lower_bound(info->chunkInfos.cbegin(), info->chunkInfos.cend(), chunk, check_chunkinfo);
    if(findEnd != info->chunkInfos.cend()) {
      ::lmdb::val keyval, startval, endval;

      SK_CONSTR(k, COLLECTION_CLSID, info->collectionId, findStart->chunkId);
      keyval.assign(k, sizeof(k));
      if(!m_dbi.get(m_txn, keyval, startval)) return false;

      byte_t *datastart = startval.data<byte_t>() + ChunkHeader_sz;
      size_t offs = startIndex - findStart->startIndex;
      datastart += offs * elementSize;

      if(findStart == findEnd) {
        //all data in same chunk, Cool, we're done
        *data = datastart;
        *owned = false;
      }
      else {
        //data crosses chunks. Too bad, need to copy
        size_t startlen = findStart->dataSize - (datastart - startval.data<byte_t>());
        size_t datalen = startlen;
        for(auto fs=findStart+1; fs != findEnd; fs++)
          datalen += fs->dataSize - ChunkHeader_sz;

        SK_CONSTR(k, COLLECTION_CLSID, info->collectionId, findEnd->chunkId);
        keyval.assign(k, sizeof(k));
        if(!m_dbi.get(m_txn, keyval, endval)) return false;

        size_t endlen=0, endcount = startIndex + length - findEnd->startIndex;
        endlen = endcount * elementSize;
        datalen += endlen;

        *data = malloc(datalen);
        *owned = true;

        char *dta = (char *)*data;
        memcpy(dta, datastart, startlen);
        dta += startlen;

        for(auto fs=findStart+1; fs != findEnd; fs++) {
          SK_CONSTR(k, COLLECTION_CLSID, info->collectionId, fs->chunkId);
          keyval.assign(k, sizeof(k));
          ::lmdb::val dataval;
          if(!m_dbi.get(m_txn, keyval, dataval)) return false;

          memcpy(dta, dataval.data<byte_t>()+ChunkHeader_sz, fs->dataSize-ChunkHeader_sz);
          dta += fs->dataSize-ChunkHeader_sz;
        }
        memcpy(dta, endval.data()+ChunkHeader_sz, endlen);
      }
      return true;
    }
  }
  return false;
}

PropertyId Transaction::getMaxPropertyId(ClassId classId, ObjectId objectId)
{
  SK_CONSTR(k, classId, objectId, 0xFFFF);
  ::lmdb::val key {k, sizeof(k)};

  auto cursor = ::lmdb::cursor::open(m_txn, m_dbi);

  bool ok;
  if(cursor.get(key, nullptr, MDB_SET_RANGE))
    ok = cursor.get(key, nullptr, MDB_PREV);
  else
    ok = cursor.get(key, nullptr, MDB_LAST);

  if(ok && SK_CLASSID(key.data<byte_t>()) == classId) {
    return SK_PROPID(key.data<byte_t>());
  }
  return PropertyId(0);
}

ClassCursorHelper * Transaction::_openCursor(ClassId classId)
{
  return new ClassCursorHelper(m_txn, m_dbi, classId);
}

VectorCursorHelper * Transaction::_openCursor(ClassId classId, ObjectId objectId, PropertyId propertyId)
{
  return new VectorCursorHelper(m_txn, m_dbi, classId, objectId, propertyId);
}

CollectionCursorHelper * Transaction::_openCursor(ClassId classId, ObjectId collectionId)
{
  return new CollectionCursorHelper(m_txn, m_dbi, classId, collectionId);
}

ObjectId KeyValueStoreImpl::findMaxObjectId(::lmdb::txn &txn, ClassId classId)
{
  ObjectId maxId = 0;

  auto cursor = ::lmdb::cursor::open(txn, m_dbi_data);

  //first try to position on next class and go one back
  SK_CONSTR(k, classId+1, 0, 0);
  ::lmdb::val key {k, sizeof(k)};

  if(cursor.get(key, MDB_SET_RANGE)) {
    if(cursor.get(key, MDB_PREV) && SK_CLASSID(key.data<byte_t>()) == classId) {
      maxId = SK_OBJID(key.data<byte_t>());
    }
  }
  else {
    //there's no next class
    if(cursor.get(key, MDB_LAST) && SK_CLASSID(key.data<byte_t>()) == classId) {
      maxId = SK_OBJID(key.data<byte_t>());
    }
  }
  cursor.close();

  return maxId;
}

KeyValueStoreBase::PropertyMetaInfoPtr KeyValueStoreImpl::make_metainfo(MDB_val *mdbVal)
{
  byte_t *readPtr = (byte_t *)mdbVal->mv_data;
  PropertyMetaInfoPtr mi(new PropertyMetaInfo());

  mi->id = read_integer<PropertyId>(readPtr, 2);
  readPtr += 2;
  mi->name = (const char *)readPtr;
  readPtr += mi->name.length() + 1;
  mi->typeId = read_integer<unsigned>(readPtr, 2);
  readPtr += 2;
  mi->isVector = read_integer<char>(readPtr, 1) != 0;
  readPtr += 1;
  mi->byteSize = read_integer<unsigned>(readPtr, 2);
  readPtr += 2;
  if(readPtr - (byte_t *)mdbVal->mv_data < mdbVal->mv_size)
    mi->className = (const char *)readPtr;

  return mi;
}

MDB_val KeyValueStoreImpl::make_val(unsigned id, const PropertyAccessBase *prop)
{
  size_t nameLen = strlen(prop->name) + 1;
  size_t size = nameLen + 7;
  size_t classLen = 0;
  if(prop->type.className) {
    classLen = strlen(prop->type.className) + 1;
    size += classLen;
  }

  MDB_val val;
  val.mv_size = size;
  val.mv_data = malloc(size);

  byte_t *writePtr = (byte_t *)val.mv_data;

  write_integer<unsigned>(writePtr, id, 2);
  writePtr += 2;
  memcpy(writePtr, prop->name, nameLen);
  writePtr += nameLen;
  write_integer<unsigned>(writePtr, prop->type.id, 2);
  writePtr += 2;
  write_integer<char>(writePtr, prop->type.isVector ? 1 : 0, 1);
  writePtr += 1;
  write_integer<unsigned>(writePtr, prop->type.byteSize, 2);
  writePtr += 2;
  if(classLen > 0)
    memcpy(writePtr, prop->type.className, classLen);

  return val;
}

void KeyValueStoreImpl::loadSaveClassMeta(
    AbstractClassInfo *classInfo,
    PropertyAccessBase * currentProps[],
    unsigned numProps,
    vector<PropertyMetaInfoPtr> &propertyInfos)
{
  using MetaInfo = KeyValueStoreBase::PropertyMetaInfoPtr;

  auto txn = ::lmdb::txn::begin(m_env, nullptr);

  ::lmdb::val key, val;
  key.assign(classInfo->name);
  auto cursor = ::lmdb::cursor::open(txn, m_dbi_meta);
  if(cursor.get(key, val, MDB_SET)) {
    //class already exists
    bool first = true;
    for (bool read = cursor.get(key, val, MDB_FIRST_DUP); read; read = cursor.get(key, val, MDB_NEXT_DUP)) {
      if(first) {
        //first record is [propertyId == 0, classId]
        classInfo->classId = read_integer<ClassId>(val.data<byte_t>()+2, 2);
        first = false;
      }
      else //rest is properties
        propertyInfos.push_back(make_metainfo((MDB_val *)val));
    }
    cursor.close();

    classInfo->maxObjectId = findMaxObjectId(txn, classInfo->classId);

    txn.abort();
  }
  else {
    //class appears for the first time
    cursor.close();

    classInfo->classId = ++m_maxClassId;

    //save the first record [0, classId]
    byte_t buf[4];
    write_integer<PropertyId>(buf, 0, 2);
    write_integer<ClassId>(buf+2, classInfo->classId, 2);
    key.assign(classInfo->name);
    val.assign(buf, 4);
    m_dbi_meta.put(txn, key, val);

    //Save properties
    for(unsigned i=0; i < numProps; i++) {
      const PropertyAccessBase *prop = currentProps[i];
      MDB_val val = make_val(i+1, prop);
      ::lmdb::dbi_put(txn, m_dbi_meta.handle(), (MDB_val *)key, &val, 0);
      free(val.mv_data);
    }
    txn.commit();

    classInfo->maxObjectId = 0;
  }
}

} //lmdb
} //persistence
} //flexis

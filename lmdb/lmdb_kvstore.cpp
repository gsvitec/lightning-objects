//
// Created by chris on 10/7/15.
//

#include "lmdb_kvstore.h"
#include "liblmdb/lmdb++.h"
#include <algorithm>
#include <cstdio>

namespace flexis {
namespace persistence {
namespace lmdb {

using namespace std;

static const char * CLASSDATA = "classdata";
static const char * CLASSMETA = "classmeta";

namespace ps = flexis::persistence;

static const unsigned ObjectId_off = ClassId_sz;
static const unsigned PropertyId_off = ClassId_sz + ObjectId_sz;

#define SK_CONSTR(nm, c, o, p) char nm[StorageKey::byteSize]; *(ClassId *)nm = c; *(ObjectId *)(nm+ObjectId_off) = o; \
*(PropertyId *)(nm+PropertyId_off) = p

#define SK_RET(nm, k) nm.classId = *(ClassId *)k; nm.objectId = *(ObjectId *)(k+ObjectId_off); \
nm.propertyId = *(PropertyId *)(k+PropertyId_off)

#define SK_CLASSID(k) *(ClassId *)k
#define SK_OBJID(k) *(ObjectId *)(k+ObjectId_off)
#define SK_PROPID(k) *(PropertyId *)(k+PropertyId_off)

int key_compare(const MDB_val *a, const MDB_val *b)
{
  char *k1 = (char *)a->mv_data;
  char *k2 = (char *)b->mv_data;

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
      return SK_CLASSID(m_keyval.data()) == m_classId;
    }
    return false;
  }

  ObjectId key() override
  {
    m_cursor.get(m_keyval, MDB_GET_CURRENT);
    return SK_OBJID(m_keyval.data());
  }

  bool next() override
  {
    while(m_cursor.get(m_keyval, MDB_NEXT)) {
      if(SK_CLASSID(m_keyval.data()) != m_classId) {
        //not anymore the requested class
        return false;
      }
      else if(SK_PROPID(m_keyval.data()) == 0) {
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
      SK_RET(key, m_keyval.data());
      rb.start(dataval.data(), dataval.size());
    }
  }

  const char *getObjectData() override {
    ::lmdb::val dataval{};
    if(m_cursor.get(m_keyval, dataval, MDB_GET_CURRENT)) {
      return dataval.data();
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
  ChunkCursorImpl(::lmdb::txn &txn, ::lmdb::dbi &dbi, ClassId classId, ObjectId objectId, bool atEnd=false)
      : m_txn(txn), m_dbi(dbi), m_cursor(::lmdb::cursor::open(txn, dbi)), m_classId(classId), m_objectId(objectId)
  {
    if(atEnd) {
      SK_CONSTR(k, classId, objectId, 0xFFFF);
      keyval.assign(k, sizeof(k));

      auto cursor = ::lmdb::cursor::open(m_txn, m_dbi);
      m_atEnd = !(cursor.get(keyval, nullptr, MDB_SET_RANGE)
                && cursor.get(keyval, dataval, MDB_PREV) && SK_CLASSID(keyval.data()) == classId);
    }
    else {
      SK_CONSTR(k, classId, objectId, 1);
      keyval.assign(k, sizeof(k));

      m_atEnd = !m_cursor.get(keyval, dataval, MDB_SET);
    }
  }

  PropertyId chunkId() override {
    return SK_PROPID(keyval.data());
  }

  bool next() override {
    m_atEnd = !m_cursor.get(keyval, dataval, MDB_NEXT);
    if(!m_atEnd)
      m_atEnd = SK_CLASSID(keyval.data()) != m_classId || SK_OBJID(keyval.data()) != m_objectId;
    return !m_atEnd;
  }

  void get(ReadBuf &rb) override {
    rb.start(dataval.data(), dataval.size());
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
  const char *m_data = nullptr;
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
      rb.start(dataval.data(), dataval.size());
  }

  const char *getObjectData()
  {
    ::lmdb::val keyval {m_data, StorageKey::byteSize};
    ::lmdb::val dataval;

    return m_dbi.get(m_txn, keyval, dataval) ? dataval.data() : nullptr;
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
    return SK_OBJID(m_vectordata + m_index * StorageKey::byteSize);
  }

  bool next() override
  {
    return ++m_index < m_size;
  }

  void erase() override
  {
    const char *keydata = m_vectordata.data() + m_index * StorageKey::byteSize;
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
      const char *keydata = m_vectordata.data() + m_index * StorageKey::byteSize;
      ::lmdb::val keyval;
      keyval.assign(keydata, StorageKey::byteSize);
      ::lmdb::val dataval;

      if(m_dbi.get(m_txn, keyval, dataval)) {
        SK_RET(key, keydata);
        rb.start(dataval.data(), dataval.size());
      }
      else {
        throw new persistence_error("corrupted vector: item not found");
      }
    }
  }

  const char *getObjectData() override {
    if(m_index < m_size) {
      const char *keydata = m_vectordata.data() + m_index * StorageKey::byteSize;
      ::lmdb::val keyval;
      keyval.assign(keydata, StorageKey::byteSize);
      ::lmdb::val dataval;

      if(m_dbi.get(m_txn, keyval, dataval)) {
        return dataval.data();
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
class FlexisPersistence_EXPORT Transaction : public flexis::persistence::kv::WriteTransaction
{
  const ::lmdb::env &env;

  ::lmdb::txn m_txn;
  ::lmdb::dbi m_dbi;

protected:
  bool putData(ClassId classId, ObjectId objectId, PropertyId propertyId, WriteBuf &buf) override;
  void getData(ReadBuf &buf, ClassId classId, ObjectId objectId, PropertyId propertyId) override;
  bool remove(ClassId classId, ObjectId objectId, PropertyId propertyid) override;

  ClassCursorHelper * _openCursor(ClassId classId) override;
  CollectionCursorHelper * _openCursor(ClassId classId, ObjectId collectionId) override;
  VectorCursorHelper * _openCursor(ClassId classId, ObjectId objectId, PropertyId propertyId) override;

  PropertyId getMaxPropertyId(ClassId classId, ObjectId objectId) override;
  ChunkCursor::Ptr _openChunkCursor(ClassId classId, ObjectId objectId, bool atEnd) override;

public:
  enum class Mode {read, append, write};

  Transaction(ps::KeyValueStore &store, Mode mode, ::lmdb::env &env)
      : flexis::persistence::kv::WriteTransaction(store, mode == Mode::append),
        env(env),
        m_txn(::lmdb::txn::begin(env, nullptr, mode == Mode::read ? MDB_RDONLY : 0)),
        m_dbi(::lmdb::dbi::open(m_txn, CLASSDATA))
  {
    //important!!
    m_dbi.set_compare(m_txn, key_compare);
  }

  void commit() override
  {
    m_txn.commit();
  }

  void abort() override
  {
    m_txn.abort();
  }
};

/**
 * LMDB-based KeyValueStore implementation
 */
class KeyValueStoreImpl : public KeyValueStore
{
  ::lmdb::env m_env;
  weak_ptr<Transaction> writeTxn;
  string m_dbpath;

  static int meta_dup_compare(const MDB_val *a, const MDB_val *b);

  PropertyMetaInfoPtr make_metainfo(MDB_val *mdbVal);
  MDB_val make_val(unsigned id, const PropertyAccessBase *prop);
  ObjectId findMaxObjectId(ClassId classId);

protected:
  void loadSaveClassMeta(
      ClassInfo &classInfo,
      PropertyAccessBase * currentProps[],
      unsigned numProps,
      std::vector<PropertyMetaInfoPtr> &propertyInfos) override;

public:
  KeyValueStoreImpl(std::string location, std::string name);
  ~KeyValueStoreImpl();

  ps::TransactionPtr beginRead() override;
  ps::WriteTransactionPtr beginWrite(bool append) override;
};

//Start KeyValueStoreImpl implementation

KeyValueStore::Factory::operator flexis::persistence::KeyValueStore *() const
{
  return new KeyValueStoreImpl(location, name);
}

KeyValueStoreImpl::KeyValueStoreImpl(string location, string name) : m_env(::lmdb::env::create())
{
  m_dbpath = location + "/"+ (name.empty() ? "kvdata" : name);

  size_t sz = size_t(1) * size_t(1024) * size_t(1024) * size_t(1024); //1 GiB
  m_env.set_mapsize(sz);
  m_env.set_max_dbs(2);
  m_env.open(m_dbpath.c_str(), MDB_NOLOCK | MDB_NOSUBDIR, 0664);

  m_maxCollectionId = findMaxObjectId(COLLECTION_CLSID);
}

KeyValueStoreImpl::~KeyValueStoreImpl()
{
#ifdef _WIN32
  std::replace(m_dbpath.begin(), m_dbpath.end(), '/', '\\');
  string tmp = m_dbpath + "_tmp";
  mdb_env_copy2(m_env.handle(), tmp.c_str(), MDB_CP_COMPACT);
#endif

  m_env.close();

#ifdef _WIN32
  std::remove(m_dbpath.c_str());
  std::rename(tmp.c_str(), m_dbpath.c_str());
#endif
}

ps::TransactionPtr KeyValueStoreImpl::beginRead()
{
  return ps::TransactionPtr(new Transaction(*this, Transaction::Mode::read, m_env));
}

ps::WriteTransactionPtr KeyValueStoreImpl::beginWrite(bool append)
{
  if(writeTxn.lock()) throw invalid_argument("a write transaction is already running");

  Transaction::Mode mode = append ? Transaction::Mode::append : Transaction::Mode::write;
  auto tptr = shared_ptr<Transaction>(new Transaction(*this, mode, m_env));
  writeTxn = tptr;

  return tptr;
}

bool Transaction::putData(ClassId classId, ObjectId objectId, PropertyId propertyId, WriteBuf &buf)
{
  SK_CONSTR(kv, classId, objectId, propertyId);
  ::lmdb::val k{kv, sizeof(kv)};
  ::lmdb::val v{buf.data(), buf.size()};

  return ::lmdb::dbi_put(m_txn, m_dbi.handle(), k, v, m_append ? MDB_APPEND : 0);
}

void Transaction::getData(ReadBuf &buf, ClassId classId, ObjectId objectId, PropertyId propertyId)
{
  SK_CONSTR(kv, classId, objectId, propertyId);
  ::lmdb::val k{kv, sizeof(kv)};
  ::lmdb::val v{};
  if(::lmdb::dbi_get(m_txn, m_dbi.handle(), k, v))
    buf.start(v.data(), v.size());
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

PropertyId Transaction::getMaxPropertyId(ClassId classId, ObjectId objectId)
{
  SK_CONSTR(k, classId, objectId, 0xFFFF);
  ::lmdb::val key {k, sizeof(k)};

  auto cursor = ::lmdb::cursor::open(m_txn, m_dbi);
  if(cursor.get(key, nullptr, MDB_SET_RANGE) && cursor.get(key, nullptr, MDB_PREV) && SK_CLASSID(key.data()) == classId) {
    return SK_PROPID(key.data());
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

ObjectId KeyValueStoreImpl::findMaxObjectId(ClassId classId)
{
  auto txn = ::lmdb::txn::begin(m_env, nullptr);

  //create the classdata database if needed
  auto dbi = ::lmdb::dbi::open(txn, CLASSDATA, MDB_CREATE);
  dbi.set_compare(txn, key_compare); //important!!

  ObjectId maxId = 0;

  auto cursor = ::lmdb::cursor::open(txn, dbi);

  //first try to position on next class and go one back
  SK_CONSTR(k, classId+1, 0, 0);
  ::lmdb::val key {k, sizeof(k)};

  if(cursor.get(key, MDB_SET_RANGE)) {
    if(cursor.get(key, MDB_PREV) && SK_CLASSID(key.data()) == classId) {
      maxId = SK_OBJID(key.data());
    }
  }
  else {
    //there's no next class
    if(cursor.get(key, MDB_LAST) && SK_CLASSID(key.data()) == classId) {
      maxId = SK_OBJID(key.data());
    }
  }
  cursor.close();
  txn.commit();

  return maxId;
}

int KeyValueStoreImpl::meta_dup_compare(const MDB_val *a, const MDB_val *b)
{
  PropertyId id1 = read_integer<PropertyId>((char *)a->mv_data, 2);
  PropertyId id2 = read_integer<PropertyId>((char *)b->mv_data, 2);

  return id1 - id2;
}

KeyValueStoreBase::PropertyMetaInfoPtr KeyValueStoreImpl::make_metainfo(MDB_val *mdbVal)
{
  char *readPtr = (char *)mdbVal->mv_data;
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
  if(readPtr - (char *)mdbVal->mv_data < mdbVal->mv_size)
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

  char *writePtr = (char *)val.mv_data;

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
    ClassInfo &classInfo,
    PropertyAccessBase * currentProps[],
    unsigned numProps,
    vector<PropertyMetaInfoPtr> &propertyInfos)
{
  using MetaInfo = KeyValueStoreBase::PropertyMetaInfoPtr;

  auto txn = ::lmdb::txn::begin(m_env, nullptr);
  auto dbi = ::lmdb::dbi::open(txn, CLASSMETA, MDB_DUPSORT | MDB_CREATE);

  dbi.set_dupsort(txn, meta_dup_compare);

  ::lmdb::val key, val;
  key.assign(classInfo.name);
  auto cursor = ::lmdb::cursor::open(txn, dbi);
  if(cursor.get(key, val, MDB_SET)) {
    bool first = true;
    for (bool read = cursor.get(key, val, MDB_FIRST_DUP); read; read = cursor.get(key, val, MDB_NEXT_DUP)) {
      if(first) {
        //class already known. First record is [propertyId == 0, classId]
        classInfo.classId = read_integer<ClassId>(val.data()+2, 2);
        first = false;
      }
      else //rest is properties
        propertyInfos.push_back(make_metainfo((MDB_val *)val));
    }
    cursor.close();
    txn.abort();
  }
  else {
    cursor.close();

    //find the maximum classId + 1
    key.assign((char *)0, 0);
    val.assign((char *)0, 0);
    cursor = ::lmdb::cursor::open(txn, dbi);
    while (cursor.get(key, val, MDB_NEXT_NODUP)) {
      ClassId cid = read_integer<ClassId>(val.data()+2, 2);
      if(cid > classInfo.classId) classInfo.classId = cid;
    }
    cursor.close();
    classInfo.classId++;

    //save the first record [0, classId]
    char buf[4];
    write_integer<PropertyId>(buf, 0, 2);
    write_integer<ClassId>(buf+2, classInfo.classId, 2);
    key.assign(classInfo.name);
    val.assign(buf, 4);
    dbi.put(txn, key, val);

    //Save properties
    for(unsigned i=0; i < numProps; i++) {
      const PropertyAccessBase *prop = currentProps[i];
      MDB_val val = make_val(i+1, prop);
      ::lmdb::dbi_put(txn, dbi.handle(), (MDB_val *)key, &val, 0);
      free(val.mv_data);
    }
    txn.commit();
  }
  classInfo.maxObjectId = findMaxObjectId(classInfo.classId);
}

} //lmdb
} //persistence
} //flexis

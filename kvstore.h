//
// Created by chris on 10/7/15.
//

#ifndef FLEXIS_FLEXIS_KVSTORE_H
#define FLEXIS_FLEXIS_KVSTORE_H

#include <string>
#include <typeinfo>
#include <typeindex>
#include <memory>
#include <functional>
#include <set>
#include <unordered_map>
#include <type_traits>

#include "kvtraits.h"

#define PROPERTY_ID(__cls, __name) flexis::persistence::kv::ClassTraits<__cls>::__name->id
#define PROPERTY(__cls, __name) flexis::persistence::kv::ClassTraits<__cls>::__name

#define IS_SAME(cls, var, prop, other) flexis::persistence::kv::ClassTraits<cls>::same(\
  var, flexis::persistence::kv::ClassTraits<cls>::PropertyIds::prop, other)

namespace flexis {
namespace persistence {

/* 2 helper templates for checking for the presence of the "static const bool traits_has_objid"
 * ClassTraits member variable. The variable is always declared, but only defined by the OBJECT_ID
 * macro. The true_type specialization will only be instantiated in that case. Used in static_assert
 * to make sure any use of value-based mappings has an accompanying OBJECT_ID mapping
 */
template<typename T, typename V = bool>
struct has_objid : std::false_type
{};

template<typename T>
struct has_objid<T,
    typename std::enable_if<T::traits_has_objid, bool>::type> : std::true_type
{};

using namespace kv;

static const ClassId COLLECTION_CLSID = 1;
static const ClassId COLLINFO_CLSID = 2;
static const size_t DEFAULT_CHUNKSIZE = 1024 * 2; //default chunksize. All data in one page

class incompatible_schema_error : public persistence_error
{
public:
  struct Property {
    std::string name;
    unsigned position;
    std::string description;
    std::string runtime, saved;

    Property(std::string nm, unsigned pos) : name(nm), position(pos) {}
  };
  std::vector<Property> properties;

  incompatible_schema_error(const char *className, std::vector<Property> errs)
      : persistence_error(make_what(className), make_detail(errs)), properties(errs) {}

private:
  static std::string make_what(const char *cls);
  static std::string make_detail(std::vector<Property> &errs);
};

class class_not_registered_error : public persistence_error
{
public:
  class_not_registered_error(const std::string className)
      : persistence_error("class has not been registered", className) {}
};

namespace kv {
class ReadTransaction;
class ExclusiveReadTransaction;
class WriteTransaction;
}

class KeyValueStoreBase
{
  friend class kv::ReadTransaction;
  friend class kv::WriteTransaction;

protected:
  virtual ~KeyValueStoreBase() {}

  //property info stored in database
  struct PropertyMetaInfo {
    std::string name;
    PropertyId id;
    unsigned typeId;
    bool isVector;
    unsigned byteSize;
    std::string className;
    StoreLayout storeLayout;
  };
  using PropertyMetaInfoPtr = std::shared_ptr<PropertyMetaInfo>;

  /**
   * check if class schema already exists. If so, check compatibility. If not, create
   * @param classInfo the runtime classInfo to check
   * @param properties the runtime class properties
   * @param numProperties size of the former
   * @errors (out) compatibility errors detected during check
   * @return true if the class already existed
   */
  bool updateClassSchema(AbstractClassInfo *classInfo, const PropertyAccessBase ** properties[], unsigned numProperties,
                         std::vector<incompatible_schema_error::Property> &errors);

  /**
   * load class metadata from the store. If it doesn't already exist, save currentProps as metadata
   *
   * @param (in/out) the ClassInfo which holds the fully qualified class name. The other fields will
   * be set
   * @param (in) currentProps the currently live persistent properties
   * @param (in) numProps the length of the above array
   * @param (out) the persistent propertyInfos. This will be empty if the class was newly declared
   */
  virtual void loadSaveClassMeta(
      AbstractClassInfo *classInfo,
      const PropertyAccessBase ** currentProps[],
      unsigned numProps,
      std::vector<PropertyMetaInfoPtr> &propertyInfos) = 0;
};

using ReadTransactionPtr = std::shared_ptr<kv::ReadTransaction>;
using ExclusiveReadTransactionPtr = std::shared_ptr<kv::ExclusiveReadTransaction>;
using WriteTransactionPtr = std::shared_ptr<kv::WriteTransaction>;

using ObjectProperties = std::unordered_map<ClassId, Properties *>;
using ObjectClassInfos = std::unordered_map<ClassId, AbstractClassInfo *>;

using TypeInfoRef = std::reference_wrapper<const std::type_info>;

struct TypeinfoHasher {
  std::size_t operator()(TypeInfoRef code) const
  {
    return code.get().hash_code();
  }
};

struct TypeinfoEqualTo {
  bool operator()(TypeInfoRef lhs, TypeInfoRef rhs) const
  {
    return lhs.get() == rhs.get();
  }
};

namespace put_schema {
/*
 * helper structs for processing the variadic template list which is passed to KeyValueStore#putSchema
 */

struct validate_info {
  AbstractClassInfo * const classInfo;
  const PropertyAccessBase *** const decl_props;
  Properties * const properties;
  const unsigned num_decl_props;

  validate_info(AbstractClassInfo *classInfo, Properties *properties, const PropertyAccessBase ** decl_props[], unsigned num_decl_props)
      : classInfo(classInfo), properties(properties), decl_props(decl_props), num_decl_props(num_decl_props) {}
};

//primary template
template<typename... Sargs>
struct register_type;

//worker template
template<typename S>
struct register_type<S>
{
  using Traits = ClassTraits<S>;

  static void addTypes(std::vector<validate_info> &vinfos)
  {
    //collect infos
    vinfos.push_back(
        validate_info(Traits::traits_info, Traits::traits_properties, Traits::decl_props, Traits::num_decl_props));

    Traits::init();
  }
};

//helper for working the variadic temnplate list
template<typename S, typename... Sargs>
struct register_helper
{
  static void addTypes(std::vector<validate_info> &vinfos) {
    register_type<S>().addTypes(vinfos);
    register_type<Sargs...>().addTypes(vinfos);
  }
};

//secondary template
template<typename... Sargs>
struct register_type {
  static void addTypes(std::vector<validate_info> &vinfos) {
    register_helper<Sargs...>().addTypes(vinfos);
  }
};

} //put_schema

/**
 * high-performance key/value store interface. Most application-relevant functions are provided by ReadTransaction
 * and WriteTransaction, which can be obtined from this class
 */
class KeyValueStore : public KeyValueStoreBase
{
  friend class kv::ReadTransaction;
  friend class kv::ExclusiveReadTransaction;
  friend class kv::WriteTransaction;

  ClassId minAbstractClassId = UINT32_MAX;

  //backward mapping from ClassId, used during polymorphic operations
  ObjectProperties objectProperties;
  ObjectClassInfos objectClassInfos;

  std::unordered_map<TypeInfoRef, ClassId, TypeinfoHasher, TypeinfoEqualTo> objectTypeInfos;

protected:
  ClassId m_maxClassId = AbstractClassInfo::MIN_USER_CLSID;
  ObjectId m_maxCollectionId = 0;

public:

  /**
   * register and validate the class schema for this store
   * @param throwIfIncompatible if true (default), throw an incompatible_schema_error if any schema incompatibility
   * was detected
   */
  template <typename... Cls>
  void putSchema(bool throwIfIncompatible=true)
  {
    std::vector<put_schema::validate_info> vinfos;
    put_schema::register_type<Cls...>::addTypes(vinfos);

    //first process individual classes
    for(auto &info : vinfos) {
      std::vector<incompatible_schema_error::Property> errs;
      updateClassSchema(info.classInfo, info.decl_props, info.num_decl_props, errs);
      if(throwIfIncompatible && !errs.empty()) {
        throw incompatible_schema_error(info.classInfo->name, errs);
      }

      //make sure all propertyaccessors have correct classId
      for(int i=0; i<info.num_decl_props; i++)
        const_cast<PropertyAccessBase *>(*info.decl_props[i])->classId = info.classInfo->classId;

      //initialize lookup maps
      objectProperties[info.classInfo->classId] = info.properties;
      objectClassInfos[info.classInfo->classId] = info.classInfo;
      objectTypeInfos[info.classInfo->typeinfo] = info.classInfo->classId;
    }
  }

  /**
   * set refcounting state for the given template aparameter class. When refcounting is on, a separate entry will
   * be written for each object which holds the reference count. The reference count will be increased whenever
   * the object is added to a shared_ptr-mapped container.
   */
  template <typename T> void setRefCounting(bool refcount=true)
  {
    ClassTraits<T>::traits_info->setRefCounting(refcount);
    ClassId cid = ClassTraits<T>::traits_info->classId;
    for(auto &op : objectProperties) {
      if(op.first != cid && op.second->preparesUpdates(cid)) {
        if(refcount)
          objectClassInfos[op.first]->prepareClasses.insert(cid);
        else
          objectClassInfos[op.first]->prepareClasses.erase(cid);
        break;
      }
    }
  }

  /**
   * register a substitute type to be used in polymorphic operations where a subclass of T is unknown.
   */
  template <typename T, typename Subst>
  void registerSubstitute()
  {
    ClassTraits<T>::traits_info-> template setSubstitute<Subst>();
  }

  template <typename T> bool isNew(std::shared_ptr<T> &obj)
  {
    return ClassTraits<T>::getObjectKey(obj)->isNew();
  }

  /**
   * @return a transaction object that allows reading the database.
   */
  virtual ReadTransactionPtr beginRead() = 0;

  /**
   * @return a transaction object that allows reading the database but prevents writing
   */
  virtual ExclusiveReadTransactionPtr beginExclusiveRead() = 0;

  /**
   * @param append enable append mode. Append mode, if supported, is useful if a large number of homogenous simple objects
   * are written, homogenous meaning that objects are of the same type (or subtypes). One essential requirement is that keys
   * are written in sequential order (hence append), which is maintained if only the putObject API is used. However, this
   * cannot be ensured if the objects written are complex, i.e. contain references to other mapped objects (either direct or
   * as array elements), which is why these objects are not allowed.
   * Writing in append mode can be much more efficient than standard write
   *
   * @param needsKBs database space required by this transaction. If not set, the default will be used.
   *
   * @return a transaction object that allows reading + writing the database.
   * @throws InvalidArgumentException if in append mode the above prerequisites are not met
   * @throws persistence_error if write operations are currently blocked (beginRead(true))
   */
  virtual WriteTransactionPtr beginWrite(bool append=false, unsigned needsKBs=0) = 0;
};

namespace kv {

void readChunkHeader(ReadBuf &buf, size_t *dataSize, size_t *startIndex, size_t *elementCount);
void readObjectHeader(ReadBuf &buf, ClassId *classId, ObjectId *objectId, size_t *size=nullptr, bool *deleted=nullptr);

template <typename T>
bool all_predicate(std::shared_ptr<T> t=nullptr) {return true;}

/**
 * read object data polymorphically
 */
template<typename T> void readObject(ReadTransaction *tr, ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj)
{
  Properties *props = ClassTraits<T>::getProperties(classId);
  if(!props) throw persistence_error("unknown classId. Class not registered");

  for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
    const PropertyAccessBase *p = props->get(px);
    if(!p->enabled) continue;

    ClassTraits<T>::load(tr, buf, classId, objectId, obj, p);
  }
}

/**
 * read object data non-polymorphically
 */
template<typename T> void readObject(ReadTransaction *tr, ReadBuf &buf, T &obj,
                                     ClassId classId, ObjectId objectId, StoreMode mode = StoreMode::force_none)
{
  Properties *props = ClassTraits<T>::traits_properties;

  for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
    const PropertyAccessBase *p = props->get(px);
    if(!p->enabled) continue;

    ClassTraits<T>::load(tr, buf, classId, objectId, &obj, p, mode);
  }
}

/**
 * class that must be subclassed by CollectionIterProperty replacements
 */
class KVPropertyBackend
{
protected:
  ObjectId m_objectId = 0;
  KeyValueStore *m_store = nullptr;

public:
  template <typename O>
  static void assign(KeyValueStore &store, O &o, const PropertyAccessBase *pa, ObjectId objectId)
  {
    void * ib = ClassTraits<O>::initMember(&o, pa);
    if(!ib) throw persistence_error(std::string("property ")+pa->name+" is not a collection member");

    //bad luck if pa->storage was not an CollectionIterPropertyStorage
    ((KVPropertyBackend *)ib)->setObjectId(objectId);
    ((KVPropertyBackend *)ib)->setKVStore(&store);
  }

  ObjectId getObjectId() {return m_objectId;}
  void setObjectId(ObjectId objectId) { m_objectId = objectId;}
  void setKVStore(KeyValueStore *store) {m_store = store;}
};
using IterPropertyBackendPtr = std::shared_ptr<KVPropertyBackend>;

/**
 * buffer for reading raw object data
 */
class ObjectBuf
{
protected:
  const bool makeCopy = false;
  bool dataChecked = false;
  size_t markOffs = 0;
  ReadBuf readBuf;
  virtual void checkData() {}

  void checkData(ReadTransaction *tr, ClassId cid, ObjectId oid);

public:
  ObjectKey key;

  ObjectBuf(bool makeCopy) : makeCopy(makeCopy) {}
  ObjectBuf(byte_t *data, size_t size) {
    dataChecked = true;
    readBuf.start(data, size);
  }
  ObjectBuf(ObjectKey key, bool makeCopy) : key(key), makeCopy(makeCopy) {}
  ObjectBuf(ReadTransaction *tr, ClassId classId, ObjectId objectId, bool makeCopy)
      : key({classId, objectId}), makeCopy(makeCopy) {
    checkData(tr, key.classId, key.objectId);
  }

  void start(byte_t *data, size_t size) {
    readBuf.start(data, size);
  }
  bool null() {
    return readBuf.null();
  }
  void reset() {
    readBuf.reset();
  }
  const byte_t *read(size_t sz=0) {
    checkData();
    return readBuf.read(sz);
  }
  template <typename T>
  T readInteger(unsigned sz) {
    checkData();
    return readBuf.readInteger<T>(sz);
  }
  void read(ClassId &cid, ObjectId &oid) {
    checkData();
    cid = readBuf.readRaw<ClassId>();
    oid = readBuf.readRaw<ObjectId>();
  }
  size_t strlen() {
    checkData();
    return readBuf.strlen();
  }

  void mark() {readBuf.mark();}

  void unmark(size_t offs) {
    if(readBuf.null())
      markOffs = offs;
    else
      readBuf.unmark(offs);
  }
};

/**
 * lazy variant of ObjectBuf, will read persistent data when required
 */
class LazyBuf : public ObjectBuf
{
  ReadTransaction * const m_txn;

protected:
  void checkData() override;

public:
  LazyBuf(ReadTransaction * txn, const ObjectKey &key, bool makeCopy) : ObjectBuf(key, makeCopy), m_txn(txn) {}
};

/**
 * Helper interface used by cursor, to be extended by implementors
 */
class CursorHelper {
  template <typename T> friend class ClassCursor;

protected:
  ClassId m_currentClassId = 0;
  ObjectId m_currentObjectId = 0;

  virtual ~CursorHelper() {}

  /**
   * position the cursor at the first object of the given class.
   * @return true if an object was found
   */
  virtual bool start() = 0;

  /**
   * position the cursor at the next object.
   * @return true if an object was found
   */
  virtual bool next() = 0;

  /**
   * delete the object at the current cursor position. Cursor is moved
   *
   * @return true if the cursor is not at end
   */
  virtual bool erase() = 0;

  /**
   * @return the objectId of the item at the current cursor position
   */
  ObjectId currentObjectId() {return m_currentObjectId;}

  /**
   * @return the classId of the item at the current cursor position
   */
  ClassId currentClassId() {return m_currentClassId;}

  /**
   * close the cursor an release all resources
   */
  virtual void close() = 0;

  /**
   * read the data at the current cursor position into the key and buffer
   */
  virtual void get(ObjectKey &key, ReadBuf &rb) = 0;

  /**
   * @return the data at the current cursor position
   */
  virtual void getObjectData(ObjectBuf &buf) = 0;
};

struct ChunkInfo {
  PropertyId chunkId = 0;
  size_t startIndex = 0;
  size_t elementCount = 0;
  size_t dataSize = 0;

  ChunkInfo() {}
  ChunkInfo(PropertyId chunkId, size_t startIndex, size_t elementCount=0, size_t dataSize=0)
      : chunkId(chunkId), startIndex(startIndex), elementCount(elementCount), dataSize(dataSize) {}
  ChunkInfo(PropertyId chunkId) : chunkId(chunkId) {}

  bool operator == (const ChunkInfo &other) {
    return chunkId == other.chunkId;
  }
  bool operator <= (const ChunkInfo &other) {
    return chunkId <= other.chunkId;
  }
};
struct CollectionInfo
{
  //unique collection id
  ObjectId collectionId = 0;

  //collection chunks
  std::vector <ChunkInfo> chunkInfos;

  PropertyId nextChunkId = 1;
  size_t nextStartIndex = 0;

  CollectionInfo() {}
  CollectionInfo(ObjectId collectionId) : collectionId(collectionId) {}
};

class ChunkCursor
{
protected:
  bool m_atEnd;

public:
  using Ptr = std::shared_ptr<ChunkCursor>;

  bool atEnd() const {return m_atEnd;}
  virtual bool next() = 0;
  virtual void get(ReadBuf &rb) = 0;
  virtual PropertyId chunkId() = 0;
  virtual void close() = 0;
};

/**
 * top-level chunked collection cursor base
 */
class CollectionCursorBase
{
protected:
  ChunkCursor::Ptr m_chunkCursor;
  ReadTransaction * const m_tr;
  const ObjectId m_collectionId;

  ReadBuf m_readBuf;
  size_t m_elementCount = 0, m_curElement = 0;
  virtual bool isValid() {return true;}
public:
  CollectionCursorBase(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr chunkCursor);
  bool atEnd();
  bool next();
};

/**
 * cursor for iterating over top-level object collections
 */
template <typename T>
class ObjectCollectionCursor : public CollectionCursorBase
{
  const ClassId m_declClass;

  ClassInfo<T> *m_curClassInfo = nullptr;

protected:
  bool isValid() override
  {
    if(!read_integer<byte_t>(m_readBuf.data() + ObjectHeader_sz - 1, 1)) { //check deleted flag
      ClassId cid = read_integer<ClassId>(m_readBuf.cur(), ClassId_sz);
      m_curClassInfo = FIND_CLS(T, cid);

      return m_curClassInfo != nullptr || ClassTraits<T>::traits_info->substitute != nullptr;
    }
    return false;
  }

public:
  using Ptr = std::shared_ptr<ObjectCollectionCursor<T>>;

  ObjectCollectionCursor(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr chunkCursor)
      : CollectionCursorBase(collectionId, tr, chunkCursor), m_declClass(ClassTraits<T>::traits_info->classId)
  {
    if(!isValid()) next();
  }

  T *get()
  {
    ClassId classId;
    ObjectId objectId;
    readObjectHeader(m_readBuf, &classId, &objectId);

    if(m_curClassInfo) {
      T *tp = m_curClassInfo->makeObject(classId);
      readObject<T>(m_tr, m_readBuf, classId, objectId, tp);
      return tp;
    }
    else {
      T *sp = ClassTraits<T>::getSubstitute();
      readObject<T>(m_tr, m_readBuf, *sp, classId, objectId);
      return sp;
    }
  }
};

/**
 * cursor for iterating over top-level object collections
 */
template <typename T>
class ValueCollectionCursor : public CollectionCursorBase
{
public:
  using Ptr = std::shared_ptr<ValueCollectionCursor<T>>;

  ValueCollectionCursor(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr chunkCursor)
      : CollectionCursorBase(collectionId, tr, chunkCursor)
  {}

  T get()
  {
    T val;
    ValueTraits<T>::getBytes(m_readBuf, val);
    return val;
  }
};

/**
 * cursor for iterating over class objects (each with its own key)
 */
template <typename T>
class ClassCursor
{
  ClassCursor(ClassCursor<T> &other) = delete;

  CursorHelper * const m_helper;
  ReadTransaction * const m_tr;
  bool m_hasData;
  ClassInfo<T> *m_classInfo;

  bool validateClass() {
    m_classInfo = FIND_CLS(T, m_helper->currentClassId());
    return m_classInfo != nullptr || ClassTraits<T>::traits_info->substitute != nullptr;
  }

public:
  using Ptr = std::shared_ptr<ClassCursor<T>>;

  ClassCursor(CursorHelper *helper, ReadTransaction *tr) : m_helper(helper), m_tr(tr)
  {
    bool hasData = helper->start();
    bool clsFound = validateClass();

    while(hasData && !clsFound) {
      hasData = helper->next();
      clsFound = hasData && validateClass();
    }
    m_hasData = hasData && clsFound;
    if(!m_hasData) close();
  }

  virtual ~ClassCursor() {
    delete m_helper;
  }

  /**
   * delete the object at the current cursor position. The cursor is moved to the next valid position or atEnd == true.
   * 
   * @true true if the cursor has not reached the end
   */
  bool erase(WriteTransactionPtr tr)
  {
    ObjectKey key;

    ReadBuf readBuf;
    m_helper->get(key, readBuf);
    if(readBuf.null()) return m_hasData;
    if(key.refcount > 1) throw persistence_error("removeObject: refcount > 1");

    using Traits = ClassTraits<T>;

    if(Traits::needsPrepare()) {
      Properties *props = Traits::getProperties(key.classId);
      ObjectBuf obuf(readBuf.data(), readBuf.size());

      for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
        const PropertyAccessBase *pa = props->get(px);

        if(!pa->enabled) continue;

        obuf.mark();
        size_t psz = ClassTraits<T>::prepareDelete(tr.get(), obuf, pa);
        obuf.unmark(psz);
      }
    }
    //now remove the object proper
    if(m_helper->erase()) {
      bool hasData=true, clsFound;
      do {
        clsFound = hasData && validateClass();
        hasData = m_helper->next();
      } while(hasData && !clsFound);

      m_hasData = hasData && clsFound;
      return true;
    }
    else m_hasData = false;

    if(!m_hasData) close();
    return m_hasData;
  }

  /**
   * retrieve the address of the value of the given object property at the current cursor position. Note that
   * the address may point to database-owned memory and therefore must not be written to. It may also become
   * invalid after the end of the transaction.
   *
   * @param pa the property mapping
   * @param objectBuf (out) the object data buffer. If the buffer is null(), data will be loaded from the database.
   * Otherwise, already-loaed data will be reused. When the function completes, and objectBuf is not null(),
   * read*() will return the address of the raw property data
   */
  void get(const PropertyAccessBase *pa, ObjectBuf &objectBuf)
  {
    using Traits = ClassTraits<T>;

    if(!pa->enabled || pa->type.isVector) {
      return;
    }

    //load / reset object buffer
    if(objectBuf.null())
      m_helper->getObjectData(objectBuf);
    else
      objectBuf.reset();

    //calculate the property offset
    for(unsigned i=0, sz=Traits::traits_properties->full_size(); i<sz; i++) {
      auto prop = Traits::traits_properties->get(i);
      if(prop == pa) {
        return;
      }
      objectBuf.mark();
      size_t psz = pa->storage->size(objectBuf);
      objectBuf.unmark(psz);
    }
  }

  /**
   * @param key (out) the key to be read into
   * @return the ready instantiated object at the current cursor position
   */
  T *get(ObjectKey &key)
  {
    //load the data buffer
    ReadBuf readBuf;
    m_helper->get(key, readBuf);

    //nothing here
    if(readBuf.null()) return nullptr;

    if(m_classInfo) {
      T *obj = m_classInfo->makeObject(key.classId);
      readObject<T>(m_tr, readBuf, key.classId, key.objectId, obj);
      return obj;
    }
    else {
      T *sp = ClassTraits<T>::getSubstitute();
      readObject<T>(m_tr, readBuf, *sp, key.classId, key.objectId);
      return sp;
    }
  }

  /**
   * @return the ready instantiated object at the current cursor position. The shared_ptr also
   * contains the ObjectId
   */
  std::shared_ptr<T> get()
  {
    ObjectId id;
    object_handler<T> handler;
    T *obj = get(handler);
    return std::shared_ptr<T>(obj, handler);
  }

  bool next() {
    bool hasData, clsFound;
    do {
      hasData = m_helper->next();
      clsFound = hasData && validateClass();
    } while(hasData && !clsFound);

    m_hasData = hasData && clsFound;

    if(!m_hasData) close();
    return m_hasData;
  }

  bool atEnd() {
    return !m_hasData;
  }

  void close() {
    m_helper->close();
  }
};

/**
 * container for a raw data pointer obtained from a top-level value collection
 */
template <typename V> class CollectionData
{
  V *m_data;
  bool m_owned;

public:
  using Ptr = std::shared_ptr<CollectionData>;

  CollectionData(void *data, bool owned) : m_data((V*)data), m_owned(owned) {}
  ~CollectionData() {
    if(m_owned) free(m_data);
  }
  V *data() {return m_data;}
};

/**
 * Transaction that allows read operations only. Read transactions can be run concurrently
 */
class ReadTransaction
{
  template<typename T, typename V> friend class ValueVectorPropertyStorage;
  template<typename T, typename V> friend class ValueSetPropertyStorage;
  template<typename T, typename V> friend class ObjectPropertyStorage;
  template<typename T, typename V> friend class ObjectPropertyStorageEmbedded;
  template<typename T, typename V> friend class ObjectPtrPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrPropertyStorageEmbedded;
  template<typename T, typename V> friend class ObjectVectorPropertyStorage;
  template<typename T, typename V> friend class ObjectVectorPropertyStorageEmbedded;
  template<typename T, typename V> friend class ObjectPtrVectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrVectorPropertyStorageEmbedded;
  template<typename T, typename V, typename KVIter, typename Iter> friend struct CollectionIterPropertyStorage;
  template<typename V> friend class AbstractObjectVectorStorage;
  friend class CollectionCursorBase;
  template <typename T> friend class ClassCursor;
  friend class CollectionAppenderBase;
  friend class ObjectBuf;

  CollectionInfo *readCollectionInfo(ReadBuf &readBuf);

protected:
  KeyValueStore &store;
  bool m_blockWrites;
  std::unordered_map<ObjectId, CollectionInfo *> m_collectionInfos;

  ReadTransaction(KeyValueStore &store) : store(store) {}

  void setBlockWrites(bool blockWrites) {
    m_blockWrites = blockWrites;
  }

  /**
   * load an object from the KV store non-polymorpically, non-refcounting. Used by value collections
   *
   * @param classId the actual class id, which may be the id of a subclass of T
   * @param objectId the key generated by a previous call to WriteTransaction::putObject()
   * @param obj the object to load into
   * @return true if loaded
   */
  template<typename T> bool loadObject(ClassId classId, ObjectId objectId, T &obj)
  {
    ReadBuf readBuf;
    getData(readBuf, classId, objectId, 0);

    if(readBuf.null()) return false;

    readObject<T>(this, readBuf, classId, objectId, &obj);

    return true;
  }

  /**
   * load an object from the KV store polymorphically + refcounting (if configured)
   *
   * @param classId the actual class id, which may be the id of a subclass of T
   * @param objectId the key generated by a previous call to WriteTransaction::putObject()
   * @return the object pointer, or nullptr if the key is not defined.
   */
  template<typename T> T *loadObject(ObjectKey &key)
  {
    ReadBuf readBuf;
    getData(readBuf, key, ClassTraits<T>::traits_info->refcounting);

    if(readBuf.null()) return nullptr;

    T *obj = ClassTraits<T>::makeObject(key.classId);
    readObject<T>(this, readBuf, key.classId, key.objectId, obj);

    return obj;
  }

  /**
   * load a substitute object from the KV store. This API is used in cases where the class identified by dataClassId
   * is unknown, and a substitute (from the same inheritance hierarchy) has been defined. The substitute class must
   * be a subclass of T and should not ne KV mapped. Only data relevant for T will be loaded
   *
   * @param subst the substitute object. Must have the same superclass as the missing class
   * @param missingClassId the ID of the missing class for which the data should be loaded
   * @param objectId the object ID
   * @return true if object data was found in and read from the store
   */
  template<typename T> bool loadSubstitute(T &subst, ClassId missingClassId, ObjectId objectId)
  {
    ReadBuf readBuf;
    getData(readBuf, missingClassId, objectId, 0);

    if(readBuf.null()) return false;

    readObject<T>(this, readBuf, subst, missingClassId, objectId);
    return true;
  }

  /**
   * completely load the contents of a chunked collection
   */
  template <typename T, template <typename> class Ptr=std::shared_ptr> std::vector<Ptr<T>> loadChunkedCollection(
      CollectionInfo *ci)
  {
    std::vector<Ptr<T>> result;
    for(ChunkCursor::Ptr cc= _openChunkCursor(COLLECTION_CLSID, ci->collectionId); !cc->atEnd(); cc->next()) {
      ReadBuf buf;
      cc->get(buf);

      size_t elementCount;
      readChunkHeader(buf, 0, 0, &elementCount);

      for(size_t i=0; i < elementCount; i++) {
        ClassId cid;
        ObjectId oid;
        bool deleted;
        readObjectHeader(buf, &cid, &oid, nullptr, &deleted);

        if(!deleted) {
          ClassInfo<T> *ti = FIND_CLS(T, cid);
          if(!ti) {
            T *sp = ClassTraits<T>::getSubstitute();
            if(sp) {
              readObject<T>(this, buf, *sp, cid, oid);
              result.push_back(std::shared_ptr<T>(sp));
            }
          }
          else {
            T *obj = ti->makeObject(cid);
            readObject<T>(this, buf, cid, oid, obj);
            result.push_back(std::shared_ptr<T>(obj));
          }
        }
      }
    }
    return result;
  }

  /**
   * read data by full property key into a buffer. Used internally by storage traits
   */
  virtual void getData(ReadBuf &buf, ClassId classId, ObjectId objectId, PropertyId propertyId) = 0;

  /**
   * read data by object key into a buffer. Used internally by storage traits
   */
  virtual void getData(ReadBuf &buf, ObjectKey &key, bool getRefount) = 0;

  /**
   * read object data into a buffer. Used internally
   */
  //virtual void getData(ReadBuf &buf, ObjectKey &storageKey) = 0;

  virtual CursorHelper * _openCursor(const std::vector<ClassId> &classIds) = 0;
  virtual CursorHelper * _openCursor(ClassId classId, ObjectId objectId, PropertyId propertyId) = 0;
  virtual CursorHelper * _openCursor(ClassId classId, ObjectId collectionId) = 0;

  virtual void doReset() = 0;
  virtual void doRenew() = 0;
  virtual void doAbort() = 0;

  virtual uint16_t decrementRefCount(ClassId cid, ObjectId oid) = 0;

  /**
   * retrieve info about a top-level collection
   *
   * @param collectionId
   * @return the collection info or nullptr
   */
  CollectionInfo *getCollectionInfo(ObjectId collectionId);

  /**
   * @return a cursor ofer a chunked object (e.g., collection)
   */
  virtual ChunkCursor::Ptr _openChunkCursor(ClassId classId, ObjectId objectId, bool atEnd=false) = 0;

public:
  virtual ~ReadTransaction();

  /**
   * load an object from the KV store using the key generated by a previous call to WriteTransaction::putObject().
   * Non-polymorphical, T must be the exact type of the object. The object is allocated on the heap.
   *
   * @param objectId the key generated by a previous call to WriteTransaction::putObject()
   * @return the object pointer, or nullptr if the key is not defined.
   */
  template<typename T> T *getObject(ObjectKey &key)
  {
    ReadBuf readBuf;
    getData(readBuf, key, ClassTraits<T>::traits_info->refcounting);

    if(readBuf.null()) return nullptr;

    T *tp = new T();
    readObject<T>(this, readBuf, *tp, key.classId, key.objectId);

    return tp;
  }

  /**
   * load an object from the KV store, using the key generated by a previous call to WriteTransaction::putObject()
   * Non-polymorphical, T must be the exact type of the object. The object is allocated on the heap.
   *
   * @return a shared pointer to the object, or an empty shared_ptr if the key does not exist. The shared_ptr contains
   * the ObjectId and can thus be handed to other API that requires a KV-valid shared_ptr
   */
  template<typename T> std::shared_ptr<T> getObject(ObjectId objectId)
  {
    object_handler<T> handler(ClassTraits<T>::traits_info->classId, objectId);
    T *t = loadObject<T>(handler);
    return std::shared_ptr<T>(t, handler);
  }

  /**
   * reload an object from the KV store, Non-polymorphical, T must be the exact type of the object.
   * The new object is allocated on the heap.
   *
   * @return a shared pointer to the object, or an empty shared_ptr if the object does not exist anymore. The shared_ptr contains
   * the ObjectId and can thus be handed to other API that requires a KV-valid shared_ptr
   */
  template<typename T> std::shared_ptr<T> reloadObject(std::shared_ptr<T> &obj)
  {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);
    object_handler<T> handler(*key);
    return std::shared_ptr<T>(loadObject<T>(*key), handler);
  }

  /**
   * @return a cursor over all instances of the given class
   */
  template <typename T> typename ClassCursor<T>::Ptr openCursor() {
    using Traits = ClassTraits<T>;
    std::vector<ClassId> classIds = Traits::traits_info->allClassIds();

    return typename ClassCursor<T>::Ptr(new ClassCursor<T>(_openCursor(classIds), this));
  }

  /**
   * @param objectId a valid object ID
   * @param propertyId the propertyId (1-based index into declared properties)
   * @return a cursor over the contents of a vector-valued, lazy-loading object property. The cursor will be empty if the
   * given property is not vector-valued
   */
  template <typename T, typename V> typename ClassCursor<V>::Ptr openCursor(ObjectId objectId, PropertyId propertyId) {
    ClassId t_classId = ClassTraits<T>::traits_info->classId;

    return typename ClassCursor<V>::Ptr(new ClassCursor<V>(_openCursor(t_classId, objectId, propertyId), this));
  }

  /**
   * @param obj the object that holds the property
   * @param propertyId the propertyId (1-based index into declared properties)
   * @return a cursor over the contents of a vector-valued, lazy-loading object property. The cursor will be empty if the
   * given property is not vector-valued
   */
  template <typename T, typename V> typename ClassCursor<V>::Ptr openCursor(std::shared_ptr<T> obj, PropertyId propertyId) {
    ClassId cid = ClassTraits<T>::traits_info->classId;
    ObjectId oid = ClassTraits<T>::getObjectKey(obj)->objectId;

    return typename ClassCursor<V>::Ptr(new ClassCursor<V>(_openCursor(cid, oid, propertyId), this));
  }

  /**
   * @param collectionId the id of a top-level object collection
   * @return a cursor over the contents of the collection
   */
  template <typename V> typename ObjectCollectionCursor<V>::Ptr openCursor(ObjectId collectionId) {
    return typename ObjectCollectionCursor<V>::Ptr(
        new ObjectCollectionCursor<V>( collectionId, this, _openChunkCursor(COLLECTION_CLSID, collectionId)));
  }

  /**
   * @param collectionId the id of a top-level collection
   * @return a cursor over the contents of the collection. The cursor is non-polymorphic
   */
  template <typename V> typename ValueCollectionCursor<V>::Ptr openValueCursor(ObjectId collectionId) {
    return typename ValueCollectionCursor<V>::Ptr(
        new ValueCollectionCursor<V>(collectionId, this, _openChunkCursor(COLLECTION_CLSID, collectionId)));
  }

  /**
   * convenience function to retrieve all instances of a given mapped class (including mapped subclasses) that
   * match an (optional) predicate
   *
   * @param predicate a function that is applied to each object read from the database. Defaults to always-true
   */
  template <typename T>
  std::vector<std::shared_ptr<T>> getInstances(std::function<bool(std::shared_ptr<T>)> predicate=all_predicate<T>)
  {
    std::vector<std::shared_ptr<T>> result;
    for(auto curs = openCursor<T>(); !curs->atEnd(); curs->next()) {
      auto instance = curs->get();
      if(predicate(instance)) {
        result.push_back(instance);
      }
    }
    return result;
  }

  /**
   * retrieve an attached member collection. Attached mebers are stored under a key that is derived from
   * the object they are attached to. The key is the same as if the member was a property member of the
   * attached-to object, but the property does not exist. Instead, the attached member must be loaded and saved
   * explicitly using the given API
   *
   * @param obj the object this property is attached to
   * @param propertyId a property Id which must not be one of the mapped property's id
   * @vect (out)the contents of the attached collection.
   */
  template <typename T, typename V>
  void getCollection(std::shared_ptr<T> &obj, PropertyId propertyId, std::vector<std::shared_ptr<V>> &vect)
  {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);

    ReadBuf buf;
    getData(buf, key->classId, key->objectId, propertyId);
    if(buf.null()) return ;

    size_t elementCount = buf.readInteger<size_t>(4);
    vect.reserve(elementCount);

    for(size_t i=0; i < elementCount; i++) {
      object_handler<V> hdl;
      buf.read(hdl);

      ClassInfo<V> *vi = FIND_CLS(V, hdl.classId);
      if(!vi) {
        V *vp = ClassTraits<V>::getSubstitute();
        if(vp) {
          loadSubstitute<V>(*vp, hdl.classId, hdl.objectId);
          vect.push_back(std::shared_ptr<V>(vp, hdl));
        }
      }
      else {
        V *obj = loadObject<V>(hdl);
        if(!obj) throw persistence_error("collection object not found");
        vect.push_back(std::shared_ptr<V>(obj, hdl));
      }
    }
  }

  /**
   * load a top-level (chunked) object collection
   *
   * @param collectionId an id returned from a previous #putCollection call
   */
  template <typename T, template <typename> class Ptr=std::shared_ptr> std::vector<Ptr<T>> getCollection(ObjectId collectionId)
  {
    CollectionInfo *ci = getCollectionInfo(collectionId);
    return loadChunkedCollection<T, Ptr>(ci);
  }

  /**
   * load a top-level (chunked) member collection.
   *
   * @param o the object that holds the member
   * @param p pointer to the the member variable
   * @return the collection contents
   */
  template <typename O, typename T, template <typename> class Iter>
  std::vector<std::shared_ptr<T>> getCollection(O &o, std::shared_ptr<Iter<T>> O::*p)
  {
    KVPropertyBackend &ib = dynamic_cast<KVPropertyBackend &>(*(o.*p));

    CollectionInfo *ci = getCollectionInfo(ib.getObjectId());
    return loadChunkedCollection<T, std::shared_ptr>(ci);
  }

  /**
   * load a top-level (chunked) scalar collection
   *
   * @param collectionId an id returned from a previous #putCollection call
   */
  template <typename T>
  std::vector<T> getValueCollection(ObjectId collectionId)
  {
    std::vector<T> result;
    for(ChunkCursor::Ptr cc= _openChunkCursor(COLLECTION_CLSID, collectionId); !cc->atEnd(); cc->next()) {
      ReadBuf buf;
      cc->get(buf);

      size_t elementCount;
      readChunkHeader(buf, 0, 0, &elementCount);

      for(size_t i=0; i < elementCount; i++) {
        T val;
        ValueTraits<T>::getBytes(buf, val);
        result.push_back(val);
      }
    }
    return result;
  }

  /**
   * load a member variable of the given, already persistent object. This is only useful for members which are configured
   * as lazy (only Object* properties)
   *
   * @param objectId an Id obtained from a previous put
   * @param obj the persistent object
   * @param propertyId the propertyId (1-based index of the declared property). Note that the template type
   * parameter must refer to the (super-)class which declares the member
   */
  template <typename T>
  void loadMember(ObjectId objId, T &obj, const PropertyAccessBase *pa)
  {
    using Traits = ClassTraits<T>;

    ReadBuf rb;
    ClassTraits<T>::load(this, rb, Traits::traits_info->classId, objId, &obj, pa, StoreMode::force_all);
  }

  /**
   * load a member variable of the given, already persistent object. This is only useful for members which are configured
   * as lazy (only Object* properties)
   *
   * @param obj the persistent object pointer. Note that this pointer must have been obtained from the KV store
   * @param propertyId the propertyId (1-based index of the declared property). Note that the template type
   * parameter must refer to the (super-)class which declares the member
   */
  template <typename T>
  void loadMember(std::shared_ptr<T> &obj, const PropertyAccessBase *pa)
  {
    using Traits = ClassTraits<T>;

    ObjectId objId = ClassTraits<T>::getObjectId(obj);

    ReadBuf rb;
    ClassTraits<T>::load(this, rb, Traits::traits_info->classId, objId, &obj, pa, StoreMode::force_all);
  }

  ClassId getClassId(const std::type_info &ti) {
    return store.objectTypeInfos[ti];
  }

  /**
   * same as abort, but keeps resources allocated for a subsequent renew()
   */
  void reset();
  /**
   * renew a bewviously reset() transaction
   */
  void renew();
  /**
   * abort (close) this transaction, The transaction must not be used afterward
   */
  void abort();
};

#define RAWDATA_API_ASSERT static_assert(TypeTraits<T>::byteSize == sizeof(T), \
"collection data access only supported for fixed-size types with native size equal byteSize");
#define VALUEAPI_ASSERT static_assert(has_objid<ClassTraits<T>>::value, \
"class must define an OBJECT_ID mapping to be usable with value-based API");

/**
 * Transaction for exclusive read and operations. Opening write transactions while an exclusive read is open
 * will fail with an exception. Likewise creating an exclusive read transcation while a write is ongoing
 */
class ExclusiveReadTransaction : public virtual ReadTransaction
{
  virtual bool _getCollectionData(
      CollectionInfo *info, size_t startIndex, size_t length, size_t elementSize, void **data, bool *owned) = 0;

protected:
  ExclusiveReadTransaction(KeyValueStore &store) : ReadTransaction(store) {}

public:
  /**
   * Note that the raw data API is only usable for floating point (float, double) and for integral data types that
   * conform to the LP64 data model. This precludes the long data type on Windows platforms
   *
   * @return a pointer to a memory chunk containing the raw collection data. The memory chunk may be
   * database-owned or copied, depending on whether start and end lie within the same chunk.
   */
  template <typename T> typename
  CollectionData<T>::Ptr getDataCollection(ObjectId collectionId, size_t startIndex, size_t length)
  {
    RAWDATA_API_ASSERT
    void *data;
    bool owned;
    CollectionInfo *ci = getCollectionInfo(collectionId);
    if(!ci) return nullptr;

    if(_getCollectionData(ci, startIndex, length, TypeTraits<T>::byteSize, &data, &owned)) {
      return typename CollectionData<T>::Ptr(new CollectionData<T>(data, owned));
    }
    return nullptr;
  }
};

class CollectionAppenderBase
{
protected:
  CollectionInfo *m_collectionInfo;
  const size_t m_chunkSize;
  WriteTransaction * const m_wtxn;

  WriteBuf &m_writeBuf;
  size_t m_elementCount;

  CollectionAppenderBase(WriteTransaction *wtxn, ObjectId collectionId, size_t chunkSize);
  void startChunk(size_t size);

public:
  void close();
};

/**
 * calculate shallow byte size, i.e. the size of the buffer required for properties that dont't get saved under
 * an individual key
 */
template<typename T>
static size_t calculateBuffer(T *obj, Properties *properties)
{
  if(properties->fixedSize) return properties->fixedSize;

  size_t size = 0;
  for(unsigned i=0, sz=properties->full_size(); i<sz; i++) {
    auto pa = properties->get(i);

    if(!pa->enabled) continue;

    //calculate variable size
    ClassTraits<T>::addSize(obj, pa, size);
  }
  return size;
}

/**
 * Transaction for read and write operations. Only one write transaction can be active at a time, and it should
 * be accessed from one thread only
 */
class WriteTransaction : public virtual ReadTransaction
{
  template<typename T, typename V> friend class BasePropertyStorage;
  template<typename T, typename V> friend class SimplePropertyStorage;
  template<typename T, typename V> friend class ValueVectorPropertyStorage;
  template<typename T, typename V> friend class ValueSetPropertyStorage;
  template<typename T, typename V> friend class ObjectPropertyStorage;
  template<typename T, typename V> friend class ObjectPropertyStorageEmbedded;
  template<typename T, typename V> friend class ObjectPtrPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrPropertyStorageEmbedded;
  template<typename T, typename V> friend class ObjectVectorPropertyStorage;
  template<typename T, typename V> friend class ObjectVectorPropertyStorageEmbedded;
  template<typename T, typename V> friend class ObjectPtrVectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrVectorPropertyStorageEmbedded;
  template<typename T, typename V, typename KVIter, typename Iter> friend struct CollectionIterPropertyStorage;
  template<typename V> friend class AbstractObjectVectorStorage;
  friend class CollectionAppenderBase;

  WriteBuf writeBufStart;
  WriteBuf  *curBuf;

  void writeChunkHeader(size_t startIndex, size_t elementCount);
  void writeObjectHeader(ClassId classId, ObjectId objectId, size_t size);

  /**
   * start a new chunk by allocating memory from the KV store for it. Also write the chunk header for the
   * current chunk, if any
   *
   * @param ci
   * @param chunkSize
   * @param elementCount the number of elements written to the current chunk. Used to write the header. If
   */
  void startChunk(CollectionInfo *collectionInfo, size_t chunkSize, size_t elementCount);

protected:
  const bool m_append;

  WriteTransaction(KeyValueStore &store, bool append=false) : ReadTransaction(store), m_append(append) {
    curBuf = &writeBufStart;
  }

  /**
   * remove an object from the KV store, also cleaning up referenced data
   *
   * @param classId
   * @param objectId
   */
  template <typename T>
  bool removeObject(ClassId classId, ObjectId objectId)
  {
    using Traits = ClassTraits<T>;
    Properties *props = Traits::getProperties(classId);

    if(Traits::needsPrepare()) {
      ObjectBuf prepBuf(this, classId, objectId, true);
      if(!prepBuf.null()) {
        for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
          const PropertyAccessBase *pa = props->get(px);

          if(!pa->enabled) continue;

          prepBuf.mark();
          size_t psz = ClassTraits<T>::prepareDelete(this, prepBuf, pa);
          prepBuf.unmark(psz);
        }
        //now remove the object proper
        return remove(classId, objectId);
      }
      return false;
    }
    else {
      for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
        const PropertyAccessBase *pa = props->get(px);

        if(pa->enabled && pa->storage->layout == StoreLayout::property)
          remove(classId, objectId, pa->id);
      }
      return remove(classId, objectId);
    }
  }

  /**
   * serialize the object to the write buffer
   */
  template <typename T>
  void writeObject(ClassId classId, ObjectId objectId, T &obj, Properties *properties, bool shallow)
  {
    //put data into buffer
    for(unsigned px=0, sz=properties->full_size(); px < sz; px++) {
      const PropertyAccessBase *pa = properties->get(px);
      if(!pa->enabled) continue;

      ClassTraits<T>::save(this, classId, objectId, &obj, pa, shallow ? StoreMode::force_buffer : StoreMode::force_none);
    }
  }

  template<typename T>
  void prepareUpdate(ObjectKey &key, T *obj, Properties *properties)
  {
    LazyBuf prepBuf(this, key, false);

    for(unsigned i=0, sz=properties->full_size(); i<sz; i++) {
      auto pa = properties->get(i);

      if(!pa->enabled) continue;

      prepBuf.mark();
      size_t psz = ClassTraits<T>::prepareUpdate(prepBuf, obj, pa);
      prepBuf.unmark(psz);
    }
  }

  /**
   * non-polymorphic, non-refcounting save. Used by value references
   *
   * @param objectId the object ID
   * @param obj the object to save
   * @param shallow if true, only shallow buffer will be written
   */
  template <typename T>
  ObjectId saveValueObject(ObjectId objectId, T &obj, bool shallow=false)
  {
    auto classInfo = ClassTraits<T>::traits_info;
    Properties *properties = ClassTraits<T>::traits_properties;

    size_t size = calculateBuffer(&obj, properties);
    if(!objectId) objectId = ++classInfo->maxObjectId;

    writeBuf().start(size);
    writeObject(classInfo->classId, objectId, obj, properties, shallow);

    if(!putData(classInfo->classId, objectId, 0, writeBuf()))
      throw persistence_error("data was not saved");

    writeBuf().reset();
    return objectId;
  }

  /**
   * fully polymorphic, refcounting save
   *
   * @param key the object key
   * @param obj the object to save
   * @param pa property that has changed. If null, everything will be written
   * @param shallow skip properties that go to separate keys (except pa if present)
   */
  template <typename T>
  void saveObject(ObjectKey &key, T &obj, bool setRefcount, bool shallow=false, const PropertyAccessBase *pa=nullptr)
  {
    Properties *properties;
    size_t size;

    using Traits = ClassTraits<T>;

    bool poly = Traits::traits_info->isPoly();
    if(key.isNew()) {
      if(poly) {
        key.classId = getClassId(typeid(obj));

        AbstractClassInfo *classInfo = store.objectClassInfos.at(key.classId);
        if(!classInfo) throw persistence_error("class not registered");

        if(setRefcount && classInfo->refcounting) key.refcount =  1;

        key.objectId = ++classInfo->maxObjectId;
        properties = store.objectProperties[key.classId];
      }
      else {
        key.classId = Traits::traits_info->classId;
        key.objectId = ++Traits::traits_info->maxObjectId;
        if(setRefcount && Traits::traits_info->refcounting) key.refcount =  1;
        properties = Traits::traits_properties;
      }
    }
    else {
      properties = poly ? store.objectProperties[key.classId] : Traits::traits_properties;
      if(Traits::needsPrepare()) prepareUpdate(key, &obj, properties);
    }

    if(pa && shallow)
      Traits::save(this, key.classId, key.objectId, &obj, pa, StoreMode::force_property);

    //create the data buffer
    size = calculateBuffer(&obj, properties);
    writeBuf().start(size);
    writeObject(key.classId, key.objectId, obj, properties, shallow);

    if(!putData(key, writeBuf()))
      throw persistence_error("data was not saved");

    writeBuf().reset();
  }

  WriteBuf &writeBuf() {
    return *curBuf;
  }

  void pushWriteBuf() {
    curBuf = curBuf->push();
  }

  void popWriteBuf() {
    curBuf = curBuf->pop();
  }


  struct chunk_helper {
    ClassId classId;
    ObjectId objectId;
    size_t size;
    Properties *properties;

    void set(ClassId cid, ObjectId oid, size_t sz, Properties *props) {
      classId= cid; objectId = oid; size = sz; properties = props;
    }
  };

  template <typename T, template <typename T> class Ptr>
  chunk_helper *prepare_collection(const std::vector<Ptr<T>> &vect, size_t &chunkSize)
  {
    chunk_helper *helpers = new chunk_helper[vect.size()];

    chunkSize = 0;
    for(size_t i=0, vectSize = vect.size(); i<vectSize; i++) {
      ClassId classId = getClassId(typeid(*vect[i]));
      Properties *properties = store.objectProperties[classId];
      AbstractClassInfo *classInfo = store.objectClassInfos[classId];
      ObjectId objectId = ++classInfo->maxObjectId;

      size_t sz = calculateBuffer(&(*vect[i]), properties) + ObjectHeader_sz;
      helpers[i].set(classId, objectId, sz, properties);
      chunkSize += sz;
    }
    return helpers;
  }

  /**
   * save object collection chunk
   *
   * @param vect the collection
   * @param collectionId the id of the collection
   * @param chunkId the chunk index
   * @param poly lookup classes dynamically (slight runtime overhead)
   */
  template <typename T, template <typename T> class Ptr>
  void saveChunk(const std::vector<Ptr<T>> &vect, CollectionInfo *collectionInfo, bool poly)
  {
    if(vect.empty()) return;

    if(poly) {
      size_t chunkSize = 0;
      chunk_helper *helpers = prepare_collection(vect, chunkSize);

      startChunk(collectionInfo, chunkSize, vect.size());

      for(size_t i=0, vectSize = vect.size(); i<vectSize; i++) {
        chunk_helper &helper = helpers[i];

        writeObjectHeader(helper.classId, helper.objectId, helper.size);
        writeObject(helper.classId, helper.objectId, *vect[i], helper.properties, true);
      }
      delete [] helpers;
    }
    else {
      size_t chunkSize = 0;
      size_t *sizes = new size_t[vect.size()];

      for(size_t i=0, vectSize = vect.size(); i<vectSize; i++) {
        sizes[i] = calculateBuffer(&(*vect[i]), ClassTraits<T>::traits_properties) + ObjectHeader_sz;
        chunkSize += sizes[i];
      }
      startChunk(collectionInfo, chunkSize, vect.size());

      for(size_t i=0, vectSize = vect.size(); i<vectSize; i++) {
        using Traits = ClassTraits<T>;

        AbstractClassInfo *classInfo = Traits::traits_info;
        ClassId classId = classInfo->classId;
        ObjectId objectId = ++classInfo->maxObjectId;

        size_t size = sizes[i];
        writeObjectHeader(classId, objectId, size);
        writeObject(classId, objectId, *vect[i], Traits::traits_properties, true);
      }
      delete [] sizes;
    }
  }

  /**
   * save value collection chunk
   *
   * @param vect the collection
   * @param collectionId the id of the collection
   */
  template <typename T>
  void saveChunk(const std::vector<T> &vect, CollectionInfo *ci)
  {
    if(vect.empty()) return;

    size_t chunkSize = 0;
    for(size_t i=0, vectSize = vect.size(); i<vectSize; i++)
      chunkSize += ValueTraits<T>::size(vect[i]);

    startChunk(ci, chunkSize, vect.size());

    for(size_t i=0, vectSize = vect.size(); i<vectSize; i++)
      ValueTraits<T>::putBytes(writeBuf(), vect[i]);
  }

  /**
   * save raw data collection chunk
   *
   * @param array (in, out) the raw data array. If != nullptr, data will be copied from here. Otherwise, the
   * chunk data pointer will be stored here
   * @param arraySize the number of items in array
   * @param ci the collection metadata
   */
  template <typename T>
  void saveChunk(T *&array, size_t arraySize, CollectionInfo *ci)
  {
    if(!arraySize) return;

    size_t chunkSize = arraySize * sizeof(T);

    startChunk(ci, chunkSize, arraySize);
    if(array) writeBuf().append((byte_t *)array, chunkSize);
    array = (T *)writeBuf().cur();
  }

  /**
   * save a data buffer under a full property key
   */
  virtual bool putData(ClassId classId, ObjectId objectId, PropertyId propertyId, WriteBuf &buf) = 0;

  /**
   * save a data buffer under an object key, including refcount
   */
  virtual bool putData(ObjectKey &key, WriteBuf &buf) = 0;

  /**
   * save a sub-object data buffer
   */
  virtual bool allocData(ClassId classId, ObjectId objectId, PropertyId propertyId, size_t size, byte_t **data) = 0;

  /**
   * remove an object key from the KV store. This will NOT cleanup referenced data
   */
  virtual bool remove(ClassId classId, ObjectId objectId) = 0;

  /**
   * remove a property key from the KV store. This will NOT cleanup referenced data
   */
  virtual bool remove(ClassId classId, ObjectId objectId, PropertyId propertyId) = 0;

  /**
   * clear refcounting data for all classes
   */
  virtual void clearRefCounts(std::vector<ClassId> classes) = 0;

  virtual void doCommit() = 0;

public:
  virtual ~WriteTransaction()
  {
    //assume all was popped
    curBuf->deleteChain();
  }

  void commit();

  /**
   * put a new object into the KV store. Generate a new ObjectKey and store it inside the returned shared_ptr.
   * The object becomes directly owned by the application,
   *
   * @return a shared_ptr to the input object which contains the ObjectKey
   */
  template <typename T>
  std::shared_ptr<T> putObject(T *obj)
  {
    object_handler<T> handler;
    saveObject<T>(handler, *obj, true);
    return std::shared_ptr<T>(obj, handler);
  }

  /**
   * put a new object into the KV store. Generate a new ObjectKey. The object becomes directly owned by the application,
   *
   * @return the new ObjectKey
   */
  template <typename T>
  ObjectKey putObject(T &obj)
  {
    ObjectKey key;
    saveObject<T>(key, obj, true);
    return key;
  }

  /**
   * save object state into the KV store. Use the passed ObjectKey to determine whether a new key will be assigned or an
   * existing key will be overwritten (insert or update). Update the key accordingly
   *
   * @param  obj the object to save
   * @param key (in, out) the object key
   * @param setRefCount set refcount to 1 for newly created objects. Defaults to true;
   */
  template <typename T>
  void saveObject(T &obj, ObjectKey &key, bool setRefCount=true)
  {
    saveObject<T>(key, obj, setRefCount);
  }

  /**
   * save object state into the KV store. Use the ObjectKey stored inside the shared_ptr to determine whether
   * a new key will be assigned or an existing key will be overwritten (insert or update).. Update the key accordingly
   *
   * @param obj a persistent object pointer, which must have been obtained from KV
   * @param setRefCount set refcount to 1 for newly created objects. Defaults to true;
   * @return the object ID
   */
  template <typename T>
  ObjectId saveObject(const std::shared_ptr<T> &obj, bool setRefCount=true)
  {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);
    saveObject<T>(*key, *obj, setRefCount);
    return key->objectId;
  }

  /**
   * update a member variable of the given, already persistent object. The current variable state is written to the
   * store
   *
   * @param objectId an Id obtained from a previous put
   * @param obj the persistent object
   * @param propertyId the propertyId (1-based index of the declared property). Note that the template type
   * parameter must refer to the (super-)class which declares the member
   */
  template <typename T>
  void updateMember(ObjectKey &key, T &obj, const PropertyAccessBase *pa, bool shallow=false)
  {
    switch(pa->storage->layout) {
      case StoreLayout::property:
        //property goes to a separate key, no need to touch the object buffer
        ClassTraits<T>::save(this, key.classId, key.objectId, &obj, pa, shallow ? StoreMode::force_buffer : StoreMode::force_all);
        break;
      case StoreLayout::embedded_key:
        //save property value and shallow buffer
        saveObject<T>(key, obj, false ,true, pa);
        break;
      case StoreLayout::all_embedded:
        //shallow buffer only
        saveObject<T>(key, obj, false, true, nullptr);
    }
  }

  /**
   * update a member variable of the given, already persistent object. The current variable state is written to the
   * store
   *
   * @param obj the persistent object pointer. Note that this pointer must have been obtained from the KV store
   * @param propertyId the propertyId (1-based index of the declared property). Note that the template type
   * parameter must refer to the (super-)class which declares the member
   */
  template <typename T>
  void updateMember(std::shared_ptr<T> &obj, const PropertyAccessBase *pa, bool shallow=false)
  {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);
    updateMember(*key, *obj, pa, shallow);
  }

  /**
   * insert an attached member collection (see explanation in accompanying get function). Deviant from other put* functions.
   * this will not alwways create a new collection but rather overwrite an existing one if present.
   *
   * @param obj the object this collection is attached to
   * @param propertyId a unique property Id
   * @param vect the contents of the collection
   * @param saveMembers if true (default), collection members will be saved, too
   */
  template <typename T, typename V>
  void putCollection(std::shared_ptr<T> &obj, PropertyId propertyId, const std::vector<std::shared_ptr<V>> &vect,
                     bool saveMembers=true)
  {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);
    byte_t *data;
    size_t bufSz = vect.size()*ObjectKey_sz+4;

    writeBuf().start(bufSz);
    writeBuf().appendInteger<size_t>(vect.size(), 4);

    for(auto &v : vect) {
      ObjectKey *key = ClassTraits<V>::getObjectKey(v);

      if(saveMembers) {
        pushWriteBuf();
        saveObject(v);
        popWriteBuf();
      }
      else {
        if(key->isNew()) {
          pushWriteBuf();
          saveObject(*v, *key);
          popWriteBuf();
        }
      }
      writeBuf().append(*key);
    }

    if(!putData(key->classId, key->objectId, propertyId, writeBuf()))
      throw persistence_error("putData failed");
  }

  /**
   * delete an attached member collection.
   *
   * @param obj the object this collection is attached to
   * @param propertyId a unique property Id
   * @param deleteMembers if true (default), collection members will be deleted, too
   */
  template <typename T, typename V>
  void deleteCollection(std::shared_ptr<T> &obj, PropertyId propertyId, bool deleteMembers=true)
  {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);

    if(deleteMembers) {
      ReadBuf buf;
      getData(buf, key->classId, key->objectId, propertyId);
      if(!buf.null()) {
        size_t sz = buf.readInteger<size_t>(4);
        for(size_t i=0; i<sz; i++) {
          ObjectKey key;
          buf.read(key);
          uint16_t refcount = ClassTraits<V>::traits_info->refcounting ?
                              decrementRefCount(key.classId, key.objectId) : uint16_t(0);

          if(refcount <= 1) removeObject<V>(key.classId, key.objectId);
        }
      }
    }
    remove(key->classId, key->objectId, propertyId);
  }

  /**
   * create a top-level (chunked) object collection.
   *
   * @param vect the initial collection contents
   * @return the collection ID
   */
  template <typename T, template <typename> class Ptr> ObjectId putCollection(const std::vector<Ptr<T>> &vect)
  {
    CollectionInfo *ci = new CollectionInfo(++store.m_maxCollectionId);
    m_collectionInfos[ci->collectionId] = ci;

    saveChunk(vect, ci, ClassTraits<T>::traits_info->isPoly());

    return ci->collectionId;
  }

  /**
   * create a top-level (chunked) member object collection and assign it to the given property, which is expected to be
   * mapped as CollectionIterPropertyAssign
   *
   * @param o the object that holds the member
   * @param pa the property accessor, usually obtained via PROPERTY macro
   * @param vect the initial collection contents
   */
  template <typename O, typename T> void putCollection(O &o, const PropertyAccessBase *pa, const std::vector<std::shared_ptr<T>> &vect)
  {
    ObjectId collectionId = putCollection<T, std::shared_ptr>(vect);
    KVPropertyBackend::assign(store, o, pa, collectionId);
  }

  /**
   * save a top-level (chunked) value collection.
   *
   * @param vect the initial collection contents
   */
  template <typename T>
  ObjectId putValueCollection(const std::vector<T> &vect)
  {
    CollectionInfo *ci = new CollectionInfo(++store.m_maxCollectionId);
    m_collectionInfos[ci->collectionId] = ci;

    saveChunk(vect, ci);

    return ci->collectionId;
  }

  /**
   * create a top-level (chunked) member value collection and assign it to the given property, which is expected to be
   * mapped as CollectionIterPropertyAssign
   *
   * @param o the object that holds the member
   * @param pa the property accessor, usually obtained via PROPERTY macro
   * @param vect the initial collection contents
   */
  template <typename O, typename T> void putValueCollection(O &o, const PropertyAccessBase *pa, const std::vector<T> &vect)
  {
    ObjectId collectionId = putValueCollection(vect);
    KVPropertyBackend::assign(store, o, pa, collectionId);
  }

  /**
   * save a top-level (chunked) raw data collection. Note that the rwaw data API is only usable for
   * floating point (float, double) and for integral data types that conform to the LP64 data model.
   * This precludes the long data type on Windows platforms
   *
   * @param array the initial collection contents.
   * @param arraySize length of the contents
   */
  template <typename T>
  ObjectId putDataCollection(const T* array, size_t arraySize)
  {
    RAWDATA_API_ASSERT
    CollectionInfo *ci = new CollectionInfo(++store.m_maxCollectionId);
    m_collectionInfos[ci->collectionId] = ci;

    saveChunk(array, arraySize, ci);

    return ci->collectionId;
  }

  /**
   * create an initial chunk for a top-level raw data collection. Note that the rwaw data API is only usable for
   * floating point (float, double) and for integral data types that conform to the LP64 data model.
   * This precludes the long data type on Windows platforms
   *
   * Note: the chunk data pointer is only usable until the next update operation, or untile the transaction completes
   *
   * @param array (out) the initial collection chunk data area.
   * @param arraySize length in bytes of the data area
   */
  template <typename T>
  ObjectId putDataCollection(T ** array, size_t arraySize)
  {
    RAWDATA_API_ASSERT
    CollectionInfo *ci = new CollectionInfo(++store.m_maxCollectionId);
    m_collectionInfos[ci->collectionId] = ci;

    *array = nullptr;
    saveChunk(*array, arraySize, ci);

    return ci->collectionId;
  }
  /**
   * create a top-level (chunked) member data collection and assign it to the given property, which is expected to be
   * mapped as CollectionIterPropertyAssign
   *
   * @param o the object that holds the member
   * @param pa the property accessor, usually obtained via PROPERTY macro
   * @param array the initial collection contents
   * @param arraySize length of the contents
   */
  template <typename O, typename T> void putDataCollection(O &o, PropertyAccessBase *pa, const T* array, size_t arraySize)
  {
    ObjectId collectionId = putDataCollection(array, arraySize);
    KVPropertyBackend::assign(store, o, pa, collectionId);
  }

  /**
   * append to a top-level (chunked) object collection.
   *
   * @param vect the collection contents
   * @param chunkSize size of chunk
   */
  template <typename T, template <typename T> class Ptr>
  void appendCollection(ObjectId collectionId, const std::vector<Ptr<T>> &vect)
  {
    CollectionInfo *ci = getCollectionInfo(collectionId);
    if(!ci) throw persistence_error("collection not found");

    saveChunk(vect, ci, ClassTraits<T>::traits_info->isPoly());
  }

  /**
   * append to a top-level (chunked) value collection.
   *
   * @param vect the collection contents
   * @param chunkSize size of keys chunk
   */
  template <typename T>
  void appendValueCollection(ObjectId collectionId, const std::vector<T> &vect)
  {
    CollectionInfo *ci= getCollectionInfo(collectionId);
    if(!ci) throw persistence_error("collection not found");

    saveChunk(vect, ci);
  }

  /**
   * append to a top-level (chunked) raw data collection. Note that the raw data API is only usable for floating-point
   * (float, double) and for integral data types that conform to the LP64 data model. This precludes the long data
   * type on Windows platforms
   *
   * @param data the chunk contents
   * @param chunkSize size of keys chunk
   */
  template <typename T>
  void appendDataCollection(ObjectId collectionId, const T *data, size_t dataSize)
  {
    RAWDATA_API_ASSERT
    CollectionInfo *ci = getCollectionInfo(collectionId);
    if(!ci) throw persistence_error("collection not found");

    saveChunk(data, dataSize, ci);
  }

  /**
   * allocate a new chunk, appending to a top-level (chunked) raw data collection.
   * Note that the raw data API is only usable for floating-point (float, double) and for integral data types that conform to the
   * LP64 data model. This precludes the long data type on Windows platforms
   *
   * Note: the chunk data pointer is only usable until the next update operation, or until the transaction completes
   *
   * @param chunkSize size of keys chunk
   * @return the address of the data area of the chunk
   */
  template <typename T>
  void appendDataCollection(ObjectId collectionId, T **data, size_t dataSize)
  {
    RAWDATA_API_ASSERT
    CollectionInfo *ci = getCollectionInfo(collectionId);
    if(!ci) throw persistence_error("collection not found");

    *data = nullptr;
    saveChunk(*data, dataSize, ci);
  }

  template <typename T>
  void deleteObject(ObjectKey &key) {
    if(key.refcount > 1) throw persistence_error("removeObject: refcount > 1");
    removeObject<T>(key.classId, key.objectId);
  }

  template <typename T>
  void deleteObject(std::shared_ptr<T> &obj) {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);
    if(key->refcount > 1) throw persistence_error("removeObject: refcount > 1");
    removeObject<T>(key->classId, key->objectId);
  }

  /**
   * clear out refcounting data for all classes in the hierarchy starting at T
   */
  template <typename T>
  void clearRefCounts() {
    clearRefCounts(ClassTraits<T>::traits_info->allClassIds());
  }

  /**
  * appender for sequentially extending a top-level, chunked object collection
  */
  template <typename T>
  class ObjectCollectionAppender : public CollectionAppenderBase
  {
    const bool m_poly;
    ObjectClassInfos * const m_objectClassInfos;
    ObjectProperties * const m_objectProperties;

  protected:
    void _put(T &obj)
    {
      ClassId cid;
      ObjectId oid;

      Properties *properties;
      if(m_poly) {
        cid = m_wtxn->getClassId(typeid(obj));

        AbstractClassInfo *classInfo = m_objectClassInfos->at(cid);
        oid = ++classInfo->maxObjectId;
        properties = m_objectProperties->at(cid);
      }
      else {
        cid = ClassTraits<T>::traits_info->classId;
        oid = ++ClassTraits<T>::traits_info->maxObjectId;
        properties = ClassTraits<T>::traits_properties;
      }
      size_t size = calculateBuffer(&obj, properties) + ObjectHeader_sz;

      if(m_writeBuf.avail() < size) startChunk(size);

      m_wtxn->writeObjectHeader(cid, oid, size);
      m_wtxn->writeObject(cid, oid, obj, properties, false);

      m_elementCount++;
    }

  public:
    using Ptr = std::shared_ptr<ObjectCollectionAppender>;

    ObjectCollectionAppender(WriteTransaction *wtxn, ObjectId collectionId,
                             size_t chunkSize, ObjectClassInfos *objectClassInfos, ObjectProperties *objectProperties, bool poly)
        : CollectionAppenderBase(wtxn, collectionId, chunkSize),
          m_objectClassInfos(objectClassInfos), m_objectProperties(objectProperties), m_poly(poly)
    {
    }

    void put(std::shared_ptr<T> obj) {
      _put(*obj);
    }

    void put(T *obj) {
      _put(*obj);
    }
  };

  /**
  * appender for sequentially extending a top-level, chunked value collection
  */
  template <typename T>
  class ValueCollectionAppender : public CollectionAppenderBase
  {
  public:
    using Ptr = std::shared_ptr<ValueCollectionAppender>;

    ValueCollectionAppender(WriteTransaction *wtxn, ObjectId collectionId, size_t chunkSize)
        : CollectionAppenderBase(wtxn, collectionId, chunkSize)
    {}

    void put(T val)
    {
      size_t sz = TypeTraits<T>::byteSize;
      if(sz == 0) sz = ValueTraits<T>::size(val);

      size_t avail = m_writeBuf.avail();

      if(avail < sz) startChunk(sz);

      ValueTraits<T>::putBytes(m_writeBuf, val);
      m_elementCount++;
    }
  };

  /**
  * appender for sequentially extending a top-level, chunked value collection
  */
  template <typename T>
  class DataCollectionAppender : public CollectionAppenderBase
  {
  public:
    using Ptr = std::shared_ptr<DataCollectionAppender>;

    DataCollectionAppender(WriteTransaction *wtxn, ObjectId collectionId, size_t chunkSize)
        : CollectionAppenderBase(wtxn, collectionId, chunkSize)
    {}

    void put(T *val, size_t dataSize)
    {
      size_t avail = m_writeBuf.avail();
      byte_t *data = (byte_t *)val;

      if(avail >= sizeof(T)) {
        m_writeBuf.append(data, avail);
        m_elementCount += avail / sizeof(T);
        dataSize -= avail;
        data += avail;
      }
      if(dataSize) {
        startChunk(dataSize < m_chunkSize ? m_chunkSize : dataSize);
        m_writeBuf.append(data, dataSize);
        m_elementCount += dataSize / sizeof(T);
      }
    }
  };

  /**
   * create an appender for the given top-level object collection
   *
   * @param collectionId the id of a top-level collection
   * @param chunkSize the chunk size
   * @return an appender over the contents of the collection.
   */
  template <typename V> typename ObjectCollectionAppender<V>::Ptr appendCollection(
      ObjectId collectionId, size_t chunkSize = DEFAULT_CHUNKSIZE)
  {
    return typename ObjectCollectionAppender<V>::Ptr(new ObjectCollectionAppender<V>(
        this, collectionId, chunkSize, &store.objectClassInfos, &store.objectProperties, ClassTraits<V>::traits_info->isPoly()));
  }

  /**
   * create an appender for the given top-level raw-data collection
   *
   * @param collectionId the id of a top-level collection
   * @param chunkSize the chunk size
   * @return an appender over the contents of the collection.
   */
  template <typename V> typename ValueCollectionAppender<V>::Ptr appendValueCollection(
      ObjectId collectionId, size_t chunkSize = DEFAULT_CHUNKSIZE)
  {
    return typename ValueCollectionAppender<V>::Ptr(new ValueCollectionAppender<V>(this, collectionId, chunkSize));
  }

  /**
   * create an appender for the given top-level raw-data collection
   *
   * @param collectionId the id of a top-level collection
   * @param chunkSize the keychunk size
   * @return an appender over the contents of the collection.
   */
  template <typename T> typename ValueCollectionAppender<T>::Ptr appendDataCollection(
      ObjectId collectionId, size_t chunkSize = DEFAULT_CHUNKSIZE)
  {
    RAWDATA_API_ASSERT
    return typename DataCollectionAppender<T>::Ptr(new DataCollectionAppender<T>(this, collectionId, chunkSize));
  }
};

/**
 * storage class template for base types that go directly into the shallow buffer
 */
template<typename T, typename V>
struct BasePropertyStorage : public StoreAccessBase
{
  BasePropertyStorage() : StoreAccessBase(StoreLayout::all_embedded, TypeTraits<V>::byteSize) {}

  size_t size(ObjectBuf &buf) const override {return TypeTraits<V>::byteSize;}
  size_t size(void *obj, const PropertyAccessBase *pa) override {return TypeTraits<V>::byteSize;}

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    ValueTraits<V>::putBytes(tr->writeBuf(), val);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ValueTraits<V>::getBytes(buf, val);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage class template for cstring, with dynamic size calculation (type.byteSize is 0). Note that after loading
 * the data store, the pointed-to belongs to the datastore and will in all likelihood become invalid by the end of
 * the trasaction. It is up to the application to copy the value away (or use std::string)
 */
template<typename T>
struct BasePropertyStorage<T, const char *> : public StoreAccessBase
{
  size_t size(const byte_t *buf) const override {
    return strlen(buf)+1;
  }
  size_t size(void *obj, const PropertyAccessBase *pa) override {
    const char * val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    return strlen(val) + 1;
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    const char * val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    ValueTraits<const char *>::putBytes(tr->writeBuf(), val);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    const char * val;
    T *tp = reinterpret_cast<T *>(obj);
    ValueTraits<const char *>::getBytes(buf, val);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage template class for std::string, with dynamic size calculation (type.byteSize is 0)
 */
template<typename T>
struct BasePropertyStorage<T, std::string> : public StoreAccessBase
{
  size_t size(ObjectBuf &buf) const override {
    return buf.strlen()+1;
  }
  size_t size(void *obj, const PropertyAccessBase *pa) override {
    std::string val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    return val.length() + 1;
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::string val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    ValueTraits<std::string>::putBytes(tr->writeBuf(), val);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::string val;
    T *tp = reinterpret_cast<T *>(obj);
    ValueTraits<std::string>::getBytes(buf, val);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage template for ClassId-typed properties. The ClassId (which is already part of the key) is mapped to an
 * object property
 */
template<typename T>
struct ObjectIdStorage : public StoreAccessBase
{
  ObjectIdStorage() : StoreAccessBase(StoreLayout::none) {}

  size_t size(ObjectBuf &buf) const override {return 0;}
  size_t size(void *obj, const PropertyAccessBase *pa) override {return 0;}

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    //not saved, only loaded
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, objectId);
  }
};

/**
 * storage template for std::vector of simple values. All values are serialized into one consecutive buffer which is
 * stored under a property key for the given object.
 */
template<typename T, typename V>
struct ValueVectorPropertyStorage : public StoreAccessPropertyKey
{
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    size_t psz = 0;
    for(auto &v : val) psz += ValueTraits<V>::size(v);
    if(psz) {
      WriteBuf propBuf(psz);

      for(auto v : val) ValueTraits<V>::putBytes(propBuf, v);

      if(!tr->putData(classId, objectId, pa->id, propBuf))
        throw persistence_error("data was not saved");
    }
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<V> val;

    ReadBuf readBuf;
    tr->getData(readBuf, classId, objectId, pa->id);

    while(!readBuf.atEnd()) {
      V v;
      ValueTraits<V>::getBytes(readBuf, v);
      val.push_back(v);
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage template for sets of simnple vvalues. Similar to value vector, but based on a std::set
 */
template<typename T, typename V>
struct ValueSetPropertyStorage : public StoreAccessPropertyKey
{
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::set<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    size_t psz = 0;
    for(auto &v : val) psz += ValueTraits<V>::size(v);
    if(psz) {
      WriteBuf propBuf(psz);

      for(auto v : val) ValueTraits<V>::putBytes(propBuf, v);

      if(!tr->putData(classId, objectId, pa->id, propBuf))
        throw persistence_error("data was not saved");
    }
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::set<V> val;

    ReadBuf readBuf;
    tr->getData(readBuf, classId, objectId, pa->id);

    while(!readBuf.atEnd()) {
      V v;
      ValueTraits<V>::getBytes(readBuf, v);
      val.insert(v);
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage template for mapped non-pointer object references. Since the object is referenced by value value in the enclosing
 * class, storage can only be non-polymorphic. The object is serialized into a separate buffer, but the key is written to the
 * enclosing object's buffer. the referenced object is required to hold an ObjectId-typed member variable which is mapped as
 * OBJECT_ID
 */
template<typename T, typename V> struct ObjectPropertyStorage : public StoreAccessEmbeddedKey
{
  VALUEAPI_ASSERT

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    ClassId childClassId  = ClassTraits<V>::traits_info->classId;
    auto ida = ClassTraits<V>::objectIdAccess();

    //save the value object
    tr->pushWriteBuf();
    ObjectId childObjectId = tr->saveValueObject(ida->get(val), val);
    ida->set(val, childObjectId);
    tr->popWriteBuf();

    //save the key in this objects write buffer
    tr->writeBuf().append(childClassId, childObjectId);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    ClassId cid; ObjectId oid;
    buf.read(cid, oid);

    V v;
    tr->loadObject<V>(cid, oid, v);
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, v);
  }
};

/**
 * storage template for mapped non-pointer object references which are directly serialized into the enclosing object's
 * buffer. Storage is shallow-only, i.e. properties of the referred-to object which are not themselves shallow-serializable
 * are ignored. Since reference is by value, there is no polymorphism
 */
template<typename T, typename V> struct ObjectPropertyStorageEmbedded : public StoreAccessBase
{
  void init(const PropertyAccessBase *pa) override {
    //in case of circular dependencies, ClassTraits<V>::traits_properties will not have been intialized, and
    //thus fixedSize is 0. Not a problem, only a failed optimization
    size_t fs = ClassTraits<V>::traits_properties->fixedSize;
    fixedSize = fs ? fs + 4 : 0;
  }

  ObjectPropertyStorageEmbedded() : StoreAccessBase(StoreLayout::all_embedded) {}

  size_t size(ObjectBuf &buf) const override {return buf.readInteger<unsigned>(4);}
  size_t size(void *obj, const PropertyAccessBase *pa) override {
    size_t sz = ClassTraits<V>::traits_properties->fixedSize;
    if(sz)  return sz + 4;

    V v;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, v);
    return calculateBuffer(&v, ClassTraits<V>::traits_properties) + 4;
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    ClassId childClassId = ClassTraits<V>::traits_info->classId;
    size_t sz = ClassTraits<V>::traits_properties->fixedSize;
    if(!sz) sz = calculateBuffer(&val, ClassTraits<V>::traits_properties);

    tr->writeBuf().appendInteger(sz, 4);
    tr->writeObject(childClassId, 1, val, ClassTraits<V>::traits_properties, true);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    ClassId childClassId = ClassTraits<V>::traits_info->classId;

    V v;
    buf.readInteger<unsigned>(4);
    readObject(tr, buf, v, childClassId, 1);

    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, v);
  }
};

/**
 * storage template for shared_ptr-based mapped object references. Fully polymorphic
 */
template<typename T, typename V>
class ObjectPtrPropertyStorage : public StoreAccessEmbeddedKey
{
protected:
  bool m_lazy;
  ClassId prepCid = 0;
  ObjectId prepOid = 0;

public:
  ObjectPtrPropertyStorage(bool lazy=false) : m_lazy(lazy) {}

  bool preparesUpdates(ClassId classId) override
  {
    return ClassTraits<V>::traits_info->hasClassId(classId);
  }
  size_t prepareUpdate(ObjectBuf &buf, void *obj, const PropertyAccessBase *pa) override
  {
    if(ClassTraits<V>::traits_info->refcounting) {
      //read pre-update state
      buf.read(prepCid, prepOid);
    }
    return size(buf);
  }
  size_t prepareDelete(WriteTransaction *tr, ObjectBuf &buf, const PropertyAccessBase *pa) override {
    if(ClassTraits<V>::traits_info->refcounting) {
      ClassId cid; ObjectId oid;
      buf.read(cid, oid);
      if(cid && oid && tr->decrementRefCount(cid, oid) == 0) tr->removeObject<V>(cid, oid);
    }
    return size(buf);
  }

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::shared_ptr<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    bool refcount = ClassTraits<V>::traits_info->refcounting;
    if(val) {
      //save the pointed-to object
      ObjectKey *childKey = ClassTraits<V>::getObjectKey(val);

      if(mode != StoreMode::force_buffer) {
        if(refcount && prepOid && prepOid != childKey->objectId) {
          //previous object reference is about to be overwritten. Delete if refcount == 1
          if(tr->decrementRefCount(prepCid, prepOid) == 0) tr->removeObject<V>(prepCid, prepOid);
          childKey->refcount++;
        }
        tr->pushWriteBuf();
        tr->saveObject<V>(*childKey, *val, true);
        tr->popWriteBuf();
      }
      if(mode != StoreMode::force_property) {
        //save the key in this objects write buffer
        tr->writeBuf().append(*childKey);
      }
    }
    else {
      if(refcount && mode != StoreMode::force_buffer && prepOid && tr->decrementRefCount(prepCid, prepOid) == 0)
        tr->removeObject<V>(prepCid, prepOid);

      //save the key in this objects write buffer
      if(mode != StoreMode::force_property)
        tr->writeBuf().append(ObjectKey::NIL);
    }
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    if(m_lazy && mode == StoreMode::force_none) {
      buf.read(ObjectKey_sz);
      return;
    }

    object_handler<V> handler;
    buf.read(handler);

    std::shared_ptr<V> vp;
    ClassInfo<V> *vi = FIND_CLS(V, handler.classId);
    if(!vi) {
      V *v = ClassTraits<V>::getSubstitute();
      if(v) {
        tr->loadSubstitute<V>(*v, handler.classId, handler.objectId);
        vp = std::shared_ptr<V>(v, handler);
      }
    }
    else {
      V *v = tr->loadObject<V>(handler);
      vp = std::shared_ptr<V>(v, handler);
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, vp);
  }
};

/**
 * storage template for shared_ptr-based object references which are directly serialized into the enclosing object's
 * buffer. Storage is shallow-only, i.e. properties of the referred-to object which are not themselves shallow-serializable
 * are ignored. Fully polymorphic
 */
template<typename T, typename V> struct ObjectPtrPropertyStorageEmbedded : public StoreAccessBase
{
  size_t size(ObjectBuf &buf) const override {
    buf.read(ClassId_sz);
    unsigned objSize = buf.readInteger<unsigned>(4);
    return ClassId_sz + 4 + objSize;
  }
  size_t size(void *obj, const PropertyAccessBase *pa) override {
    std::shared_ptr<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    return ClassTraits<V>::bufferSize(&(*val)) + ClassId_sz + 4;
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::shared_ptr<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    ClassId childClassId = 0;
    size_t sz = val ? ClassTraits<V>::bufferSize(&(*val), &childClassId) : 0;

    tr->writeBuf().appendInteger(childClassId, ClassId_sz);
    tr->writeBuf().appendInteger(sz, 4);

    if(val) tr->writeObject(childClassId, 1, *val, ClassTraits<V>::getProperties(childClassId), true);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::shared_ptr<V> val;
    ClassId childClassId = buf.readInteger<ClassId>(ClassId_sz);
    unsigned sz = buf.readInteger<unsigned>(4);
    if(sz) {
      ClassInfo<V> *vi = FIND_CLS(V, childClassId);
      if(!vi) {
        buf.mark();
        V *vp = ClassTraits<V>::getSubstitute();
        if(vp) {
          readObject<V>(tr, buf, *vp, childClassId, 1, StoreMode::force_buffer);
          val.reset(vp);
        }
        buf.unmark(sz);
      }
      else {
        V *vp = vi->makeObject(childClassId);
        readObject<V>(tr, buf, childClassId, 1, vp);
        val.reset(vp);
      }
      T *tp = reinterpret_cast<T *>(obj);
      ClassTraits<T>::get(*tp, pa, val);
    }
  }
};

/**
 * storage template for mapped object vectors. Value-based, therefore not polymorphic. Vector element objects are required
 * to hold an ObjectId-typed member variable which is referenced through the keyPropertyId
 */
template<typename T, typename V> class ObjectVectorPropertyStorage : public StoreAccessPropertyKey
{
  VALUEAPI_ASSERT
  bool m_lazy;

public:
  ObjectVectorPropertyStorage(bool lazy) : m_lazy(lazy) {}

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    if(m_lazy && mode == StoreMode::force_none) return;

    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    auto ida = ClassTraits<V>::objectIdAccess();
    ClassId childClassId = ClassTraits<V>::traits_info->classId;
    size_t psz = ObjectKey_sz * val.size();
    WriteBuf propBuf(psz);

    //write new vector
    tr->pushWriteBuf();
    for(V &v : val) {
      ObjectId childId = ida->get(v);
      if(mode != StoreMode::force_buffer) {
        childId = tr->saveValueObject<V>(childId, v);
        ida->set(v, childId);
      }
      propBuf.append(childClassId, childId);
    }
    tr->popWriteBuf();

    //vector goes into a separate property key
    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");
  }

  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    if(m_lazy && mode == StoreMode::force_none) return;

    std::vector<V> val;

    //load vector base (array of object keys) data from property key
    ReadBuf readBuf;
    tr->getData(readBuf, classId, objectId, pa->id);

    if(!readBuf.null()) {
      ClassId cid;
      ObjectId oid;
      while(readBuf.read(cid, oid)) {
        V obj;
        if(tr->loadObject(cid, oid, obj)) val.push_back(obj);
      }
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage template for vectors of mapped objects where the objects are directly serialized into the enclosing object's buffer.
 * The vector elements receive an object Id that is equal vector index + 1. This ID is thus not valid outside the vector.
 * Value-based, therefore not polymorphic.
 *
 * Only the shallow object buffer is saved, non-embeddable members are ignored during serialization
 */
template<typename T, typename V> class ObjectVectorPropertyStorageEmbedded : public StoreAccessBase
{
public:
  size_t size(ObjectBuf &buf) const override {
    unsigned vectSize = buf.readInteger<unsigned>(4);
    size_t sz = ClassTraits<V>::traits_properties->fixedSize;
    if(sz) {
      return vectSize * (sz + 4) + 4;
    }
    else {
      for(unsigned i=0; i<vectSize; i++) {
        unsigned objSize = buf.readInteger<unsigned>(4);
        buf.read(objSize);
        sz += 4 + objSize;
      }
      return sz + 4;
    }
  }
  size_t size(void *obj, const PropertyAccessBase *pa) override {
    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    size_t sz = ClassTraits<V>::traits_properties->fixedSize;
    if(sz) return val.size() * (sz + 4) + 4;

    for(V &v : val)
      sz += calculateBuffer(&v, ClassTraits<V>::traits_properties) + 4;
    return sz + 4;
  }

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    tr->writeBuf().appendInteger((unsigned)val.size(), 4);
    ClassId childClassId = ClassTraits<V>::traits_info->classId;
    PropertyId childObjectId = 0;
    size_t fsz = ClassTraits<V>::traits_properties->fixedSize;
    for(V &v : val) {
      size_t sz = fsz ? fsz : calculateBuffer(&v, ClassTraits<V>::traits_properties);

      tr->writeBuf().appendInteger(sz, 4);
      tr->writeObject(childClassId, ++childObjectId, v, ClassTraits<V>::traits_properties, true);
    }
  }

  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<V> val;

    ClassId childClassId = ClassTraits<V>::traits_info->classId;
    PropertyId childObjectId = 0;

    unsigned sz = buf.readInteger<unsigned>(4);
    for(size_t i=0; i< sz; i++) {
      V v;
      buf.readInteger<unsigned>(4);
      readObject(tr, buf, v, childClassId, ++childObjectId);
      val.push_back(v);
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage template for vectors of pointers to mapped objects. Collection objects are saved under individual top-level keys.
 * The collection itself holds copies of all member keys in a shallow buffer and is stored under a property key for the enclosing
 * object.
 *
 * Fully polymorphic
 */
template<typename T, typename V> class ObjectPtrVectorPropertyStorage : public StoreAccessPropertyKey
{
  bool m_lazy;
  bool updatePrepared = false;
  const PropertyAccessBase *inverse = nullptr;

  bool preparesUpdates(ClassId classId) override
  {
    return ClassTraits<V>::traits_info->classId == classId;
  }
  size_t prepareUpdate(ObjectBuf &buf, void *obj, const PropertyAccessBase *pa) override
  {
    updatePrepared = ClassTraits<V>::traits_info->refcounting;
    return size(buf);
  }
  size_t prepareDelete(WriteTransaction *tr, ObjectBuf &buf, const PropertyAccessBase *pa) override
  {
    if(ClassTraits<V>::traits_info->refcounting) {
      ReadBuf readBuf;
      tr->getData(readBuf, buf.key.classId, buf.key.objectId, pa->id);
      if (!readBuf.null()) {
        //first load all keys, because decrement/remove will invalidate the buffer
        size_t count = readBuf.size() / ObjectKey_sz;
        std::vector<ObjectKey> oks(count);
        for(size_t i=0; i<count; i++) readBuf.read(oks[i]);

        //now work all keys by decrementing refcount and deleting
        for(size_t i=0; i<count; i++) {
          ObjectKey &key = oks[i];
          if(tr->decrementRefCount(key.classId, key.objectId) == 0)
            tr->removeObject<V>(key.classId, key.objectId);
        }
      }
    }
    tr->remove(buf.key.classId, buf.key.objectId, pa->id);
    return size(buf);
  }

  void loadStoredKeys(ReadTransaction *tr, ClassId cid, ObjectId oid, PropertyId pid, std::set<ObjectKey> &keys)
  {
    ReadBuf readBuf;
    tr->getData(readBuf, cid, oid, pid);
    if (!readBuf.null()) {
      ObjectKey sk;
      while (readBuf.read(sk)) keys.insert(sk);
    }
  }

public:
  ObjectPtrVectorPropertyStorage(bool lazy) : m_lazy(lazy) {}

  void init(const PropertyAccessBase *pa) override {
    inverse = ClassTraits<V>::getInverseAccess(pa);
  }

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    if(m_lazy && mode == StoreMode::force_none) return;

    std::vector<std::shared_ptr<V>> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    std::set<ObjectKey> oldKeys;

    if (updatePrepared && mode != StoreMode::force_buffer)
      loadStoredKeys(tr, classId, objectId, pa->id, oldKeys);
    updatePrepared = false;

    size_t psz = ObjectKey_sz * val.size();
    WriteBuf propBuf(psz);

    tr->pushWriteBuf();
    for(std::shared_ptr<V> &v : val) {
      ObjectKey *childKey = ClassTraits<V>::getObjectKey(v);

      if(mode != StoreMode::force_buffer) {
        if(ClassTraits<V>::traits_info->refcounting && !childKey->isNew()) {
          if(oldKeys.empty() || oldKeys.erase(*childKey)) childKey->refcount++;
        }
        tr->saveObject<V>(*childKey, *v, true);
      }
      propBuf.append(*childKey);
    }
    tr->popWriteBuf();

    //vector goes into a separate property key
    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");

    //cleanup orphaned objects
    for(auto &key : oldKeys) {
      if(tr->decrementRefCount(key.classId, key.objectId) == 0)
        tr->removeObject<V>(key.classId, key.objectId);
    }
  }

  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    if(m_lazy && mode == StoreMode::force_none) return;

    std::vector<std::shared_ptr<V>> val;
    T *tp = reinterpret_cast<T *>(obj);

    //load vector base (array of object keys) data from property key
    ReadBuf readBuf;
    tr->getData(readBuf, classId, objectId, pa->id);

    if(!readBuf.null()) {
      object_handler<V> handler;
      while(readBuf.read(handler)) {
        ClassInfo<V> *vi = FIND_CLS(V, handler.classId);
        if(!vi) {
          V *vp = ClassTraits<V>::getSubstitute();
          if(vp) {
            tr->loadSubstitute<V>(*vp, handler.classId, handler.objectId);
            val.push_back(std::shared_ptr<V>(vp, handler));
          }
        }
        else {
          V *obj = tr->loadObject<V>(handler);
          if(obj) {
            if(inverse) ClassTraits<V>::get(*obj, inverse, tp);
            val.push_back(std::shared_ptr<V>(obj, handler));
          }
        }
      }
    }
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage template for vectors of pointers to mapped objects where the objects are directly serialized into the enclosing object's
 * buffer. The vector elements receive an object Id that is equal vector index + 1. This ID is thus not valid outside the vector.
 * Fully polymorphic
 *
 * Only the shallow object buffer is saved, non-embeddable members are ignored during serialization
 */
template<typename T, typename V> class ObjectPtrVectorPropertyStorageEmbedded : public StoreAccessBase
{
public:
  size_t size(ObjectBuf &buf) const override {
    unsigned vectSize = buf.readInteger<unsigned>(4);
    size_t sz = ClassTraits<V>::traits_properties->fixedSize;
    if(sz) {
      return vectSize * (sz + ClassId_sz + 4) + 4;
    }
    else {
      for(unsigned i=0; i<vectSize; i++) {
        buf.read(ClassId_sz);
        unsigned objSize = buf.readInteger<unsigned>(4);
        buf.read(objSize);
        sz += ClassId_sz + 4 + objSize;
      }
      return sz + 4;
    }
  }
  size_t size(void *obj, const PropertyAccessBase *pa) override {
    std::vector<std::shared_ptr<V>> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    size_t sz = ClassTraits<V>::traits_properties->fixedSize;
    if(sz) return val.size() * (sz + ClassId_sz + 4) + 4;

    for(std::shared_ptr<V> &v : val) {
      sz += ClassTraits<V>::bufferSize(&(*v)) + ClassId_sz + 4;
    }
    return sz + 4;
  }

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<std::shared_ptr<V>> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    tr->writeBuf().appendInteger((unsigned)val.size(), 4);
    PropertyId childObjectId = 0;
    for(std::shared_ptr<V> &v : val) {
      ClassId childClassId;
      size_t sz = ClassTraits<V>::bufferSize(&(*v), &childClassId);

      tr->writeBuf().appendInteger(childClassId, ClassId_sz);
      tr->writeBuf().appendInteger(sz, 4);
      tr->writeObject(childClassId, ++childObjectId, *v, ClassTraits<V>::getProperties(childClassId), true);
    }
  }

  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<std::shared_ptr<V>> val;

    PropertyId childObjectId = 0;

    unsigned len = buf.readInteger<unsigned>(4);
    for(size_t i=0; i<len; i++) {
      ClassId childClassId = buf.readInteger<ClassId>(ClassId_sz);
      unsigned sz = buf.readInteger<unsigned>(4);

      ClassInfo<V> *vi = FIND_CLS(V, childClassId);
      if(!vi) {
        buf.mark();
        V *vp = ClassTraits<V>::getSubstitute();
        if(vp) {
          readObject<V>(tr, buf, *vp, childClassId, ++childObjectId, StoreMode::force_buffer);
          val.push_back(std::shared_ptr<V>(vp));
        }
        buf.unmark(sz);
      }
      else {
        V *vp = vi->makeObject(childClassId);
        readObject<V>(tr, buf, childClassId, ++childObjectId, vp);
        val.push_back(std::shared_ptr<V>(vp));
      }
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage template for collection iterator members. The collection id is saved within the enclosing object's buffer.
 * Storing the collection proper is done externally, or through the iterator object
 */
template<typename T, typename V, typename KVIter, typename Iter>
struct CollectionIterPropertyStorage : public StoreAccessBase
{
  static_assert(std::is_base_of<KVPropertyBackend, KVIter>::value, "KVIter must subclass KVPropertyBackend");
  static_assert(std::is_base_of<Iter, KVIter>::value, "KVIter must subclass Iter");

  CollectionIterPropertyStorage() : StoreAccessBase(StoreLayout::all_embedded, ObjectId_sz) {}

  size_t size(ObjectBuf &buf) const override {return ObjectId_sz;}
  size_t size(void *obj, const PropertyAccessBase *pa) override {return ObjectId_sz;}

  void *initMember(void *obj, const PropertyAccessBase *pa) override
  {
    T *tp = reinterpret_cast<T *>(obj);
    KVIter *it = new KVIter();
    auto ib = std::shared_ptr<KVIter>(it);
    ClassTraits<T>::get(*tp, pa, ib);

    return static_cast<KVPropertyBackend *>(it);
  }

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::shared_ptr<Iter> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    IterPropertyBackendPtr ib = std::dynamic_pointer_cast<KVPropertyBackend>(val);
    tr->writeBuf().appendRaw(ib ? ib->getObjectId() : 0);
  }

  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    T *tp = reinterpret_cast<T *>(obj);
    ObjectId collectionId = buf.readRaw<ObjectId>();

    std::shared_ptr<KVIter> it = std::make_shared<KVIter>();
    it->setObjectId(collectionId);

    ClassTraits<T>::get(*tp, pa, it);
  }
};

template<typename T> struct PropertyStorage<T, short> : public BasePropertyStorage<T, short>{};
template<typename T> struct PropertyStorage<T, unsigned short> : public BasePropertyStorage<T, unsigned short>{};
template<typename T> struct PropertyStorage<T, int> : public BasePropertyStorage<T, int>{};
template<typename T> struct PropertyStorage<T, unsigned int> : public BasePropertyStorage<T, unsigned int>{};
template<typename T> struct PropertyStorage<T, long> : public BasePropertyStorage<T, long>{};
template<typename T> struct PropertyStorage<T, unsigned long> : public BasePropertyStorage<T, unsigned long>{};
template<typename T> struct PropertyStorage<T, long long> : public BasePropertyStorage<T, long long>{};
template<typename T> struct PropertyStorage<T, unsigned long long> : public BasePropertyStorage<T, unsigned long long>{};
template<typename T> struct PropertyStorage<T, float> : public BasePropertyStorage<T, float>{};
template<typename T> struct PropertyStorage<T, double> : public BasePropertyStorage<T, double>{};
template<typename T> struct PropertyStorage<T, bool> : public BasePropertyStorage<T, bool>{};
template<typename T> struct PropertyStorage<T, const char *> : public BasePropertyStorage<T, const char *>{};
template<typename T> struct PropertyStorage<T, std::string> : public BasePropertyStorage<T, std::string>{};

template<typename T> struct PropertyStorage<T, std::vector<short>> : public ValueVectorPropertyStorage<T, short>{};
template<typename T> struct PropertyStorage<T, std::vector<unsigned short>> : public ValueVectorPropertyStorage<T, unsigned short>{};
template<typename T> struct PropertyStorage<T, std::vector<int>> : public ValueVectorPropertyStorage<T, int>{};
template<typename T> struct PropertyStorage<T, std::vector<unsigned int>> : public ValueVectorPropertyStorage<T, unsigned int>{};
template<typename T> struct PropertyStorage<T, std::vector<long>> : public ValueVectorPropertyStorage<T, long>{};
template<typename T> struct PropertyStorage<T, std::vector<unsigned long>> : public ValueVectorPropertyStorage<T, unsigned long>{};
template<typename T> struct PropertyStorage<T, std::vector<long long>> : public ValueVectorPropertyStorage<T, long long>{};
template<typename T> struct PropertyStorage<T, std::vector<unsigned long long>> : public ValueVectorPropertyStorage<T, unsigned long long>{};
template<typename T> struct PropertyStorage<T, std::vector<float>> : public ValueVectorPropertyStorage<T, float>{};
template<typename T> struct PropertyStorage<T, std::vector<double>> : public ValueVectorPropertyStorage<T, double>{};
template<typename T> struct PropertyStorage<T, std::vector<bool>> : public ValueVectorPropertyStorage<T, bool>{};
template<typename T> struct PropertyStorage<T, std::vector<const char *>> : public ValueVectorPropertyStorage<T, const char *>{};
template<typename T> struct PropertyStorage<T, std::vector<std::string>> : public ValueVectorPropertyStorage<T, std::string>{};

template<typename T> struct PropertyStorage<T, std::set<short>> : public ValueSetPropertyStorage<T, short>{};
template<typename T> struct PropertyStorage<T, std::set<unsigned short>> : public ValueSetPropertyStorage<T, unsigned short>{};
template<typename T> struct PropertyStorage<T, std::set<int>> : public ValueSetPropertyStorage<T, int>{};
template<typename T> struct PropertyStorage<T, std::set<unsigned int>> : public ValueSetPropertyStorage<T, unsigned int>{};
template<typename T> struct PropertyStorage<T, std::set<long>> : public ValueSetPropertyStorage<T, long>{};
template<typename T> struct PropertyStorage<T, std::set<unsigned long>> : public ValueSetPropertyStorage<T, unsigned long>{};
template<typename T> struct PropertyStorage<T, std::set<long long>> : public ValueSetPropertyStorage<T, long long>{};
template<typename T> struct PropertyStorage<T, std::set<unsigned long long>> : public ValueSetPropertyStorage<T, unsigned long long>{};
template<typename T> struct PropertyStorage<T, std::set<float>> : public ValueSetPropertyStorage<T, float>{};
template<typename T> struct PropertyStorage<T, std::set<double>> : public ValueSetPropertyStorage<T, double>{};
template<typename T> struct PropertyStorage<T, std::set<bool>> : public ValueSetPropertyStorage<T, bool>{};
template<typename T> struct PropertyStorage<T, std::set<const char *>> : public ValueSetPropertyStorage<T, const char *>{};
template<typename T> struct PropertyStorage<T, std::set<std::string>> : public ValueSetPropertyStorage<T, std::string>{};

/**
 * mapping configuration for an ObjectId property
 */
template <typename O, ObjectId O::*p>
struct ObjectIdAssign : public PropertyAssign<O, ObjectId, p> {
  ObjectIdAssign()
      : PropertyAssign<O, ObjectId, p>("objectId", new ObjectIdStorage<O>(), PropertyType(0, 0, false)) {}

  void setup(Properties *props) const override {
    props->setKeyProperty(this);
  }
};
/**
 * mapping configuration for a property which holds another mapped Object by value. The referred-to object is
 * saved under a top-level key
 */
template <typename O, typename P, P O::*p>
struct ObjectPropertyAssign : public PropertyAccess<O, P> {
  ObjectPropertyAssign(const char * name)
      : PropertyAccess<O, P>(name, new ObjectPropertyStorage<O, P>(), object_t<P>()) {}
  void set(O &o, P val) const override { o.*p = val;}
  P get(O &o) const override { return o.*p;}
};
/**
 * mapping configuration for a property which holds another mapped Object by value. The referred-to object is
 * serialized into the enclsing object's buffer
 */
template <typename O, typename P, P O::*p>
struct ObjectPropertyEmbeddedAssign : public PropertyAccess<O, P> {
  ObjectPropertyEmbeddedAssign(const char * name)
      : PropertyAccess<O, P>(name, new ObjectPropertyStorageEmbedded<O, P>(), object_t<P>()) {}
  void set(O &o, P val) const override { o.*p = val;}
  P get(O &o) const override { return o.*p;}
};
/**
 * mapping configuration for a property which holds another mapped Object by shared_ptr. The referred-to object is
 * saved under a top-level key
 */
template <typename O, typename P, std::shared_ptr<P> O::*p>
struct ObjectPtrPropertyAssign : public PropertyAccess<O, std::shared_ptr<P>> {
  ObjectPtrPropertyAssign(const char * name)
      : PropertyAccess<O, std::shared_ptr<P>>(name, new ObjectPtrPropertyStorage<O, P>(), object_t<P>()) {}
  void set(O &o, std::shared_ptr<P> val) const override { o.*p = val;}
  std::shared_ptr<P> get(O &o) const override { return o.*p;}
};
/**
 * mapping configuration for a property which holds another mapped Object by shared_ptr. The referred-to object is
 * serialized into the enclosing object's buffer
 */
template <typename O, typename P, std::shared_ptr<P> O::*p>
struct ObjectPtrPropertyEmbeddedAssign : public PropertyAccess<O, std::shared_ptr<P>> {
  ObjectPtrPropertyEmbeddedAssign(const char * name)
      : PropertyAccess<O, std::shared_ptr<P>>(name, new ObjectPtrPropertyStorageEmbedded<O, P>(), object_t<P>()) {}
  void set(O &o, std::shared_ptr<P> val) const override { o.*p = val;}
  std::shared_ptr<P> get(O &o) const override { return o.*p;}
};
/**
 * mapping configuration for a property which holds a vector of mapped Objects by value. Referred-to objects are
 * saved under top-level keys
 */
template <typename O, typename P, std::vector<P> O::*p>
struct ObjectVectorPropertyAssign : public PropertyAssign<O, std::vector<P>, p> {
  ObjectVectorPropertyAssign(const char * name, bool lazy=false)
      : PropertyAssign<O, std::vector<P>, p>(name, new ObjectVectorPropertyStorage<O, P>(lazy), object_vector_t<P>()) {}
};
/**
 * mapping configuration for a property which holds a vector of mapped Objects by value. Referred-to objects are
 * serialized into the buffer of the enclosing object. They don't carry ObjectIds
 */
template <typename O, typename P, std::vector<P> O::*p>
struct ObjectVectorPropertyEmbeddedAssign : public PropertyAssign<O, std::vector<P>, p> {
  ObjectVectorPropertyEmbeddedAssign(const char * name)
      : PropertyAssign<O, std::vector<P>, p>(name, new ObjectVectorPropertyStorageEmbedded<O, P>(), object_vector_t<P>()) {}
};
/**
 * mapping configuration for a property which holds a vector of mapped Objects by shared_ptr. Referred-to objects are
 * saved under top-level keys
 */
template <typename O, typename P, std::vector<std::shared_ptr<P>> O::*p>
struct ObjectPtrVectorPropertyAssign : public PropertyAssign<O, std::vector<std::shared_ptr<P>>, p> {
  ObjectPtrVectorPropertyAssign(const char * name, bool lazy=false)
      : PropertyAssign<O, std::vector<std::shared_ptr<P>>, p>(name, new ObjectPtrVectorPropertyStorage<O, P>(lazy), object_vector_t<P>()) {}
};
/**
 * mapping configuration for a property which holds a vector of mapped Objects by shared_ptr. Referred-to objects are
 * serialized into the buffer of the enclosing object. They don't carry ObjectIds
 */
template <typename O, typename P, std::vector<std::shared_ptr<P>> O::*p>
struct ObjectPtrVectorPropertyEmbeddedAssign : public PropertyAssign<O, std::vector<std::shared_ptr<P>>, p> {
  ObjectPtrVectorPropertyEmbeddedAssign(const char * name)
      : PropertyAssign<O, std::vector<std::shared_ptr<P>>, p>(name, new ObjectPtrVectorPropertyStorageEmbedded<O, P>(), object_vector_t<P>()) {}
};
/**
 * mapping configuration for a property which holds the inverse pointer for a mapped object vector
 */
template <typename O, typename P, P * O::*p>
struct ObjectVectorInverseAssign : public PropertyAssign<O, P *, p> {
  ObjectVectorInverseAssign(const char * name, const char *inverse_name) : PropertyAssign<O, P *, p>(name, inverse_name, object_t<P>()) {}
};
/**
 * mapping configuration for a property which holds an object iterator. An object iterator has access to a collectionId which refers to
 * a top-level object collection.
 */
template <typename O, typename P, typename KVIter, typename Iter, std::shared_ptr<Iter> O::*p>
struct CollectionIterPropertyAssign : public PropertyAssign<O, std::shared_ptr<Iter>, p> {
  CollectionIterPropertyAssign(const char * name)
      : PropertyAssign<O, std::shared_ptr<Iter>, p>(name, new CollectionIterPropertyStorage<O, P, KVIter, Iter>(), object_vector_t<P>()) {}
};

} //kv
} //persistence
} //flexis

#endif //FLEXIS_FLEXIS_KVSTORE_H

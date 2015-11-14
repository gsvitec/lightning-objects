//
// Created by chris on 10/7/15.
//

#ifndef FLEXIS_FLEXIS_KVSTORE_H
#define FLEXIS_FLEXIS_KVSTORE_H

#include <string>
#include <typeinfo>
#include <memory>
#include <functional>
#include <unordered_map>
#include <persistence_error.h>
#include <FlexisPersistence_Export.h>
#include "kvtraits.h"

#define PROPERTY_ID(cls, name) ClassTraits<cls>::PropertyIds::name

namespace flexis {
namespace persistence {

using namespace kv;

static const ClassId COLLECTION_CLSID = 1;
static const ClassId COLLINFO_CLSID = 2;
static const ClassId CHUNKINFO_CLSID = 3;
static const size_t CHUNKSIZE = 1024 * 512; //default chunksize

class incompatible_schema_error : public persistence_error
{
public:
  incompatible_schema_error(std::string detail)
      : persistence_error("database is not compatible with current class schema", detail) {}
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

class FlexisPersistence_EXPORT KeyValueStoreBase
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
  };
  using PropertyMetaInfoPtr = std::shared_ptr<PropertyMetaInfo>;

  /**
   * check if class schema already exists. If so, check compatibility. If not, create
   * @throws incompatible_schema_error
   */
  void updateClassSchema(ClassInfo &classInfo, PropertyAccessBase * properties[], unsigned numProperties);

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
      ClassInfo &classInfo,
      PropertyAccessBase * currentProps[],
      unsigned numProps,
      std::vector<PropertyMetaInfoPtr> &propertyInfos) = 0;
};

using ReadTransactionPtr = std::shared_ptr<kv::ReadTransaction>;
using ExclusiveReadTransactionPtr = std::shared_ptr<kv::ExclusiveReadTransaction>;
using WriteTransactionPtr = std::shared_ptr<kv::WriteTransaction>;

using ObjectFactories = std::unordered_map<ClassId, std::function<void *()>>;
using ObjectProperties = std::unordered_map<ClassId, Properties *>;
using ObjectClassInfos = std::unordered_map<ClassId, ClassInfo *>;

/**
 * high-performance key/value store interface
 */
class FlexisPersistence_EXPORT KeyValueStore : public KeyValueStoreBase
{
  friend class kv::ReadTransaction;
  friend class kv::ExclusiveReadTransaction;
  friend class kv::WriteTransaction;

  //backward mapping from ClassId, used during polymorphic operations
  ObjectFactories objectFactories;
  ObjectProperties objectProperties;
  ObjectClassInfos objectClassInfos;

  std::unordered_map<const std::type_info *, ClassId> typeInfos;

protected:
  ObjectId m_maxCollectionId;
  bool m_reuseChunkspace = true;

public:
  /**
   * register a type for key/value persistence. It is assumed that a ClassTraits<type> implementation is visibly defined in the
   * current namespace. If this is the first call for this type, a ClassId and a ObjectId generator will be persistently
   * allocated.
   * Since this call determines the persistence mapping, care must be taken in case of class changes to ensure downward
   * compatibility for already stored class instance data
   */
  template <typename T>
  void registerType() {
    using Traits = ClassTraits<T>;

    unsigned index = 0;
    updateClassSchema(Traits::info, Traits::decl_props, ARRAY_SZ(Traits::decl_props));

    objectFactories[Traits::info.classId] = []() {return new T();};
    objectProperties[Traits::info.classId] = Traits::properties;
    objectClassInfos[Traits::info.classId] = &Traits::info;

    const std::type_info &ti = typeid(T);
    typeInfos[&ti] = Traits::info.classId;
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
   * @return a transaction object that allows reading + writing the database.
   * @throws InvalidArgumentException if in append mode the above prerequisites are not met
   * @throws persistence_error if write operations are currently blocked (beginRead(true))
   */
  virtual WriteTransactionPtr beginWrite(bool append=false) = 0;
};

namespace kv {

void readChunkHeader(const byte_t *data, size_t *dataSize, size_t *startIndex, size_t *elementCount);
void readChunkHeader(ReadBuf &buf, size_t *dataSize, size_t *startIndex, size_t *elementCount);
void readObjectHeader(ReadBuf &buf, ClassId *classId, ObjectId *objectId, size_t *size);

/**
 * custom deleter used to store the objectId inside a std::shared_ptr
 */
template <typename T> struct object_handler {
  const ObjectId objectId;

  object_handler(ObjectId objectId) : objectId(objectId) {}

  void operator () (T *t) {
    delete t;
  }
};

/**
 * Helper interface used by cursor, to be extended by implementors
 */
class FlexisPersistence_EXPORT CursorHelper {
  template <typename T, template <typename T> class Fact> friend class ClassCursor;

protected:
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
   * delete the object at the current cursor position. Cursor is not moved
   */
  virtual void erase() = 0;

  /**
   * @return the objectId of the item at the current cursor position
   */
  virtual ObjectId key() = 0;

  /**
   * close the cursor an release all resources
   */
  virtual void close() = 0;

  /**
   * read the data at the current cursor position into the key and buffer
   */
  virtual void get(StorageKey &key, ReadBuf &rb) = 0;

  /**
   * @return the data at the current cursor position
   */
  virtual const byte_t *getObjectData() = 0;
};

//non-polymorphic cursor object factory
template<typename T> struct SimpleFact
{
  T *makeObj(ClassId classId) {
    return new T();
  }
  Properties *properties(ClassId classId) {
    return ClassTraits<T>::properties;
  }
};

//polymorphic cursor object factory
template<typename T> class PolyFact
{
  ObjectFactories * const m_factories;
  ObjectProperties * const m_properties;

public:
  PolyFact(ObjectFactories *factories, ObjectProperties *properties) : m_factories(factories), m_properties(properties) {}

  T *makeObj(ClassId classId) {
    return (T *)m_factories->at(classId)();
  }
  Properties *properties(ClassId classId) {
    return m_properties->at(classId);
  }
};

struct ChunkInfo {
  PropertyId chunkId = 0;
  size_t startIndex = 0;
  size_t elementCount = 0;
  size_t dataSize = 0;

  ChunkInfo() {}
  ChunkInfo(PropertyId chunkId, size_t startIndex=0, size_t elementCount=0)
      : chunkId(chunkId), startIndex(startIndex), elementCount(elementCount) {}
  bool operator == (const ChunkInfo &other) {
    return chunkId == other.chunkId;
  }
  bool operator <= (const ChunkInfo &other) {
    return chunkId <= other.chunkId;
  }
};
struct CollectionInfo {
  ObjectId collectionId = 0;
  std::vector <ChunkInfo> chunkInfos;

  PropertyId nextChunkId = 0;
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
 * top-level collection cursor
 */
class CollectionCursorBase
{
protected:
  ChunkCursor::Ptr m_chunkCursor;
  ReadTransaction * const m_tr;
  const ObjectId m_collectionId;

  ReadBuf m_readBuf;
  size_t m_elementCount = 0, m_curElement = 0;

public:
  CollectionCursorBase(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr chunkCursor);
  bool atEnd();
  bool next();
};

/**
 * cursor for iterating over top-level object collections
 */
template <typename T, template <typename T> class Fact>
class ObjectCollectionCursor : public CollectionCursorBase
{
  const ClassId m_declClass;
  Fact<T> m_fact;

public:
  using Ptr = std::shared_ptr<ObjectCollectionCursor<T, Fact>>;

  ObjectCollectionCursor(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr chunkCursor, Fact<T> fact)
      : CollectionCursorBase(collectionId, tr, chunkCursor), m_declClass(ClassTraits<T>::info.classId), m_fact(fact)
  {}

  T *get()
  {
    ClassId classId;
    ObjectId objectId;
    readObjectHeader(m_readBuf, &classId, &objectId, 0);

    T *obj = m_fact.makeObj(classId);
    Properties *properties = m_fact.properties(classId);

    PropertyId propertyId = 0;
    for(unsigned px=0, sz=properties->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = properties->get(px);
      if(!p->enabled) continue;

      p->storage->load(m_tr, m_readBuf, m_declClass, objectId, obj, p);
    }
    return obj;
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
 * polymorphic collection cursor
 */
template<typename T> struct PolyCollectionCursor :public ObjectCollectionCursor<T, PolyFact> {
  PolyCollectionCursor(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr cc,
                       ObjectFactories *factories, ObjectProperties *properties)
      : ObjectCollectionCursor<T, PolyFact>(collectionId, tr, cc, PolyFact<T>(factories, properties)) {}
};
/**
 * non-polymorphic collection cursor
 */
template<typename T> struct SimpleCollectionCursor : public ObjectCollectionCursor<T, SimpleFact> {
  SimpleCollectionCursor(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr cc)
      : ObjectCollectionCursor<T, SimpleFact>(collectionId, tr, cc, SimpleFact<T>()) {}
};

/**
 * cursor for iterating over class objects (each with its own key)
 */
template <typename T, template <typename T> class Fact>
class ClassCursor
{
  ClassCursor(ClassCursor<T, Fact> &other) = delete;

  const ClassId m_classId;
  CursorHelper * const m_helper;
  ReadTransaction * const m_tr;
  Fact<T> m_fact;
  bool m_hasData;

public:
  using Ptr = std::shared_ptr<ClassCursor<T, Fact>>;

  ClassCursor(ClassId classId, CursorHelper *helper, ReadTransaction *tr, Fact<T> fact)
      : m_classId(classId), m_helper(helper), m_tr(tr), m_fact(fact)
  {
    m_hasData = helper->start();
  }

  virtual ~ClassCursor() {
    delete m_helper;
  };

  ObjectId key()
  {
    return m_helper->key();
  }

  void erase()
  {
    m_helper->erase();
  }

  /**
   * retrieve the address of the value of the given object property at the current cursor position. Note that
   * the address may point to database-owned memory and therefore must not be written to. It may also become
   * invalid after the end of the transaction.
   *
   * @param propertyId the property ID (1-based index into declared properties)
   * @param data (out) pointer to the property value. Note that the pointer will be invalid for Object pointer and
   * vector properties
   * @param buf (int/out) pointer to the object data buffer. If this pointer is non-null, the address of the object data buffer
   * will be stored there on the first call and reused on subsequent calls
   */
  void get(PropertyId propertyId, const byte_t **data, const byte_t **buf=nullptr)
  {
    using Traits = ClassTraits<T>;

    PropertyAccessBase *p = Traits::decl_props[propertyId-1];
    if(!p->enabled || p->type.isVector) {
      *data = nullptr;
      return;
    }

    //load class buffer
    const byte_t *dta;
    if(buf) {
      if(*buf) dta = *buf;
      else *buf = dta = m_helper->getObjectData();
    }
    else dta = m_helper->getObjectData();

    //calculate the buffer offset
    for(unsigned i=0, sz=Traits::properties->full_size(); i<sz; i++) {
      auto prop = Traits::properties->get(i);
      if(prop == p) {
        *data = dta;
        return;
      }
      dta += prop->storage->size(dta);
    }
  }

  /**
   * @param objId (out) the address to store the ObjectId
   * @return the ready instantiated object at the current cursor position
   */
  T *get(ObjectId *objId)
  {
    //load the data buffer
    ReadBuf readBuf;
    StorageKey key;
    m_helper->get(key, readBuf);

    //nothing here
    if(readBuf.empty()) return nullptr;

    T *obj = m_fact.makeObj(key.classId);
    Properties *properties = m_fact.properties(key.classId);

    if(objId) *objId = key.objectId;

    PropertyId propertyId = 0;
    for(unsigned px=0, sz=properties->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = properties->get(px);
      if(!p->enabled) continue;

      p->storage->load(m_tr, readBuf, m_classId, key.objectId, obj, p);
    }
    return obj;
  }

  /**
   * @return the ready instantiated object at the current cursor position. The shared_ptr also
   * contains the ObjectId
   */
  std::shared_ptr<T> get()
  {
    ObjectId id;
    T *obj = get(&id);
    return std::shared_ptr<T>(obj, object_handler<T>(id));
  }

  ClassCursor &operator++() {
    m_hasData = m_helper->next();
    return *this;
  }

  bool next() {
    m_hasData = m_helper->next();
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
 * polymorphic cursor
 */
template<typename T> struct PolyClassCursor :public ClassCursor<T, PolyFact> {
  PolyClassCursor(ClassId classId,
             CursorHelper *helper,
             ReadTransaction *tr, ObjectFactories *factories, ObjectProperties *properties)
      : ClassCursor<T, PolyFact>(classId, helper, tr, PolyFact<T>(factories, properties)) {}
};
/**
 * non-polymorphic cursor
 */
template<typename T> struct SimpleClassCursor : public ClassCursor<T, SimpleFact> {
  SimpleClassCursor(ClassId classId, CursorHelper *helper, ReadTransaction *tr)
      : ClassCursor<T, SimpleFact>(classId, helper, tr, SimpleFact<T>()) {}
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
  V *data() {return m_data;};
};

/**
 * Transaction that allows read operations only. Read transactions can be run concurrently
 */
class FlexisPersistence_EXPORT ReadTransaction
{
  template<typename T, typename V> friend class VectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPropertyStorage;
  template<typename T, typename V> friend class ObjectVectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrVectorPropertyStorage;
  friend class CollectionCursorBase;
  friend class CollectionAppenderBase;

protected:
  KeyValueStore &store;
  bool m_blockWrites;

  ReadTransaction(KeyValueStore &store) : store(store) {}

  void setBlockWrites(bool blockWrites) {
    m_blockWrites = blockWrites;
  }

  /**
   * @return the ObjectId which was (hopefully) stored with a custom deleter
   * @throws persistence_error if no ObjectId
   */
  template<typename T> ObjectId get_objectid(std::shared_ptr<T> obj)
  {
    object_handler<T> *ohm = std::get_deleter<object_handler<T>>(obj);
    if(!ohm) throw persistence_error("no objectId. Was shared_ptr read from the KV store?");
    return ohm->objectId;
  }

  template<typename T> T *readObject(ReadBuf &buf, ClassId classId, ObjectId objectId)
  {
    T *obj = (T *)store.objectFactories[classId]();
    Properties *props = store.objectProperties[classId];

    PropertyId propertyId = 0;
    for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = props->get(px);
      if(!p->enabled) continue;

      p->storage->load(this, buf, classId, objectId, obj, p);
    }
    return obj;
  }

  template<typename T> void readObject(ReadBuf &buf, T &obj, ClassId classId, ObjectId objectId)
  {
    Properties *props = ClassTraits<T>::properties;

    PropertyId propertyId = 0;
    for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = props->get(px);
      if(!p->enabled) continue;

      p->storage->load(this, buf, classId, objectId, &obj, p);
    }
  }

  /**
   * load an object from the KV store polymorphically, allocating the object on the heap.
   *
   * @param objectId the key generated by a previous call to WriteTransaction::putObject()
   * @return the object pointer, or nullptr if the key is not defined.
   */
  template<typename T> T *loadObject(ObjectId objectId)
  {
    ClassId classId = ClassTraits<T>::info.classId;

    //load the data buffer
    ReadBuf readBuf;
    getData(readBuf, classId, objectId, 0);

    //nothing here
    if(readBuf.empty()) return nullptr;

    return readObject<T>(readBuf, classId, objectId);
  }

  /**
   * load an object from the KV store polymorphically. Used by collections that store the classId
   *
   * @param classId the actual class id, which may be the id of a subclass of T
   * @param objectId the key generated by a previous call to WriteTransaction::putObject()
   * @return the object pointer, or nullptr if the key is not defined.
   */
  template<typename T> T *loadObject(ClassId classId, ObjectId objectId)
  {
    //load the data buffer
    ReadBuf readBuf;
    getData(readBuf, classId, objectId, 0);

    //nothing here
    if(readBuf.empty()) return nullptr;

    return readObject<T>(readBuf, classId, objectId);
  }

  /**
   * load an object from the KV store by value (non-polymorphically)
   *
   * @param objectId the key generated by a previous call to WriteTransaction::putObject()
   * @return true if the object was loaded
   */
  template<typename T> bool loadObject(ObjectId objectId, T &obj)
  {
    ClassId classId = ClassTraits<T>::info.classId;

    //load the data buffer
    ReadBuf readBuf;
    getData(readBuf, classId, objectId, 0);

    //nothing here
    if(readBuf.empty()) return false;

    readObject(readBuf, obj, classId, objectId);
    return true;
  }

  /**
   * read sub-object data into a buffer. Used internally by storage traits
   */
  virtual void getData(ReadBuf &buf, ClassId classId, ObjectId objectId, PropertyId propertyId) = 0;

  virtual CursorHelper * _openCursor(ClassId classId) = 0;
  virtual CursorHelper * _openCursor(ClassId classId, ObjectId objectId, PropertyId propertyId) = 0;
  virtual CursorHelper * _openCursor(ClassId classId, ObjectId collectionId) = 0;

  /**
   * @return the highes currently stored property ID for the given values
   */
  virtual PropertyId getMaxPropertyId(ClassId classId, ObjectId objectId) = 0;

  virtual bool getNextChunkInfo(ObjectId collectionId, PropertyId *propertyId, size_t *startindex) = 0;

  /**
   * retrieve info about a collection
   *
   * @param collectionId
   * @param loadNextInfo also get info for appending chunks
   * @return the collection info or nullptr
   */
  bool getCollectionInfo(ObjectId collectionId, CollectionInfo &collectionInfo, bool loadNextInfo);

  /**
   * @return a cursor ofer a chunked object (e.g., collection)
   */
  virtual ChunkCursor::Ptr _openChunkCursor(ClassId classId, ObjectId objectId, bool atEnd=false) = 0;

public:
  virtual ~ReadTransaction() {}

  /**
   * @return a cursor over all instances of the given class. The returned cursor is non-polymorphic
   */
  template <typename T> typename SimpleClassCursor<T>::Ptr openCursor() {
    using Traits = ClassTraits<T>;
    ClassId classId = Traits::info.classId;

    return typename SimpleClassCursor<T>::Ptr(new SimpleClassCursor<T>(classId, _openCursor(classId), this));
  }

  /**
   * @param obj a loaded object
   * @param propertyId the propertyId (1-based index into declared properties)
   * @return a cursor over the contents of a vector-valued, lazy-loading object property. The cursor will be empty if the
   * given property is not vector-valued. The cursor is polymorphic
   */
  template <typename T, typename V> typename PolyClassCursor<V>::Ptr openCursor(ObjectId objectId, T *obj, PropertyId propertyId) {
    ClassId t_classId = ClassTraits<T>::info.classId;
    ClassId v_classId = ClassTraits<V>::info.classId;

    return typename PolyClassCursor<V>::Ptr(
        new PolyClassCursor<V>(v_classId,
                          _openCursor(t_classId, objectId, propertyId),
                          this, &store.objectFactories, &store.objectProperties));
  }

  /**
   * @param collectionId the id of a top-level object collection
   * @return a cursor over the contents of the collection. The cursor is polymorphic
   */
  template <typename V> typename PolyCollectionCursor<V>::Ptr openCursor(ObjectId collectionId) {
    return typename PolyCollectionCursor<V>::Ptr(
        new PolyCollectionCursor<V>(collectionId, this, _openChunkCursor(COLLECTION_CLSID, collectionId),
                                    &store.objectFactories, &store.objectProperties));
  }

  /**
   * @param collectionId the id of a top-level collection
   * @return a cursor over the contents of the collection. The cursor is non-polymorphic
   */
  template <typename V> typename SimpleCollectionCursor<V>::Ptr openSimpleCursor(ObjectId collectionId) {
    return typename SimpleCollectionCursor<V>::Ptr(
        new SimpleCollectionCursor<V>(collectionId, this, _openChunkCursor(COLLECTION_CLSID, collectionId)));
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
   * load a top-level (chunked) object collection
   *
   * @param collectionId an id returned from a previous #putCollection call
   */
  template <typename T, template <typename T> class Ptr=std::shared_ptr>
  std::vector<Ptr<T>> getCollection(ObjectId collectionId)
  {
    std::vector<Ptr<T>> result;
    for(ChunkCursor::Ptr cc= _openChunkCursor(COLLECTION_CLSID, collectionId); !cc->atEnd(); cc->next()) {
      ReadBuf buf;
      cc->get(buf);

      size_t elementCount;
      readChunkHeader(buf, 0, 0, &elementCount);

      for(size_t i=0; i < elementCount; i++) {
        ClassId cid;
        ObjectId oid;
        readObjectHeader(buf, &cid, &oid, 0);

        T *obj = readObject<T>(buf, cid, oid);
        if(obj) result.push_back(std::shared_ptr<T>(obj));
        else
          throw persistence_error("collection object not found");
      }
    }
    return result;
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
   * load an object from the KV store, using the key generated by a previous call to WriteTransaction::putObject()
   *
   * @return the object, or nullptr if the key does not exist. The returned object is owned by the caller
   */
  template<typename T> T *getObject(ObjectId objectId)
  {
    return loadObject<T>(objectId);
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
  void loadMember(ObjectId objId, T &obj, PropertyId propertyId)
  {
    using Traits = ClassTraits<T>;

    ReadBuf rbuf;
    PropertyAccessBase *pa = Traits::decl_props[propertyId-1];
    pa->storage->load(this, rbuf, Traits::info.classId, objId, &obj, pa, true);
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
  void loadMember(std::shared_ptr<T> &obj, PropertyId propertyId)
  {
    using Traits = ClassTraits<T>;

    ObjectId objId = get_objectid(obj);

    ReadBuf rbuf;
    PropertyAccessBase *pa = Traits::decl_props[propertyId-1];
    pa->storage->load(this, rbuf, Traits::info.classId, objId, &obj, pa, true);
  }

  ClassId getClassId(const std::type_info &ti) {
    return store.typeInfos[&ti];
  }

  virtual void commit() = 0;
  virtual void abort() = 0;
};

#define DATA_API_ASSERT static_assert(TypeTraits<T>::byteSize == sizeof(T), \
"collection data access only supported for fixed-size types with native size equal byteSize");

/**
 * Transaction for exclusive read and operations. Opening write transactions while an exclusive read is open
 * will fail with an exception. Likewise creating an exclusive read transcation while a write is ongoing
 */
class FlexisPersistence_EXPORT ExclusiveReadTransaction : public virtual ReadTransaction
{
  virtual bool _getCollectionData(
      CollectionInfo &info, size_t startIndex, size_t length, std::shared_ptr<ValueTraitsBase>, void **data, bool *owned) = 0;

protected:
  ExclusiveReadTransaction(KeyValueStore &store) : ReadTransaction(store) {}

public:
  template <typename T> typename
  CollectionData<T>::Ptr getValueCollectionData(ObjectId collectionId, size_t startIndex, size_t endIndex)
  {
    DATA_API_ASSERT
    void *data;
    bool owned;
    CollectionInfo ci;
    if(!getCollectionInfo(collectionId, ci, false)) return nullptr;

    std::shared_ptr<ValueTraitsBase> vt = std::make_shared<ValueTraits<T>>();
    if(_getCollectionData(ci, startIndex, endIndex, vt, &data, &owned)) {
      return typename CollectionData<T>::Ptr(new CollectionData<T>(data, owned));
    }
    return nullptr;
  }
};

class CollectionAppenderBase
{
protected:
  ChunkCursor::Ptr m_chunkCursor;
  CollectionInfo m_collectionInfo;
  const size_t m_chunkSize;
  WriteTransaction * const m_wtxn;

  size_t m_elementCount, m_startIndex;
  PropertyId m_chunkId;

  CollectionAppenderBase(ChunkCursor::Ptr chunkCursor, WriteTransaction *wtxn, ObjectId collectionId, size_t chunkSize);
  void preparePut(size_t size);

public:
  void close();
};

/**
 * Transaction for read and write operations. Only one write transaction can be active at a time, and it should
 * be accessed from one thread only
 */
class FlexisPersistence_EXPORT WriteTransaction : public virtual ReadTransaction
{
  template<typename T, typename V> friend class BasePropertyStorage;
  template<typename T, typename V> friend class SimplePropertyStorage;
  template<typename T, typename V> friend class VectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPropertyStorage;
  template<typename T, typename V> friend class ObjectVectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrVectorPropertyStorage;
  friend class CollectionAppenderBase;

  /**
   * calculate shallow byte size, i.e. the size of the buffer required for properties that dont't get saved under
   * an individual key
   */
  template<typename T>
  static size_t calculateBuffer(T *obj, Properties *properties)
  {
    size_t size = 0;
    for(unsigned i=0, sz=properties->full_size(); i<sz; i++) {
      auto info = properties->get(i);

      if(!info->enabled) continue;

      //calculate variable size
      size += info->storage->size(obj, info);
    }
    return size;
  }
  WriteBuf writeBufStart;
  WriteBuf  *curBuf;

  void writeChunkHeader(size_t startIndex, size_t elementCount);
  void writeObjectHeader(ClassId classId, ObjectId objectId, size_t size);

  /**
   * start a new chunk by allocating memory from the KV store for it. Also write the chunk header for the
   * current chunk, if any
   *
   * @param ci
   * @param chunkId
   * @param chunkSize
   * @param elementCount the number of elements written to the current chunk. Used to write the header. If
   */
  bool startChunk(CollectionInfo &collectionInfo, PropertyId chunkId, size_t chunkSize, size_t startIndex, size_t elementCount);

protected:
  const bool m_append;

  WriteTransaction(KeyValueStore &store, bool append=false) : ReadTransaction(store), m_append(append) {
    curBuf = &writeBufStart;
  }

  bool reuseChunkspace() {
    return store.m_reuseChunkspace;
  }

  /**
   * non-polymorphic object removal
   */
  template <typename T>
  bool removeObject(ObjectId objectId, T &obj)
  {
    using Traits = ClassTraits<T>;
    ClassId classId = Traits::info.classId;

    //first kill all separately stored (vector) properties
    PropertyId propertyId = 0;
    for(unsigned px=0, sz=Traits::properties->full_size(); px < sz; px++) {
      PropertyAccessBase *p = Traits::properties->get(px);
      if(p->type.isVector) remove(classId, objectId, p->id);
    }
    //now remove the object proper
    remove(classId, objectId, 0);
  }

  /**
   * serialize the object to the write buffer
   */
  template <typename T>
  ObjectId writeObject(ClassId classId, ObjectId objectId, T &obj, Properties *properties, bool shallow)
  {
    //put data into buffer
    PropertyId propertyId = 0;
    for(unsigned px=0, sz=properties->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = properties->get(px);
      if(!p->enabled) continue;

      p->storage->save(this, classId, objectId, &obj, p);
    }
    return objectId;
  }


  /**
   * non-polymorphic save. Use in statically typed context
   *
   * @param id the object id
   * @param obj the object to save
   * @param newObject whether the object key needs to be generated
   * @param shallow skip vector properties that go to separate keys
   */
  template <typename T>
  ObjectId saveObject(ObjectId id, T &obj, bool newObject, bool shallow=false)
  {
    using Traits = ClassTraits<T>;

    ClassInfo &classInfo = Traits::info;
    ClassId classId = classInfo.classId;
    ObjectId objectId = newObject ? ++classInfo.maxObjectId : id;

    //create the data buffer
    size_t size = calculateBuffer(&obj, Traits::properties);
    writeBuf().start(size);

    writeObject(classId, objectId, obj, Traits::properties, shallow);

    if(!putData(classId, objectId, 0, writeBuf()))
      throw persistence_error("data was not saved");

    writeBuf().reset();

    return objectId;
  }

  /**
   * polymorphic save
   *
   * @param classId the actual classId of the object to save, possibly a subclass of T
   * @param id the object id
   * @param obj the object to save
   * @param newObject whether the object key needs to be generated
   */
  template <typename T>
  ObjectId saveObject(ClassId classId, ObjectId id, T &obj, bool newObject)
  {
    ClassInfo *classInfo = store.objectClassInfos.at(classId);
    if(!classInfo) throw persistence_error("class not registered");

    Properties *properties = store.objectProperties[classId];
    ObjectId objectId = newObject ? ++classInfo->maxObjectId : id;

    //create the data buffer
    size_t size = calculateBuffer(&obj, properties);
    writeBuf().start(size);

    writeObject(classId, objectId, obj, properties, false);

    if(!putData(classId, objectId, 0, writeBuf()))
      throw persistence_error("data was not saved");

    writeBuf().reset();

    return objectId;
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

  void putCollectionInfo(CollectionInfo &info, size_t startIndex, size_t elementCount);

  /**
   * save object collection chunks
   *
   * @param vect the collection
   * @param collectionId the id of the collection
   * @param chunkId the chunk index
   * @param chunkSize the chunk size in bytes
   * @param poly lookup classes dynamically (slight runtime overhead)
   */
  template <typename T, template <typename T> class Ptr>
  void saveChunks(const std::vector<Ptr<T>> &vect,
                  CollectionInfo collectionInfo, PropertyId chunkId, size_t chunkSize, size_t startIndex, bool poly)
  {
    if(!vect.empty()) startChunk(collectionInfo, chunkId, chunkSize, 0, 0);

    size_t elementCount = 0;
    for(size_t i=0, vectSize = vect.size(); i<vectSize; i++, elementCount++) {
      if(poly) {
        ClassId classId = getClassId(typeid(*vect[i]));
        ClassInfo *classInfo = store.objectClassInfos[classId];
        Properties *properties = store.objectProperties[classId];
        ObjectId objectId = ++classInfo->maxObjectId;

        size_t size = calculateBuffer(&(*vect[i]), properties) + ObjectHeader_sz;
        if(writeBuf().avail() < size) {
          if(elementCount == 0) throw persistence_error("chunk size too small");
          startChunk(collectionInfo, ++chunkId, chunkSize, startIndex, elementCount);
          startIndex += elementCount;
          elementCount = 0;
        }

        writeObjectHeader(classId, objectId, size);
        writeObject(classId, objectId, *vect[i], properties, true);
      }
      else {
        using Traits = ClassTraits<T>;

        ClassInfo &classInfo = Traits::info;
        ClassId classId = classInfo.classId;
        ObjectId objectId = ++classInfo.maxObjectId;

        size_t size = calculateBuffer(&(*vect[i]), Traits::properties) + ObjectHeader_sz;
        if(writeBuf().avail() < size) {
          if(elementCount == 0) throw persistence_error("chunk size too small");
          startChunk(collectionInfo, ++chunkId, chunkSize, startIndex, elementCount);
          startIndex += elementCount;
          elementCount = 0;
        }

        writeObjectHeader(classId, objectId, size);
        writeObject(classId, objectId, *vect[i], Traits::properties, true);
      }
    }
    if(elementCount) writeChunkHeader(startIndex, elementCount);
    putCollectionInfo(collectionInfo, startIndex, elementCount);
  }

  /**
   * save value collection chunks
   *
   * @param vect the collection
   * @param collectionId the id of the collection
   * @param chunkId the chunk index
   * @param chunkSize the chunk size in bytes
   */
  template <typename T>
  void saveChunks(const std::vector<T> &vect,
                  CollectionInfo ci, PropertyId chunkId, size_t chunkSize, size_t startIndex)
  {
    if(!vect.empty()) startChunk(ci, chunkId, chunkSize, 0, 0);

    size_t elementCount = 0;
    for(size_t i=0, vectSize = vect.size(); i<vectSize; i++, elementCount++) {
      if(writeBuf().avail() < ValueTraits<T>::size(vect[i])) {
        if(elementCount == 0) throw persistence_error("chunk size too small");
        startChunk(ci, ++chunkId, chunkSize, startIndex, elementCount);
        startIndex += elementCount;
        elementCount = 0;
      }
      ValueTraits<T>::putBytes(writeBuf(), vect[i]);
    }
    if(elementCount) writeChunkHeader(startIndex, elementCount);
    putCollectionInfo(ci, startIndex, elementCount);
  }

  /**
   * save value collection chunks
   *
   * @param vect the collection
   * @param collectionId the id of the collection
   * @param chunkId the chunk index
   * @param chunkSize the chunk size in bytes
   */
  template <typename T>
  void saveChunks(const T *array, size_t arraySize,
                  CollectionInfo &ci, PropertyId chunkId, size_t chunkSize, size_t startIndex)
  {
    if(arraySize) startChunk(ci, chunkId, chunkSize, 0, 0);

    size_t chunkEls = size_t(writeBuf().avail() / TypeTraits<T>::byteSize);
    size_t elementCount = chunkEls < arraySize ? chunkEls : arraySize;
    byte_t *data = (byte_t *)array;

    while(elementCount) {
      writeBuf().append(data, elementCount * TypeTraits<T>::byteSize);

      arraySize -= elementCount;
      if(arraySize == 0) break;

      startChunk(ci, ++chunkId, chunkSize, startIndex, elementCount);

      startIndex += elementCount;
      data += elementCount * TypeTraits<T>::byteSize;
      elementCount = arraySize >= chunkEls ? chunkEls : arraySize;
    }
    if(elementCount) writeChunkHeader(startIndex, elementCount);
    putCollectionInfo(ci, startIndex, elementCount);
  }

  /**
   * save a sub-object data buffer
   */
  virtual bool putData(ClassId classId, ObjectId objectId, PropertyId propertyId, WriteBuf &buf) = 0;

  /**
   * save a sub-object data buffer
   */
  virtual bool allocData(ClassId classId, ObjectId objectId, PropertyId propertyId, size_t size, byte_t **data) = 0;

  /**
   * remove an object from the KV store
   */
  virtual bool remove(ClassId classId, ObjectId objectId, PropertyId propertyId) = 0;

public:
  virtual ~WriteTransaction()
  {
    //assume all was popped
    curBuf->deleteChain();
  }

  /**
   * put a new object into the KV store. A key will be generated that consists of [class identifier/object identifier]. The
   * key, which will be unique within the class identifier, will be stored inside the shared_ptr
   *
   * @return the object identifier
   */
  template <typename T>
  std::shared_ptr<T> putObject(T *obj)
  {
    ObjectId oid = saveObject<T>(0, *obj, true);
    return std::shared_ptr<T>(obj, object_handler<T>(oid));
  }

  /**
   * put a new object into the KV store. A key will be generated that consists of [class identifier/object identifier]. The
   * object identifier will be unique within the class identifier.
   *
   * @return the object identifier
   */
  template <typename T>
  ObjectId putObject(T &obj)
  {
    return saveObject<T>(0, obj, true);
  }

  /**
   * put a new object into the KV store. A key will be generated that consists of [class identifier/object identifier]. The
   * object identifier will be unique within the class identifier.
   *
   * @return the object identifier
   */
  template <typename T>
  ObjectId putObjectP(T *obj)
  {
    return saveObject<T>(0, *obj, true);
  }

  /**
   * update an existing object in the KV store.
   *
   * @param objectId the object identifier as returned from a previous call to putObject
   * @param obj the object to update
   */
  template <typename T>
  void updateObject(ObjectId objectId, T &obj)
  {
    saveObject<T>(objectId, obj, false);
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
  void updateMember(ObjectId objId, T &obj, PropertyId propertyId)
  {
    using Traits = ClassTraits<T>;

    PropertyAccessBase *pa = Traits::decl_props[propertyId-1];
    if(pa->type.isVector)
      //vector goes to a separate key, no need to touch the object buffer
      pa->storage->save(this, Traits::info.classId, objId, &obj, pa, true);
    else
      //update the complete shallow buffer
      saveObject<T>(objId, obj, false, true);
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
  void updateMember(std::shared_ptr<T> &obj, PropertyId propertyId)
  {
    ObjectId objId = get_objectid(obj);
    updateMember(objId, *obj, propertyId);
  }

  /**
   * save a top-level (chunked) object collection.
   *
   * @param vect the collection contents
   * @param chunkSize size of chunk
   * @param poly emply polymorphic type resolution.
   */
  template <typename T, template <typename T> class Ptr>
  ObjectId putCollection(const std::vector<Ptr<T>> &vect, size_t chunkSize = CHUNKSIZE,
                         bool poly = true)
  {
    CollectionInfo ci(++store.m_maxCollectionId);

    saveChunks(vect, ci, 1, chunkSize, 0, poly);

    return ci.collectionId;
  }

  /**
   * save a top-level (chunked) value collection.
   *
   * @param vect the collection contents
   * @param chunkSize size of chunk
   */
  template <typename T>
  ObjectId putValueCollection(const std::vector<T> &vect, size_t chunkSize = CHUNKSIZE)
  {
    CollectionInfo ci(++store.m_maxCollectionId);

    saveChunks(vect, ci, 1, chunkSize, 0);

    return ci.collectionId;
  }

  /**
   * save a top-level (chunked) value collection.
   *
   * @param vect the collection contents
   * @param chunkSize size of chunk
   */
  template <typename T>
  ObjectId putValueCollectionData(const T* array, size_t arraySize, size_t chunkSize = CHUNKSIZE)
  {
    DATA_API_ASSERT
    CollectionInfo ci(++store.m_maxCollectionId);

    saveChunks(array, arraySize, ci, 1, chunkSize, 0);

    return ci.collectionId;
  }

  /**
   * append to a top-level (chunked) object collection.
   *
   * @param vect the collection contents
   * @param chunkSize size of chunk
   * @param poly emply polymorphic type resolution.
   */
  template <typename T, template <typename T> class Ptr>
  void appendCollection(ObjectId collectionId,
                        const std::vector<Ptr<T>> &vect, size_t chunkSize = CHUNKSIZE,
                        bool poly = true)
  {
    CollectionInfo ci;
    if(!getCollectionInfo(collectionId, ci, true)) throw persistence_error("collection not found");

    saveChunks(vect, collectionId, ci.nextChunkId, chunkSize, ci.nextStartIndex, poly);
  }

  /**
   * append to a top-level (chunked) value collection.
   *
   * @param vect the collection contents
   * @param chunkSize size of keys chunk
   */
  template <typename T>
  void appendValueCollection(ObjectId collectionId, const std::vector<T> &vect, size_t chunkSize = CHUNKSIZE)
  {
    CollectionInfo ci;
    if(!getCollectionInfo(collectionId, ci, true)) throw persistence_error("collection not found");

    saveChunks(vect, collectionId, ci.nextChunkId, chunkSize, ci.nextStartIndex);
  }

  /**
   * append to a top-level (chunked) value collection.
   *
   * @param vect the collection contents
   * @param chunkSize size of keys chunk
   */
  template <typename T>
  void appendValueCollectionData(ObjectId collectionId, const T *data, size_t dataSize, size_t chunkSize = CHUNKSIZE)
  {
    DATA_API_ASSERT
    CollectionInfo ci;
    if(!getCollectionInfo(collectionId, ci, true)) throw persistence_error("collection not found");

    saveChunks(data, dataSize, ci, ci.nextChunkId, chunkSize, ci.nextStartIndex);
  }

  template <typename T>
  void deleteObject(ObjectId objectId, T &obj) {
    removeObject(objectId, obj);
  }

  template <typename T>
  void deleteObject(std::shared_ptr<T> obj) {
    removeObject(get_objectid(obj), obj);
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

        ClassInfo *classInfo = m_objectClassInfos->at(cid);
        oid = ++classInfo->maxObjectId;
        properties = m_objectProperties->at(cid);
      }
      else {
        cid = ClassTraits<T>::info.classId;
        oid = ++ClassTraits<T>::info.maxObjectId;
        properties = ClassTraits<T>::properties;
      }
      size_t size = calculateBuffer(&obj, properties) + ObjectHeader_sz;
      preparePut(size);

      m_wtxn->writeObjectHeader(cid, oid, size);
      m_wtxn->writeObject(cid, oid, obj, properties, false);
    }

  public:
    using Ptr = std::shared_ptr<ObjectCollectionAppender>;

    ObjectCollectionAppender(ChunkCursor::Ptr chunkCursor, WriteTransaction *wtxn, ObjectId collectionId,
                             size_t chunkSize, ObjectClassInfos *objectClassInfos, ObjectProperties *objectProperties, bool poly)
        : CollectionAppenderBase(chunkCursor, wtxn, collectionId, chunkSize),
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

    ValueCollectionAppender(ChunkCursor::Ptr chunkCursor, WriteTransaction *wtxn, ObjectId collectionId, size_t chunkSize)
        : CollectionAppenderBase(chunkCursor, wtxn, collectionId, chunkSize)
    {}

    void put(T val)
    {
      size_t sz = TypeTraits<T>::byteSize;
      if(sz == 0) sz = ValueTraits<T>::size(val);

      preparePut(TypeTraits<T>::byteSize);
      ValueTraits<T>::putBytes(m_wtxn->writeBuf(), val);
    }
  };

  /**
   * create an appender for the given top-level object collection
   *
   * @param collectionId the id of a top-level collection
   * @param chunkSize the keychunk size
   * @param bool poly whether polymorphic type resolution should be employed
   * @return a writer over the contents of the collection.
   */
  template <typename V> typename ObjectCollectionAppender<V>::Ptr appendCollection(
      ObjectId collectionId, size_t chunkSize = CHUNKSIZE, bool poly = true)
  {
    return typename ObjectCollectionAppender<V>::Ptr(new ObjectCollectionAppender<V>(
        _openChunkCursor(COLLECTION_CLSID, collectionId, true),
        this, collectionId, chunkSize, &store.objectClassInfos, &store.objectProperties, poly));
  }

  /**
   * create an appender for the given top-level value collection
   *
   * @param collectionId the id of a top-level collection
   * @param chunkSize the keychunk size
   * @return a writer over the contents of the collection.
   */
  template <typename V> typename ValueCollectionAppender<V>::Ptr appendValueCollection(
      ObjectId collectionId, size_t chunkSize = CHUNKSIZE)
  {
    return typename ValueCollectionAppender<V>::Ptr(new ValueCollectionAppender<V>(
        _openChunkCursor(COLLECTION_CLSID, collectionId, true), this, collectionId, chunkSize));
  }
};

/**
 * storage trait for base types that go directly into the shallow buffer
 */
template<typename T, typename V>
struct BasePropertyStorage : public StoreAccessBase
{
  size_t size(const byte_t *buf) const override {
    return TypeTraits<V>::byteSize;
  }
  size_t size(void *obj, const PropertyAccessBase *pa) override {
    return TypeTraits<V>::byteSize;
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    ValueTraits<V>::putBytes(tr->writeBuf(), val);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ValueTraits<V>::getBytes(buf, val);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage trait for cstring, with dynamic size calculation (type.byteSize is 0). Note that upon loading,
 * the character data will reside in transaction memory and therefore may become invalid
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
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    const char * val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    ValueTraits<const char *>::putBytes(tr->writeBuf(), val);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    const char * val;
    T *tp = reinterpret_cast<T *>(obj);
    ValueTraits<const char *>::getBytes(buf, val);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage trait for string, with dynamic size calculation (type.byteSize is 0)
 */
template<typename T>
struct BasePropertyStorage<T, std::string> : public StoreAccessBase
{
  size_t size(const byte_t *buf) const override {
    return strlen((const char *)buf)+1;
  }
  size_t size(void *obj, const PropertyAccessBase *pa) override {
    std::string val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    return val.length() + 1;
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    std::string val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    ValueTraits<std::string>::putBytes(tr->writeBuf(), val);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    std::string val;
    T *tp = reinterpret_cast<T *>(obj);
    ValueTraits<std::string>::getBytes(buf, val);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage trait for value vector
 */
template<typename T, typename V>
struct VectorPropertyStorage : public StoreAccessPropertyKey
{
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    size_t psz;
    for(auto &v : val) psz += ValueTraits<V>::size(v);
    WriteBuf propBuf(psz);

    for(auto v : val) ValueTraits<V>::putBytes(propBuf, v);

    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
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
 * storage trait for mapped object references. Value-based, therefore non-polymorphic
 */
template<typename T, typename V> struct ObjectPropertyStorage : public StoreAccessEmbeddedKey
{
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    //save the value object
    ClassId childClassId = ClassTraits<T>::info.classId;
    tr->pushWriteBuf();
    ObjectId childId = tr->putObject<V>(val);
    tr->popWriteBuf();

    //save the key in this objects write buffer
    tr->writeBuf().append(childClassId, childId, 0);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    StorageKey sk;
    buf.read(sk);

    V *v = tr->getObject<V>(sk.objectId);
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, *v);

    //inefficient - delete the transport object, which was copied into the vector
    delete tp;
  }
};

/**
 * storage trait for mapped object references. Pointer-based, polymorphic
 */
template<typename T, typename V> class ObjectPtrPropertyStorage : public StoreAccessEmbeddedKey
{
  const bool m_lazy;

public:
  ObjectPtrPropertyStorage(bool lazy) : m_lazy(lazy) {}

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    if(m_lazy && !force) return;

    std::shared_ptr<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    ClassId childClassId = 0;
    ObjectId childId = 0;
    if(val) {
      //save the pointed-to object

      childClassId = tr->getClassId(typeid(*val));
      tr->pushWriteBuf();
      object_handler<V> *ohm = std::get_deleter<object_handler<V>>(val);
      if(ohm) {
        childId = ohm->objectId;
        tr->saveObject<V>(childClassId, childId, *val, false);
      }
      else {
        childId = tr->saveObject<V>(childClassId, 0, *val, true);
      }
      tr->popWriteBuf();

      //update the property so that it holds the object id
      std::shared_ptr<V> val2(nullptr, object_handler<V>(childId));
      val2.swap(val);
      ClassTraits<T>::get(*tp, pa, val2);
    }

    //save the key in this objects write buffer
    tr->writeBuf().append(childClassId, childId, 0);
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    if(m_lazy && !force) return;

    StorageKey sk;
    buf.read(sk);

    if(sk.classId > 0) {
      V *v = tr->getObject<V>(sk.objectId);
      std::shared_ptr<V> vp = std::shared_ptr<V>(v, object_handler<V>(sk.objectId));
      T *tp = reinterpret_cast<T *>(obj);
      ClassTraits<T>::get(*tp, pa, vp);
    }
  }
};

/**
 * storage trait for mapped object vectors. Value-based, therefore not polymorphic
 */
template<typename T, typename V> class ObjectVectorPropertyStorage : public StoreAccessPropertyKey
{
  bool m_lazy;
public:
  ObjectVectorPropertyStorage(bool lazy) : m_lazy(lazy) {}

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    if(m_lazy && !force) return;

    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    size_t psz = StorageKey::byteSize * val.size();
    WriteBuf propBuf(psz);

    tr->pushWriteBuf();
    ClassId childClassId = ClassTraits<V>::info.classId;
    for(V &v : val) {
      ObjectId childId = tr->putObject<V>(v);

      propBuf.append(childClassId, childId, 0);
    }
    tr->popWriteBuf();

    //vector goes into a separate property key
    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");
  }

  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    if(m_lazy && !force) return;

    std::vector<V> val;

    //load vector base (array of object keys) data from property key
    ReadBuf readBuf;
    tr->getData(readBuf, classId, objectId, pa->id);

    if(!readBuf.empty()) {
      StorageKey sk;
      while(readBuf.read(sk)) {
        V obj;
        tr->loadObject(sk.objectId, obj);
        val.push_back(obj);
      }
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage trait for mapped object pointer vectors. Polymorphic
 */
template<typename T, typename V> class ObjectPtrVectorPropertyStorage : public StoreAccessPropertyKey
{
  bool m_lazy;

public:
  ObjectPtrVectorPropertyStorage(bool lazy) : m_lazy(lazy) {}

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    if(m_lazy && !force) return;

    std::vector<std::shared_ptr<V>> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    size_t psz = StorageKey::byteSize * val.size();
    WriteBuf propBuf(psz);

    tr->pushWriteBuf();
    for(std::shared_ptr<V> &v : val) {
      ClassId childClassId = tr->getClassId(typeid(*v));

      ObjectId childId;
      object_handler<V> *ohm = std::get_deleter<object_handler<V>>(v);
      if(ohm) {
        childId = ohm->objectId;
        tr->saveObject<V>(childClassId, childId, *v, false);
      }
      else {
        childId = tr->saveObject<V>(childClassId, 0, *v, true);
      }

      propBuf.append(childClassId, childId, 0);
    }
    tr->popWriteBuf();

    //vector goes into a separate property key
    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");
  }

  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    if(m_lazy && !force) return;

    std::vector<std::shared_ptr<V>> val;

    //load vector base (array of object keys) data from property key
    ReadBuf readBuf;
    tr->getData(readBuf, classId, objectId, pa->id);

    if(!readBuf.empty()) {
      StorageKey sk;
      while(readBuf.read(sk)) {
        V *obj = tr->loadObject<V>(sk.classId, sk.objectId);
        val.push_back(std::shared_ptr<V>(obj, object_handler<V>(sk.objectId)));
      }
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

template<typename T> struct PropertyStorage<T, short> : public BasePropertyStorage<T, short>{};
template<typename T> struct PropertyStorage<T, unsigned short> : public BasePropertyStorage<T, unsigned short>{};
template<typename T> struct PropertyStorage<T, int> : public BasePropertyStorage<T, long>{};
template<typename T> struct PropertyStorage<T, unsigned int> : public BasePropertyStorage<T, unsigned long>{};
template<typename T> struct PropertyStorage<T, long> : public BasePropertyStorage<T, long>{};
template<typename T> struct PropertyStorage<T, unsigned long> : public BasePropertyStorage<T, unsigned long>{};
template<typename T> struct PropertyStorage<T, long long> : public BasePropertyStorage<T, long long>{};
template<typename T> struct PropertyStorage<T, unsigned long long> : public BasePropertyStorage<T, unsigned long long>{};
template<typename T> struct PropertyStorage<T, float> : public BasePropertyStorage<T, float>{};
template<typename T> struct PropertyStorage<T, double> : public BasePropertyStorage<T, double>{};
template<typename T> struct PropertyStorage<T, bool> : public BasePropertyStorage<T, bool>{};
template<typename T> struct PropertyStorage<T, const char *> : public BasePropertyStorage<T, const char *>{};
template<typename T> struct PropertyStorage<T, std::string> : public BasePropertyStorage<T, std::string>{};

template<typename T> struct PropertyStorage<T, std::vector<short>> : public VectorPropertyStorage<T, short>{};
template<typename T> struct PropertyStorage<T, std::vector<unsigned short>> : public VectorPropertyStorage<T, unsigned short>{};
template<typename T> struct PropertyStorage<T, std::vector<long>> : public VectorPropertyStorage<T, long>{};
template<typename T> struct PropertyStorage<T, std::vector<unsigned long>> : public VectorPropertyStorage<T, unsigned long>{};
template<typename T> struct PropertyStorage<T, std::vector<long long>> : public VectorPropertyStorage<T, long long>{};
template<typename T> struct PropertyStorage<T, std::vector<unsigned long long>> : public VectorPropertyStorage<T, unsigned long long>{};
template<typename T> struct PropertyStorage<T, std::vector<float>> : public VectorPropertyStorage<T, float>{};
template<typename T> struct PropertyStorage<T, std::vector<double>> : public VectorPropertyStorage<T, double>{};
template<typename T> struct PropertyStorage<T, std::vector<bool>> : public VectorPropertyStorage<T, bool>{};
template<typename T> struct PropertyStorage<T, std::vector<const char *>> : public VectorPropertyStorage<T, const char *>{};
template<typename T> struct PropertyStorage<T, std::vector<std::string>> : public VectorPropertyStorage<T, std::string>{};

template <typename O, typename P, std::shared_ptr<P> O::*p>
struct ObjectPropertyAssign : public PropertyAccess<O, P> {
  ObjectPropertyAssign(const char * name)
      : PropertyAccess<O, P>(name, new ObjectPropertyStorage<O, P>(), object_t<P>()) {}
  void set(O &o, P val) const override { o.*p = val;}
  P get(O &o) const override { return o.*p;}
};
template <typename O, typename P, std::shared_ptr<P> O::*p>
struct ObjectPtrPropertyAssign : public PropertyAccess<O, std::shared_ptr<P>> {
  ObjectPtrPropertyAssign(const char * name, bool lazy=false)
      : PropertyAccess<O, std::shared_ptr<P>>(name, new ObjectPtrPropertyStorage<O, P>(lazy), object_t<P>()) {}
  void set(O &o, std::shared_ptr<P> val) const override { o.*p = val;}
  std::shared_ptr<P> get(O &o) const override { return o.*p;}
};

template <typename O, typename P, std::vector<P> O::*p>
struct ObjectVectorPropertyAssign : public PropertyAssign<O, std::vector<P>, p> {
  ObjectVectorPropertyAssign(const char * name, bool lazy=false)
      : PropertyAssign<O, std::vector<P>, p>(name, new ObjectVectorPropertyStorage<O, P>(lazy), object_vector_t<P>()) {}
};
template <typename O, typename P, std::vector<std::shared_ptr<P>> O::*p>
struct ObjectPtrVectorPropertyAssign : public PropertyAssign<O, std::vector<std::shared_ptr<P>>, p> {
  ObjectPtrVectorPropertyAssign(const char * name, bool lazy=false)
      : PropertyAssign<O, std::vector<std::shared_ptr<P>>, p>(name, new ObjectPtrVectorPropertyStorage<O, P>(lazy), object_vector_t<P>()) {}
};

} //kv

} //persistence
} //flexis

#endif //FLEXIS_FLEXIS_KVSTORE_H

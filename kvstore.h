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
static const size_t DEFAULT_OBJECTS_CHUNKSIZE = 1024 * 1024; //1 MB

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

using TransactionPtr = std::shared_ptr<kv::ReadTransaction>;
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
  friend class kv::WriteTransaction;

  //backward mapping from ClassId, used during polymorphic operations
  ObjectFactories objectFactories;
  ObjectProperties objectProperties;
  ObjectClassInfos objectClassInfos;

  std::unordered_map<const std::type_info *, ClassId> typeInfos;

protected:
  ObjectId m_maxCollectionId;

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
  virtual TransactionPtr beginRead() = 0;

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
   */
  virtual WriteTransactionPtr beginWrite(bool append=false) = 0;
};

namespace kv {

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
  virtual const char *getObjectData() = 0;
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
 * cursor for iterating over top-level collections
 */
template <typename T, template <typename T> class Fact>
class CollectionCursor
{
  const ClassId m_declClass;
  ChunkCursor::Ptr m_chunkCursor;
  ReadTransaction * const m_tr;
  const ObjectId m_collectionId;
  Fact<T> m_fact;

  ReadBuf m_readBuf;
  size_t m_elementCount = 0, m_curElement = 0;

public:
  using Ptr = std::shared_ptr<CollectionCursor<T, Fact>>;

  CollectionCursor(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr chunkCursor, Fact<T> fact)
      : m_declClass(ClassTraits<T>::info.classId), m_collectionId(collectionId), m_tr(tr), m_chunkCursor(chunkCursor), m_fact(fact)
  {
    if(!m_chunkCursor->atEnd()) {
      m_chunkCursor->get(m_readBuf);
      m_elementCount = m_readBuf.readInteger<size_t>(4);
    }
  }

  bool atEnd() {
    return m_curElement >= m_elementCount;
  }

  bool next()
  {
    if(++m_curElement == m_elementCount && m_chunkCursor->next()) {
      m_chunkCursor->get(m_readBuf);
      m_elementCount = m_readBuf.readInteger<size_t>(4);
      m_curElement = 0;
    }
    return m_curElement < m_elementCount;
  }

  T *get()
  {
    ClassId classId = m_readBuf.readInteger<ClassId>(ClassId_sz);
    ObjectId objectId = m_readBuf.readInteger<ObjectId>(ObjectId_sz);

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
 * polymorphic collection cursor
 */
template<typename T> struct PolyCollectionCursor :public CollectionCursor<T, PolyFact> {
  PolyCollectionCursor(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr cc,
                       ObjectFactories *factories, ObjectProperties *properties)
      : CollectionCursor<T, PolyFact>(collectionId, tr, cc, PolyFact<T>(factories, properties)) {}
};
/**
 * non-polymorphic collection cursor
 */
template<typename T> struct SimpleCollectionCursor : public CollectionCursor<T, SimpleFact> {
  SimpleCollectionCursor(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr cc)
      : CollectionCursor<T, SimpleFact>(collectionId, tr, cc, SimpleFact<T>()) {}
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
  void get(PropertyId propertyId, const char **data, const char **buf=nullptr)
  {
    using Traits = ClassTraits<T>;

    PropertyAccessBase *p = Traits::decl_props[propertyId-1];
    if(!p->enabled || p->type.isVector) {
      *data = nullptr;
      return;
    }

    //load class buffer
    const char *dta;
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
      if(prop->type.isVector) {
        //vector is stored as separate key
      }
      else if(prop->type.byteSize) {
        dta += prop->type.byteSize;
      }
      else
        //calculate variable size
        dta += prop->storage->size(dta);
    }
  }

  /**
   * @return the ready instantiated object at the current cursor position
   */
  T *get(ObjectId *objId=nullptr)
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
 * Transaction that allows read operations only. Read transactions can be run concurrently
 */
class FlexisPersistence_EXPORT ReadTransaction
{
  template<typename T, typename V> friend class VectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPropertyStorage;
  template<typename T, typename V> friend class ObjectVectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrVectorPropertyStorage;
  template <typename T> friend class CollectionAppender;

protected:
  KeyValueStore &store;
  ReadTransaction(KeyValueStore &store) : store(store) {}

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

  /**
   * load an object from the KV store, allocating the object on the heap
   *
   * @param objectId the key generated by a previous call to WriteTransaction::putObject()
   * @return the object pointer, or nullptr if the key is not defined.
   */
  template<typename T> T *loadObject(ObjectId objectId)
  {
    using Traits = ClassTraits<T>;
    ClassId classId = Traits::info.classId;

    T *obj = (T *)store.objectFactories[classId]();

    //load the data buffer
    ReadBuf readBuf;
    getData(readBuf, classId, objectId, 0);

    //nothing here
    if(readBuf.empty()) return nullptr;

    PropertyId propertyId = 0;
    for(unsigned px=0, sz=Traits::properties->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = Traits::properties->get(px);
      if(!p->enabled) continue;

      p->storage->load(this, readBuf, classId, objectId, obj, p);
    }
    return obj;
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

  /**
   * load an object from the KV store polymorphically
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
   * @param collectionId the id of a top-level collection
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
   * load a top-level (chunked) collection
   *
   * @param collectionId an id returned from a previous #putCollection call
   */
  template <typename T> std::vector<std::shared_ptr<T>> getCollection(ObjectId collectionId)
  {
    std::vector<std::shared_ptr<T>> result;
    for(ChunkCursor::Ptr cc= _openChunkCursor(COLLECTION_CLSID, collectionId); !cc->atEnd(); cc->next()) {
      ReadBuf buf;
      cc->get(buf);

      size_t elementCount = buf.readInteger<size_t>(4);
      for(size_t i=0; i < elementCount; i++) {
        ClassId cid = buf.readInteger<ClassId>(ClassId_sz);
        ObjectId oid = buf.readInteger<ObjectId>(ObjectId_sz);

        T *obj = readObject<T>(buf, cid, oid);
        if(obj) result.push_back(std::shared_ptr<T>(obj));
        else
          throw persistence_error("collection object not found");
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

/**
 * Transaction for read and write operations. Only one write transaction can be active at a time, and it should
 * be accessed from one thread only
 */
class FlexisPersistence_EXPORT WriteTransaction : public ReadTransaction
{
  template<typename T, typename V> friend class BasePropertyStorage;
  template<typename T, typename V> friend class SimplePropertyStorage;
  template<typename T, typename V> friend class VectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPropertyStorage;
  template<typename T, typename V> friend class ObjectVectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrVectorPropertyStorage;
  template <typename T> friend class CollectionAppender;

  /**
   * calculate shallow byte size, i.e. the size of the buffer required for properties that dont't get saved under
   * an individual key
   */
  template<typename T>
  size_t calculateBuffer(T *obj, Properties *properties)
  {
    size_t size = 0;
    for(unsigned i=0, sz=properties->full_size(); i<sz; i++) {
      auto info = properties->get(i);

      if(!info->enabled) continue;

      if(info->type.isVector) {
        //vector is stored as separate key
      }
      else if(info->type.byteSize) {
        size += info->type.byteSize;
      }
      else
        //calculate variable size
        size += info->storage->size(obj, info);
    }
    return size;
  }
  WriteBuf writeBufStart;
  WriteBuf  *curBuf;

protected:
  const bool m_append;

  WriteTransaction(KeyValueStore &store, bool append=false) : ReadTransaction(store), m_append(append) {
    curBuf = &writeBufStart;
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
    ClassInfo *classInfo = store.objectClassInfos[classId];
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

  /**
   * save collection chunks
   *
   * @param vect the collection
   * @param collectionId the id of the collection
   * @param elementClassId the classid of the collection element type (elements may be subclasses)
   * @param chunkId the chunk index
   * @param chunkSize the chunk size
   * @param vectIndex the running index into the collection
   * @param poly lookup classIds dynamically
   */
  template <typename T>
  void saveChunks(const std::vector<std::shared_ptr<T>> &vect,
                 ObjectId collectionId, PropertyId chunkId, size_t chunkSize, bool poly)
  {
    auxbuffer_t buf;
    buf.resize(4); //reserve bytes for size

    writeBuf().start(buf);

    size_t elementCount = 0;
    for(size_t i=0, vectSize = vect.size(); i<vectSize; i++) {
      if(poly) {
        ClassId classId = getClassId(typeid(*vect[i]));
        ClassInfo *classInfo = store.objectClassInfos[classId];
        Properties *properties = store.objectProperties[classId];
        ObjectId objectId = ++classInfo->maxObjectId;

        char * hdr = writeBuf().allocate(ClassId_sz + ObjectId_sz);
        write_integer<ClassId>(hdr, classId, ClassId_sz);
        write_integer<ObjectId>(hdr+ClassId_sz, objectId, ObjectId_sz);

        writeObject(classId, objectId, *vect[i], properties, true);
      }
      else {
        using Traits = ClassTraits<T>;

        ClassInfo &classInfo = Traits::info;
        ClassId classId = classInfo.classId;
        ObjectId objectId = ++classInfo.maxObjectId;

        char * hdr = writeBuf().allocate(ClassId_sz + ObjectId_sz);
        write_integer<ClassId>(hdr, classId, ClassId_sz);
        write_integer<ObjectId>(hdr+ClassId_sz, objectId, ObjectId_sz);

        writeObject(classId, objectId, *vect[i], Traits::properties, true);
      }
      if(buf.size() >= chunkSize) {
        write_integer(&buf[0], elementCount + 1, 4);
        putData(COLLECTION_CLSID, collectionId, ++chunkId, writeBuf());
        writeBuf().reset();
        buf.resize(4); //reserve bytes for size
        elementCount = 0;
      }
      else elementCount++;
    }
    if(elementCount) {
      write_integer(&buf[0], elementCount, 4);
      putData(COLLECTION_CLSID, collectionId, ++chunkId, writeBuf());
      writeBuf().reset();
    }
  }

  /**
   * save a sub-object data buffer
   */
  virtual bool putData(ClassId classId, ObjectId objectId, PropertyId propertyId, WriteBuf &buf) = 0;

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
   * save a top-level (chunked) collection.
   *
   * @param vect the collection contents
   * @param chunkSize size of keys chunk
   * @param poly emply polymorphic type resolution.
   */
  template <typename T>
  ObjectId putCollection(const std::vector<std::shared_ptr<T>> &vect, size_t chunkSize = DEFAULT_OBJECTS_CHUNKSIZE,
                         bool poly = true)
  {
    ObjectId collectionId = ++store.m_maxCollectionId;

    saveChunks(vect, collectionId, 0, chunkSize, poly);

    return collectionId;
  }

  /**
   * append to a top-level (chunked) collection.
   *
   * @param vect the collection contents
   * @param chunkSize size of keys chunk
   * @param poly emply polymorphic type resolution.
   */
  template <typename T>
  void appendCollection(ObjectId collectionId,
                        const std::vector<std::shared_ptr<T>> &vect, size_t chunkSize = DEFAULT_OBJECTS_CHUNKSIZE,
                        bool poly = true)
  {
    PropertyId maxChunk = getMaxPropertyId(COLLECTION_CLSID, collectionId) + PropertyId(1);
    saveChunks(vect, collectionId, maxChunk, chunkSize, poly);
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
  * appender for sequentially extending a top-level, chunked collection
  */
  template <typename T>
  class CollectionAppender
  {
    ChunkCursor::Ptr m_chunkCursor;
    const ObjectId m_collectionId;
    const size_t m_chunkSize;
    const bool m_poly;
    ObjectClassInfos * const m_objectClassInfos;
    ObjectProperties * const m_objectProperties;

    WriteTransaction * const m_wtxn;

    WriteBuf &m_writeBuf;
    auxbuffer_t m_auxbuf;

    size_t m_elementCount;
    bool m_writePending;
    PropertyId m_chunkId;

  public:
    using Ptr = std::shared_ptr<CollectionAppender>;

    CollectionAppender(ChunkCursor::Ptr chunkCursor, WriteTransaction *wtxn, ObjectId collectionId,
                       size_t chunkSize, ObjectClassInfos *objectClassInfos, ObjectProperties *objectProperties, bool poly)
        : m_chunkCursor(chunkCursor), m_chunkSize(chunkSize), m_wtxn(wtxn), m_collectionId(collectionId),
          m_objectClassInfos(objectClassInfos), m_objectProperties(objectProperties), m_poly(poly),
          m_writeBuf(m_wtxn->writeBuf())
    {
      m_auxbuf.resize(4); //reserve bytes for size

      m_writeBuf.start(m_auxbuf);
      m_writePending = false;

      if(!m_chunkCursor->atEnd()) {
        ReadBuf rb;
        m_chunkCursor->get(rb);

        if(rb.size() < m_chunkSize) {
          m_elementCount = rb.readInteger<size_t>(4);
          m_writeBuf.append(rb.cur(), rb.size()-4);
          m_chunkId = m_chunkCursor->chunkId();
        }
        else
          m_chunkId = m_chunkCursor->chunkId()+PropertyId(1);
      }
      else m_chunkId = m_wtxn->getMaxPropertyId(COLLECTION_CLSID, collectionId);
    }

    void put(std::shared_ptr<T> obj)
    {
      ClassId cid;
      ObjectId oid;

      m_elementCount++;
      Properties *properties;
      if(m_poly) {
        cid = m_wtxn->getClassId(typeid(*obj));

        ClassInfo *classInfo = m_objectClassInfos->at(cid);
        oid = ++classInfo->maxObjectId;
        properties = m_objectProperties->at(cid);
      }
      else {
        cid = ClassTraits<T>::info.classId;
        oid = ++ClassTraits<T>::info.maxObjectId;
        properties = ClassTraits<T>::properties;
      }
      char * hdr = m_writeBuf.allocate(ClassId_sz + ObjectId_sz);
      write_integer<ClassId>(hdr, cid, ClassId_sz);
      write_integer<ObjectId>(hdr+ClassId_sz, oid, ObjectId_sz);
      m_wtxn->writeObject(cid, oid, *obj, properties, false);

      if(m_writeBuf.size() >= m_chunkSize) {
        write_integer(&m_auxbuf[0], m_elementCount, 4);
        m_wtxn->putData(COLLECTION_CLSID, m_collectionId, m_chunkId, m_writeBuf);

        m_auxbuf.resize(4); //reserve bytes for size
        m_writeBuf.start(m_auxbuf);
        m_chunkId++;
        m_elementCount = 0;
        m_writePending = false;
      }
      else m_writePending = true;
    }

    void close() {
      if(m_writePending) {
        write_integer(m_writeBuf.data(), m_elementCount, 4); //chunk header: size
        m_wtxn->putData(COLLECTION_CLSID, m_collectionId, m_chunkId, m_writeBuf);
      }
      m_chunkCursor->close();
    }
  };

  /**
   * create an appender for the given top-level collection
   *
   * @param collectionId the id of a top-level collection
   * @param chunkSize the keychunk size
   * @param bool poly whether polymorphic type resolution should be employed
   * @return a writer over the contents of the collection.
   */
  template <typename V> typename CollectionAppender<V>::Ptr appendCollection(
      ObjectId collectionId, size_t chunkSize = DEFAULT_OBJECTS_CHUNKSIZE, bool poly = true)
  {
    return typename CollectionAppender<V>::Ptr(new CollectionAppender<V>(
        _openChunkCursor(COLLECTION_CLSID, collectionId, true),
        this, collectionId, chunkSize, &store.objectClassInfos, &store.objectProperties, poly));
  }
};

/**
 * storage trait for base types that go directly into the shallow buffer
 */
template<typename T, typename V>
struct BasePropertyStorage : public StoreAccessBase
{
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
  size_t size(const char *buf) override {
    return strlen(buf) + 1;
  };
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
  size_t size(const char *buf) override {
    return strlen(buf) + 1;
  };
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
struct VectorPropertyStorage : public StoreAccessBase {
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>(*tp).put(pa, val);

    size_t psz = TypeTraits<V>::pt().byteSize * val.size();
    WriteBuf propBuf(psz);

    using ElementTraits = ValueTraits<V>;
    for(auto v : val) ElementTraits::putBytes(propBuf, v);

    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    std::vector<V> val;

    ReadBuf readBuf;
    tr->getData(readBuf, classId, objectId, pa->id);

    if(!readBuf.empty()) {
      V v;
      ValueTraits<V>::getBytes(readBuf, v);
      val.push_back(v);
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>(*tp).get(pa, val);
  }
};

/**
 * storage trait for mapped object references. Value-based, therefore non-polymorphic
 */
template<typename T, typename V> struct ObjectPropertyStorage : public StoreAccessBase
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
template<typename T, typename V> class ObjectPtrPropertyStorage : public StoreAccessBase
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

      ClassId childClassId = tr->getClassId(typeid(*val));
      tr->pushWriteBuf();
      ObjectId childId;
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
template<typename T, typename V> class ObjectVectorPropertyStorage : public StoreAccessBase
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
    ClassId childClassId = ClassTraits<T>::info.classId;
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
        V *obj = tr->getObject<V>(sk.objectId);
        val.push_back(*obj);
        delete obj;
      }
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

/**
 * storage trait for mapped object pointer vectors. Polymorphic
 */
template<typename T, typename V> class ObjectPtrVectorPropertyStorage : public StoreAccessBase
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

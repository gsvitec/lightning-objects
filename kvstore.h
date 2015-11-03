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
#define PROPERTY(cls, name) ClassTraits<cls>::decl_props[ClassTraits<cls>::PropertyIds::name-1]

namespace flexis {
namespace persistence {

using namespace kv;

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
  template <typename T, template <typename T> class Fact> friend class Cursor;

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
    return m_properties->at(classId);;
  }
};

/**
 * cursor for iterating over class objects
 */
template <typename T, template <typename T> class Fact>
class Cursor
{
  Cursor(Cursor<T, Fact> &other) = delete;

  const ClassId m_classId;
  CursorHelper * const m_helper;
  ReadTransaction * const m_tr;
  Fact<T> m_fact;
  bool m_hasData;

public:
  using Ptr = std::shared_ptr<Cursor<T, Fact>>;

  Cursor(ClassId classId, CursorHelper *helper, ReadTransaction *tr, Fact<T> fact)
      : m_classId(classId), m_helper(helper), m_tr(tr), m_fact(fact)
  {
    m_hasData = helper->start();
  }

  virtual ~Cursor() {
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

    using Traits = ClassTraits<T>;

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

  Cursor &operator++() {
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
template<typename T> struct PolyCursor :public Cursor<T, PolyFact> {
  PolyCursor(ClassId classId,
             CursorHelper *helper,
             ReadTransaction *tr, ObjectFactories *factories, ObjectProperties *properties)
      : Cursor<T, PolyFact>(classId, helper, tr, PolyFact<T>(factories, properties)) {}
};
/**
 * non-polymorphic cursor
 */
template<typename T> struct SimpleCursor : public Cursor<T, SimpleFact> {
  SimpleCursor(ClassId classId, CursorHelper *helper, ReadTransaction *tr)
      : Cursor<T, SimpleFact>(classId, helper, tr, SimpleFact<T>()) {}
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

  /**
   * load an object from the KV store polymorphically
   *
   * @param classId the actual class id, which may be the id of a subclass of T
   * @param objectId the key generated by a previous call to WriteTransaction::putObject()
   * @return the object pointer, or nullptr if the key is not defined.
   */
  template<typename T> T *loadObject(ClassId classId, ObjectId objectId)
  {
    T *obj = (T *)store.objectFactories[classId]();

    //load the data buffer
    ReadBuf readBuf;
    getData(readBuf, classId, objectId, 0);

    //nothing here
    if(readBuf.empty()) return nullptr;

    PropertyId propertyId = 0;
    Properties *props = store.objectProperties[classId];
    for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = props->get(px);
      if(!p->enabled) continue;

      p->storage->load(this, readBuf, classId, objectId, obj, p);
    }
    return obj;
  }

  /**
   * read sub-object data into a buffer. Used internally by storage traits
   */
  virtual void getData(ReadBuf &buf, ClassId classId, ObjectId objectId, PropertyId propertyId) = 0;

  virtual CursorHelper * _openCursor(ClassId classId) = 0;
  virtual CursorHelper * _openCursor(ClassId classId, ObjectId objectId, PropertyId propertyId) = 0;

public:
  virtual ~ReadTransaction() {}

  /**
   * @return a cursor over all instances of the given class. The returned cursor is non-polymorphic
   */
  template <typename T> typename SimpleCursor<T>::Ptr openCursor() {
    using Traits = ClassTraits<T>;
    ClassId classId = Traits::info.classId;

    return typename SimpleCursor<T>::Ptr(new SimpleCursor<T>(classId, _openCursor(classId), this));
  }

  /**
   * @param obj a loaded object
   * @param propertyId the propertyId (1-based index into declared properties)
   * @return a cursor over the contents of a vector-valued, lazy-loading object property. The cursor will be empty if the
   * given property is not vector-valued. The cursor is polymorphic
   */
  template <typename T, typename V> typename PolyCursor<V>::Ptr openCursor(ObjectId objectId, T *obj, PropertyId propertyId) {
    using Traits = ClassTraits<T>;
    ClassId classId = Traits::info.classId;

    return typename PolyCursor<V>::Ptr(
        new PolyCursor<V>(classId,
                          _openCursor(classId, objectId, propertyId),
                          this, &store.objectFactories, &store.objectProperties));
  }

  /**
   * load an object from the KV store, using the key generated by a previous call to WriteTransaction::putObject()
   *
   * @return the object, or nullptr if the key does not exist. The returned object is owned by the caller
   */
  template<typename T> T *getObject(long objectId)
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

  template <typename T>
  ObjectId saveObject(ObjectId id, T &obj, bool newObject)
  {
    using Traits = ClassTraits<T>;

    ClassInfo &classInfo = Traits::info;
    ClassId classId = classInfo.classId;
    ObjectId objectId = newObject ? ++classInfo.maxObjectId : id;

    //create the data buffer
    size_t size = calculateBuffer(&obj, Traits::properties);
    writeBuf().start(size);

    //put data into buffer
    PropertyId propertyId = 0;
    for(unsigned px=0, sz=Traits::properties->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = Traits::properties->get(px);
      if(!p->enabled) continue;

      p->storage->save(this, classId, objectId, &obj, p);
    }

    if(!putData(classId, objectId, 0, writeBuf()))
      throw persistence_error("data was not saved");

    writeBuf().reset();

    return objectId;
  }

  template <typename T>
  ObjectId saveObject(ClassId classId, ObjectId id, T &obj, bool newObject)
  {
    ClassInfo *classInfo = store.objectClassInfos[classId];
    Properties *properties = store.objectProperties[classId];
    ObjectId objectId = newObject ? ++classInfo->maxObjectId : id;

    //create the data buffer
    size_t size = calculateBuffer(&obj, properties);
    writeBuf().start(size);

    //put data into buffer
    PropertyId propertyId = 0;
    for(unsigned px=0, sz=properties->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = properties->get(px);
      if(!p->enabled) continue;

      p->storage->save(this, classId, objectId, &obj, p);
    }

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
   * save a sub-object data buffer
   */
  virtual bool putData(ClassId classId, ObjectId objectId, PropertyId propertyId, WriteBuf &buf) = 0;

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
   * update a member variable of the given, already persistent object.
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
    pa->storage->save(this, Traits::info.classId, objId, &obj, pa, true);
  }

  /**
   * update a member variable of the given, already persistent object.
   *
   * @param obj the persistent object pointer. Note that this pointer must have been obtained from the KV store
   * @param propertyId the propertyId (1-based index of the declared property). Note that the template type
   * parameter must refer to the (super-)class which declares the member
   */
  template <typename T>
  void updateMember(std::shared_ptr<T> &obj, PropertyId propertyId)
  {
    using Traits = ClassTraits<T>;

    ObjectId objId = get_objectid(obj);

    PropertyAccessBase *pa = Traits::decl_props[propertyId-1];
    pa->storage->save(this, Traits::info.classId, objId, &obj, pa, true);
  }

  template <typename T>
  void deleteObject(ObjectId objId) {

  }

  template <typename T>
  void erase()
  {
    for(auto cursor = openCursor<T>(); !cursor->atEnd(); ++(*cursor)) {
      T *loaded = cursor->get();
      //cursor->
    }
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
  void load(ReadTransaction *tr,
            ReadBuf &buf, ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
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
  void load(ReadTransaction *tr,
            ReadBuf &buf, ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
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
  void load(ReadTransaction *tr,
            ReadBuf &buf, ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
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
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa) override
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
  void load(ReadTransaction *tr,
            ReadBuf &buf, ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
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
 * storage trait for mapped object references w/ raw pointer
 */
template<typename T, typename V> struct ObjectPropertyStorage : public StoreAccessBase
{
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    WriteBuf propBuf(sizeof(StorageKey::byteSize));

    ClassId childClassId = ClassTraits<T>::info.classId;
    tr->pushWriteBuf();
    ObjectId childId = tr->putObject<V>(val);
    tr->popWriteBuf();

    propBuf.append(childClassId, childId, 0);

    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force=false) override
  {
    tr->getData(buf, classId, objectId, pa->id);
    if(!buf.empty()) {
      StorageKey sk;
      if(buf.read(sk)) {
        V *v = tr->getObject<V>(sk.objectId);
        T *tp = reinterpret_cast<T *>(obj);
        ClassTraits<T>::get(*tp, pa, *v);
        delete tp;
      }
    }
  }
};

/**
 * storage trait for mapped object references w/ raw pointer
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

    WriteBuf propBuf(StorageKey::byteSize);
    propBuf.append(childClassId, childId, 0);

    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");
  }
  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    if(m_lazy && !force) return;

    tr->getData(buf, classId, objectId, pa->id);
    if(!buf.empty()) {
      StorageKey sk;
      if(buf.read(sk) && sk.classId > 0) {
        V *v = tr->getObject<V>(sk.objectId);
        std::shared_ptr<V> vp = std::shared_ptr<V>(v, object_handler<V>(sk.objectId));
        T *tp = reinterpret_cast<T *>(obj);
        ClassTraits<T>::get(*tp, pa, vp);
      }
    }
  }
};

/**
 * storage trait for mapped object vectors
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
    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");
  }

  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    if(m_lazy && !force) return;

    std::vector<V> val;

    tr->getData(buf, classId, objectId, pa->id);
    if(!buf.empty()) {
      StorageKey sk;
      while(buf.read(sk)) {
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
 * storage trait for mapped object vectors
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
    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");
  }

  void load(ReadTransaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, bool force) override
  {
    if(m_lazy && !force) return;

    std::vector<std::shared_ptr<V>> val;

    tr->getData(buf, classId, objectId, pa->id);
    if(!buf.empty()) {
      StorageKey sk;
      while(buf.read(sk)) {
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

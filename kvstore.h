//
// Created by chris on 10/7/15.
//

#ifndef FLEXIS_FLEXIS_KVSTORE_H
#define FLEXIS_FLEXIS_KVSTORE_H

#include <string>
#include <typeinfo>
#include <memory>
#include <persistence_error.h>
#include <FlexisPersistence_Export.h>
#include "kvtraits.h"

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

/**
 * high-performance key/value store interface
 */
class FlexisPersistence_EXPORT KeyValueStore : public KeyValueStoreBase
{
public:
  struct FlexisPersistence_EXPORT Factory
  {
    virtual KeyValueStore *make(std::string location) const = 0;
  };

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
    updateClassSchema(Traits::info, Traits::properties, Traits::num_properties());
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

class FlexisPersistence_EXPORT CursorHelper {
  template <typename T> friend class Cursor;

protected:
  virtual ~CursorHelper() {}

  /**
   * position the cursor at the first object of the given class.
   * @return true if an object was found
   */
  virtual bool start(ClassId classId) = 0;

  /**
   * position the cursor at the next object.
   * @return true if an object was found
   */
  virtual bool next() = 0;

  /**
   * close the cursor an release all resources
   */
  virtual void close() = 0;

  /**
   * read the data at the current cursor position into the key and buffer
   */
  virtual void get(StorageKey &key, ReadBuf &rb) = 0;
};

/**
 * cursor for iterating over class objects
 */
template <typename T>
class Cursor
{
  Cursor(Cursor<T> &other) = delete;

  const ClassId m_classId;
  CursorHelper * const m_helper;
  ReadTransaction *const m_tr;
  bool m_hasData;

public:
  using Ptr = std::shared_ptr<Cursor<T>>;

  Cursor(ClassId classId, CursorHelper *helper, ReadTransaction *tr)
      : m_classId(classId), m_helper(helper), m_tr(tr)
  {
    m_hasData = helper->start(classId);
  }

  ~Cursor() {
    delete m_helper;
  };

  T *get()
  {
    T *obj = new T();
    using Traits = ClassTraits<T>;

    //load the data buffer
    ReadBuf readBuf;
    StorageKey key;
    m_helper->get(key, readBuf);

    //nothing here
    if(readBuf.empty()) return nullptr;

    PropertyId propertyId = 0;
    for(auto info : Traits::properties) {

      //we use the key index (+1) as id
      propertyId++;

      if(!info->enabled) continue;

      info->storage->load(m_tr, readBuf, obj, info, m_classId, key.objectId, propertyId);
    }
    return obj;
  }

  Cursor &operator++() {
    m_hasData = m_helper->next();
    return *this;
  }

  bool atEnd() {
    return !m_hasData;
  }

  void close() {
    m_helper->close();
  }
};

/**
 * Transaction that allows read operations only. Read transactions can be run concurrently
 */
class FlexisPersistence_EXPORT ReadTransaction
{
  template<typename T, typename V> friend class VectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPropertyStorage;
  template<typename T, typename V> friend class ObjectVectorPropertyStorage;

protected:
  KeyValueStore &store;
  ReadTransaction(KeyValueStore &store) : store(store) {}

  /**
   * load an object from the KV store, using the key ggenerated by a previous call to put()
   *
   * @return the object, or nullptr if the key is not defined. The returned object is owned by the caller and must be
   * disposed there
   */
  template<typename T> T *loadObject(long objectId)
  {
    T *obj = new T();
    using Traits = ClassTraits<T>;

    ClassId classId = Traits::info.classId;

    //load the data buffer
    ReadBuf readBuf;
    getData(readBuf, classId, objectId, 0);

    //nothing here
    if(readBuf.empty()) return nullptr;

    PropertyId propertyId = 0;
    for(auto info : Traits::properties) {
      //use array index (+1) as id
      propertyId++;

      if(!info->enabled) continue;

      info->storage->load(this, readBuf, obj, info, classId, objectId, propertyId);
    }
    return obj;
  }

  /**
   * read sub-object data into a buffer. Used internally by storage traits
   */
  virtual void getData(ReadBuf &buf, ClassId classId, ObjectId objectId, PropertyId propertyId) = 0;

  virtual CursorHelper * openCursor(ClassId classId) = 0;

public:
  virtual ~ReadTransaction() {}

  template <typename T> typename Cursor<T>::Ptr openCursor() {
    using Traits = ClassTraits<T>;
    ClassId classId = Traits::info.classId;

    return typename Cursor<T>::Ptr(new Cursor<T>(classId, openCursor(classId), this));
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

  virtual void commit() = 0;
  virtual void abort() = 0;
};

/**
 * Transaction for read and write operations. Only one write transaction can be active at a time, and it should
 * be accessed from one thread only
 */
class FlexisPersistence_EXPORT WriteTransaction : public ReadTransaction
{
  template<typename T, typename V> friend class SimplePropertyStorage;
  template <typename T, typename F> friend class StoreAccessFP;
  template<typename T, typename V> friend class VectorPropertyStorage;
  template<typename T, typename V> friend class ObjectPropertyStorage;
  template<typename T, typename V> friend class ObjectVectorPropertyStorage;

  /**
   * calculate shallow byte size, i.e. the size of the buffer required for properties that dont't get saved under
   * an individual key
   */
  template<typename T>
  size_t calculateBuffer(T *obj, PropertyAccessBase *properties[], unsigned numProperties)
  {
    size_t size = 0;
    for(int i=0; i<numProperties; i++) {
      auto info = properties[i];

      if(!info->enabled) continue;

      if(info->type.byteSize) {
        //vector is stored as separate key
        if(!info->type.isVector)
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
    size_t size = calculateBuffer(&obj, Traits::properties, Traits::num_properties());
    writeBuf().start(size);

    //put data into buffer
    PropertyId propertyId = 0;
    for(auto info : Traits::properties) {

      //we use the key index (+1) as id
      propertyId++;

      if(!info->enabled) continue;

      info->storage->save(this, &obj, info, classId, objectId, propertyId);
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
};

/**
 * storage trait for base types that go directly into the shallow buffer
 */
template<typename T, typename V>
struct SimplePropertyStorage : public StoreAccessBase
{
  void load(ReadTransaction *tr, ReadBuf &buf, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ValueTraits<V>::getBytes(buf, val);
    ClassTraits<T>::get(*tp, pa, val);
  }
  void save(WriteTransaction *tr, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    ValueTraits<V>::putBytes(tr->writeBuf(), val);
  }
};

/**
 * storage trait for string, with dynamic size calculation (type.byteSize is 0)
 */
template<typename T>
struct SimplePropertyStorage<T, std::string> : public StoreAccessBase
{
  size_t size(void *obj, const PropertyAccessBase *pa) override {
    std::string val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);
    return val.length() + 1;
  };
};

/**
 * storage trait for value vector
 */
template<typename T, typename V>
struct VectorPropertyStorage : public StoreAccessBase {
  void load(ReadTransaction *tr, ReadBuf &buf, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    std::vector<V> val;

    ReadBuf readBuf;
    tr->getData(readBuf, classId, objectId, propertyId);
    if(!readBuf.empty()) {
      V v;
      ValueTraits<V>::getBytes(readBuf, v);
      val.push_back(v);
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>(*tp).get(pa, val);
  }
  void save(WriteTransaction *tr, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>(*tp).put(pa, val);

    size_t psz = TypeTraits<V>::pt().byteSize * val.size();
    WriteBuf propBuf(psz);

    using ElementTraits = ValueTraits<V>;
    for(auto v : val) ElementTraits::putBytes(propBuf, v);

    if(!tr->putData(classId, objectId, propertyId, propBuf))
      throw persistence_error("data was not saved");
  }
};

/**
 * storage trait for mapped object references
 */
template<typename T, typename V> struct ObjectPropertyStorage : public StoreAccessBase
{
  void save(WriteTransaction *tr, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
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

    if(!tr->putData(classId, objectId, propertyId, propBuf))
      throw persistence_error("data was not saved");
  }
  void load(ReadTransaction *tr, ReadBuf &buf, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    tr->getData(buf, classId, objectId, propertyId);
    if(!buf.empty()) {
      StorageKey sk;
      if(buf.read(sk)) {
        V *v = tr->getObject<V>(sk.objectId);
        T *tp = reinterpret_cast<T *>(obj);
        ClassTraits<T>::get(*tp, pa, *v);
        delete obj;
      }
    }
  }
};

/**
 * storage trait for mapped object vectors
 */
template<typename T, typename V> struct ObjectVectorPropertyStorage : public StoreAccessBase {
  void save(WriteTransaction *tr, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
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
    if(!tr->putData(classId, objectId, propertyId, propBuf))
      throw persistence_error("data was not saved");
  }

  void load(ReadTransaction *tr, ReadBuf &buf, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    std::vector<V> val;

    tr->getData(buf, classId, objectId, propertyId);
    if(!buf.empty()) {
      StorageKey sk;
      while(buf.read(sk)) {
        V *obj = tr->getObject<V>(sk.objectId);
        if(obj) {
          val.push_back(*obj);
          delete obj;
        }
      }
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(*tp, pa, val);
  }
};

template<typename T> struct PropertyStorage<T, short> : public SimplePropertyStorage<T, short>{};
template<typename T> struct PropertyStorage<T, unsigned short> : public SimplePropertyStorage<T, unsigned short>{};
template<typename T> struct PropertyStorage<T, int> : public SimplePropertyStorage<T, long>{};
template<typename T> struct PropertyStorage<T, unsigned int> : public SimplePropertyStorage<T, unsigned long>{};
template<typename T> struct PropertyStorage<T, long> : public SimplePropertyStorage<T, long>{};
template<typename T> struct PropertyStorage<T, unsigned long> : public SimplePropertyStorage<T, unsigned long>{};
template<typename T> struct PropertyStorage<T, long long> : public SimplePropertyStorage<T, long long>{};
template<typename T> struct PropertyStorage<T, unsigned long long> : public SimplePropertyStorage<T, unsigned long long>{};
template<typename T> struct PropertyStorage<T, float> : public SimplePropertyStorage<T, float>{};
template<typename T> struct PropertyStorage<T, double> : public SimplePropertyStorage<T, double>{};
template<typename T> struct PropertyStorage<T, bool> : public SimplePropertyStorage<T, bool>{};
template<typename T> struct PropertyStorage<T, std::string> : public SimplePropertyStorage<T, std::string>{};

template<typename T> struct PropertyStorage<T, std::vector<short>> : public VectorPropertyStorage<T, short>{};
template<typename T> struct PropertyStorage<T, std::vector<unsigned short>> : public VectorPropertyStorage<T, unsigned short>{};
template<typename T> struct PropertyStorage<T, std::vector<long>> : public VectorPropertyStorage<T, long>{};
template<typename T> struct PropertyStorage<T, std::vector<unsigned long>> : public VectorPropertyStorage<T, unsigned long>{};
template<typename T> struct PropertyStorage<T, std::vector<long long>> : public VectorPropertyStorage<T, long long>{};
template<typename T> struct PropertyStorage<T, std::vector<unsigned long long>> : public VectorPropertyStorage<T, unsigned long long>{};
template<typename T> struct PropertyStorage<T, std::vector<float>> : public VectorPropertyStorage<T, float>{};
template<typename T> struct PropertyStorage<T, std::vector<double>> : public VectorPropertyStorage<T, double>{};
template<typename T> struct PropertyStorage<T, std::vector<bool>> : public VectorPropertyStorage<T, bool>{};
template<typename T> struct PropertyStorage<T, std::vector<std::string>> : public VectorPropertyStorage<T, std::string>{};

template <typename O, typename P, P O::*p>
struct ObjectPropertyAssign : public PropertyAssign<O, P, p> {
  ObjectPropertyAssign(const char * name)
      : PropertyAssign<O, P, p>(name, new ObjectPropertyStorage<O, P>(), object_t<P>()) {}
};

template <typename O, typename P, std::vector<P> O::*p>
struct ObjectVectorPropertyAssign : public PropertyAssign<O, std::vector<P>, p> {
  ObjectVectorPropertyAssign(const char * name)
      : PropertyAssign<O, std::vector<P>, p>(name, new ObjectVectorPropertyStorage<O, P>(), object_vector_t<P>()) {}
};
} //kv

} //persistence
} //flexis

#endif //FLEXIS_FLEXIS_KVSTORE_H

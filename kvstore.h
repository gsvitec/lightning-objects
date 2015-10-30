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

/**
 * high-performance key/value store interface
 */
class FlexisPersistence_EXPORT KeyValueStore : public KeyValueStoreBase
{
  friend class kv::ReadTransaction;

  ObjectFactories objectFactories;

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
  ReadTransaction * const m_tr;
  ObjectFactories & m_objectFactories;
  bool m_hasData;

public:
  using Ptr = std::shared_ptr<Cursor<T>>;

  Cursor(ClassId classId, CursorHelper *helper, ReadTransaction *tr, ObjectFactories &objectFactories)
      : m_classId(classId), m_helper(helper), m_tr(tr), m_objectFactories(objectFactories)
  {
    m_hasData = helper->start(classId);
  }

  ~Cursor() {
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

  T *get(ObjectId *objId=nullptr)
  {
    using Traits = ClassTraits<T>;
    T *obj = (T *)m_objectFactories[Traits::info.classId]();

    //load the data buffer
    ReadBuf readBuf;
    StorageKey key;
    m_helper->get(key, readBuf);

    //nothing here
    if(readBuf.empty()) return nullptr;

    if(objId) *objId = key.objectId;

    PropertyId propertyId = 0;
    for(unsigned px=0, sz=Traits::properties->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = Traits::properties->get(px);
      if(!p->enabled) continue;

      p->storage->load(m_tr, readBuf, obj, p, m_classId, key.objectId, propertyId);
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
  template<typename T, typename V> friend class ObjectPtrPropertyStorage;
  template<typename T, typename V> friend class ObjectPtrVectorPropertyStorage;

protected:
  KeyValueStore &store;
  ReadTransaction(KeyValueStore &store) : store(store) {}

  /**
   * load an object from the KV store, using the key (or object_ptr) generated by a previous call to
   * WriteTransaction::putObject()
   *
   * @return the object pointer, or nullptr if the key is not defined.
   */
  template<typename T> T *loadObject(ObjectId objectId)
  {
    using Traits = ClassTraits<T>;
    T *obj = (T *)store.objectFactories[Traits::info.classId]();

    ClassId classId = Traits::info.classId;

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

      p->storage->load(this, readBuf, obj, p, classId, objectId, propertyId);
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

    return typename Cursor<T>::Ptr(new Cursor<T>(classId, openCursor(classId), this, store.objectFactories));
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
    size_t size = calculateBuffer(&obj, Traits::properties);
    writeBuf().start(size);

    //put data into buffer
    PropertyId propertyId = 0;
    for(unsigned px=0, sz=Traits::properties->full_size(); px < sz; px++) {
      //we use the key index (+1) as id
      propertyId++;

      PropertyAccessBase *p = Traits::properties->get(px);
      if(!p->enabled) continue;

      p->storage->save(this, &obj, p, classId, objectId, propertyId);
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
   * update a mapped object which is a member of another object
   *
   * @param objId the objectId of the enclosing object
   * @param propertyId the id of the property which holds the member
   * @param m the member object
   */
  template <typename T, typename M>
  void updateMember(ObjectId objId, PropertyId propertyId, std::shared_ptr<M> m) {
    using Traits = ClassTraits<T>;

    ClassInfo &classInfo = Traits::info;
    ClassId classId = classInfo.classId;

    object_handler<M> *ohm = std::get_deleter<object_handler<M>>(m);
    if(ohm) {
      //its a pointer that we prepared during load => get the objectId from there
      saveObject<M>(ohm->objectId, *m, false);
    }
    else {
      //pull the StorageKey from property data
      ReadBuf rb;
      getData(rb, classId, objId, propertyId);
      StorageKey sk;
      //got it -> we have the objectId of the member
      if(rb.read(sk)) {
        saveObject<M>(sk.objectId, *m, false);
      }
    }
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
struct BasePropertyStorage<T, std::string> : public StoreAccessBase
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
 * storage trait for mapped object references w/ raw pointer
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
        delete tp;
      }
    }
  }
};

/**
 * storage trait for mapped object references w/ raw pointer
 */
template<typename T, typename V> struct ObjectPtrPropertyStorage : public StoreAccessBase
{
  void save(WriteTransaction *tr, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    std::shared_ptr<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    WriteBuf propBuf(StorageKey::byteSize);

    ClassId childClassId = ClassTraits<T>::info.classId;
    tr->pushWriteBuf();
    ObjectId childId = tr->putObject<V>(*val);
    tr->popWriteBuf();

    //update the property so that it holds the object id
    std::shared_ptr<V> val2(nullptr, object_handler<V>(childId));
    val2.swap(val);
    ClassTraits<T>::get(*tp, pa, val2);

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

/**
 * storage trait for mapped object vectors
 */
template<typename T, typename V> struct ObjectPtrVectorPropertyStorage : public StoreAccessBase {
  void save(WriteTransaction *tr, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    std::vector<std::shared_ptr<V>> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(*tp, pa, val);

    size_t psz = StorageKey::byteSize * val.size();
    WriteBuf propBuf(psz);

    tr->pushWriteBuf();
    ClassId childClassId = ClassTraits<T>::info.classId;
    for(std::shared_ptr<V> &v : val) {
      ObjectId childId = tr->putObject<V>(*v);

      propBuf.append(childClassId, childId, 0);
    }
    tr->popWriteBuf();
    if(!tr->putData(classId, objectId, propertyId, propBuf))
      throw persistence_error("data was not saved");
  }

  void load(ReadTransaction *tr, ReadBuf &buf, void *obj, const PropertyAccessBase *pa, ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    std::vector<std::shared_ptr<V>> val;

    tr->getData(buf, classId, objectId, propertyId);
    if(!buf.empty()) {
      StorageKey sk;
      while(buf.read(sk)) {
        V *obj = tr->getObject<V>(sk.objectId);
        if(obj) {
          val.push_back(std::shared_ptr<V>(obj, object_handler<V>(sk.objectId)));
          delete obj;
        }
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
  ObjectPtrPropertyAssign(const char * name)
      : PropertyAccess<O, std::shared_ptr<P>>(name, new ObjectPtrPropertyStorage<O, P>(), object_t<P>()) {}
  void set(O &o, std::shared_ptr<P> val) const override { o.*p = val;}
  std::shared_ptr<P> get(O &o) const override { return o.*p;}
};

template <typename O, typename P, std::vector<P> O::*p>
struct ObjectVectorPropertyAssign : public PropertyAssign<O, std::vector<P>, p> {
  ObjectVectorPropertyAssign(const char * name)
      : PropertyAssign<O, std::vector<P>, p>(name, new ObjectVectorPropertyStorage<O, P>(), object_vector_t<P>()) {}
};
template <typename O, typename P, std::vector<std::shared_ptr<P>> O::*p>
struct ObjectPtrVectorPropertyAssign : public PropertyAssign<O, std::vector<std::shared_ptr<P>>, p> {
  ObjectPtrVectorPropertyAssign(const char * name)
      : PropertyAssign<O, std::vector<std::shared_ptr<P>>, p>(name, new ObjectPtrVectorPropertyStorage<O, P>(), object_vector_t<P>()) {}
};

} //kv

} //persistence
} //flexis

#endif //FLEXIS_FLEXIS_KVSTORE_H

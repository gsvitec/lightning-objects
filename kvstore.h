/*
 * LightningObjects C++ Object Storage based on Key/Value API
 *
 * Copyright (C) 2016 GS Vitec GmbH <christian@gsvitec.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, and provided
 * in the LICENSE file in the root directory of this software.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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

static const kv::ClassId COLLECTION_CLSID = 1;
static const kv::ClassId COLLINFO_CLSID = 2;
static const size_t DEFAULT_CHUNKSIZE = 1024 * 2; //default chunksize. All data in one page

/**
 * data structures and enums used during schema validation
 */
class schema_compatibility
{
public:
  /**
   * describes the schema change
   */
  enum What {
    /** property definitions differ between schema and runtime */
    property_modified,

    /** a key-only property exists in the schema but not in the runtime */
    keyed_property_removed,

    /** a key-only property exists in the runtime but not in the schema */
    keyed_property_added,

    /** runtime has a new emmbedded property at the end of the buffer */
    embedded_property_appended,

    /** runtime has a new emmbedded property before the end of the buffer */
    embedded_property_inserted,

    /** runtime is missing an embedded property before the end of the buffer */
    embedded_property_removed_internal,

    /** runtime is missing an embedded property at the end of the buffer */
    embedded_property_removed_end
  };

  /**
   * describes validation results regarding a given property
   */
  struct Property {
    std::string name;
    unsigned position;
    What what;
    std::string description;
    std::string runtime, saved;

    Property(std::string nm, unsigned pos, What what) : what(what), name(nm), position(pos) {}
  };
  std::unordered_map<std::string, std::vector<Property>> classProperties;

  void put(const char *className, std::vector<Property> errs) {
    classProperties[className] = errs;
  }

  bool empty() {
    return classProperties.empty();
  }

  struct error : public persistence_error
  {
    const std::shared_ptr<const schema_compatibility> compatibility;
    error(std::string message, std::shared_ptr<const schema_compatibility> compatibility)
        : persistence_error(message), compatibility(compatibility) {}

    void printDetails(std::ostream &os);
    std::unordered_map<std::string, std::vector<std::string>> getDetails();
  };

  error make_error();
};

class class_not_registered_error : public persistence_error
{
public:
  class_not_registered_error(const std::string className)
      : persistence_error("class has not been registered", className) {}
};

namespace kv {

class Transaction;
class ReadTransaction;
class ExclusiveReadTransaction;
class WriteTransaction;
template <typename T> class ClassCursor;

using TransactionPtr = std::shared_ptr<kv::Transaction>;
using ReadTransactionPtr = std::shared_ptr<kv::ReadTransaction>;
using ExclusiveReadTransactionPtr = std::shared_ptr<kv::ExclusiveReadTransaction>;
using WriteTransactionPtr = std::shared_ptr<kv::WriteTransaction>;

using ObjectProperties = std::unordered_map<ClassId, Properties *>;
using ObjectClassInfos = std::unordered_map<ClassId, AbstractClassInfo *>;

/**
 * object cache interface
 */
struct ObjectCache {
  virtual ~ObjectCache() {}

  template <typename T> std::shared_ptr<T> &get(ObjectId id);
  template <typename T> void put(ObjectId id, std::shared_ptr<T> ptr);
  template <typename T> void erase(ObjectId id);
};
/**
 * map-based object cache implementation
 */
template <typename T>
struct TypedObjectCache : public ObjectCache {
  std::unordered_map<ObjectId, std::shared_ptr<T>> objects;
};

template <typename T> std::shared_ptr<T> &ObjectCache::get(ObjectId id) {
  return dynamic_cast<TypedObjectCache<T> *>(this)->objects[id];
}

template <typename T> void ObjectCache::put(ObjectId id, std::shared_ptr<T> ptr) {
  dynamic_cast<TypedObjectCache<T> *>(this)->objects[id] = ptr;
}

template <typename T> void ObjectCache::erase(ObjectId id) {
  dynamic_cast<TypedObjectCache<T> *>(this)->objects.erase(id);
}

/**
 * global function for assigning storage IDs
 * @return the next available storage ID
 */
StoreId nextStoreId();
} //kv

/**
 * non-templated abstract base class for KeyValueStore
 */
class KeyValueStoreBase
{
  friend class kv::Transaction;
  friend class kv::WriteTransaction;

public:
  /** 0-based id to distinguish multiple stores using the same mappings */
  const kv::StoreId id;

protected:
  KeyValueStoreBase(kv::StoreId _id) : id(_id) {}
  virtual ~KeyValueStoreBase() {}

  //property info stored in database
  struct PropertyMetaInfo {
    std::string name;
    kv::PropertyId id;
    unsigned typeId;
    bool isVector;
    unsigned byteSize;
    std::string className;
    kv::StoreLayout storeLayout;
  };
  using PropertyMetaInfoPtr = std::shared_ptr<PropertyMetaInfo>;

  /**
   * @return the optimal chunk size minus the reserved size
   */
  virtual size_t getOptimalChunkSize(size_t reserved) = 0;

  /**
   * compare loaded and declared property mapping
   */
  void compare(std::vector<schema_compatibility::Property> &errors, unsigned index,
               KeyValueStoreBase::PropertyMetaInfoPtr pi, const kv::PropertyAccessBase *pa);

  /**
   * check if class schema already exists. If so, check compatibility. If not, save it for later reference
   *
   * @param classInfo the runtime classInfo to check
   * @param properties the runtime class properties
   * @param numProperties size of the former
   * @param errors (out) compatibility errors detected during check
   * @return true if the class already existed
   */
  bool updateClassSchema(kv::AbstractClassInfo *classInfo, const kv::PropertyAccessBase ** properties[],
                         unsigned numProperties, std::vector<schema_compatibility::Property> &errors);

  /**
   * load class metadata from the store. If it doesn't already exist, save currentProps for later reference
   *
   * @param storeId
   * @param classInfo (in/out) the ClassInfo which holds the fully qualified class name. The other fields will be set
   * @param currentProps (in) the currently live persistent properties
   * @param numProps (in) the length of the above array
   * @param propertyInfos (out) the persistent propertyInfos. This will be empty if the class was newly declared
   */
  virtual void loadSaveClassMeta(
      kv::StoreId storeId,
      kv::AbstractClassInfo *classInfo,
      const kv::PropertyAccessBase ** currentProps[],
      unsigned numProps,
      std::vector<PropertyMetaInfoPtr> &propertyInfos) = 0;

public:
  /**
   * @return the optimal chunk size, which is the current page size minus the chunk header
   */
  size_t getOptimalChunkSize() {
    return getOptimalChunkSize(kv::ChunkHeader_sz);
  }
};

namespace put_schema {
/*
 * helper structs for processing the variadic template list which is passed to KeyValueStore#putSchema
 */

struct validate_info {
  kv::AbstractClassInfo * const classInfo;
  kv::ClassData * const cdata;
  const kv::PropertyAccessBase *** const decl_props;
  kv::Properties * const properties;
  const unsigned num_decl_props;

  validate_info(kv::AbstractClassInfo *classInfo, kv::ClassData *cdata, kv::Properties *properties, const kv::PropertyAccessBase ** decl_props[],
                unsigned num_decl_props)
      : classInfo(classInfo), cdata(cdata), properties(properties), decl_props(decl_props), num_decl_props(num_decl_props) {}
};

//primary template
template<typename... Sargs>
struct register_type;

//worker template
template<typename S>
struct register_type<S>
{
  using Traits = kv::ClassTraits<S>;

  static void addTypes(kv::StoreId storeId, std::vector<validate_info> &vinfos)
  {
    //collect infos
    vinfos.push_back(
        validate_info(Traits::traits_info, &Traits::traits_data(storeId), Traits::traits_properties,
                      Traits::decl_props, Traits::num_decl_props));

    Traits::init();
  }
};

//helper for working the variadic temnplate list
template<typename S, typename... Sargs>
struct register_helper
{
  static void addTypes(kv::StoreId storeId, std::vector<validate_info> &vinfos) {
    register_type<S>().addTypes(storeId, vinfos);
    register_type<Sargs...>().addTypes(storeId, vinfos);
  }
};

//secondary template
template<typename... Sargs>
struct register_type {
  static void addTypes(kv::StoreId storeId, std::vector<validate_info> &vinfos) {
    register_helper<Sargs...>().addTypes(storeId, vinfos);
  }
};

} //put_schema

/**
 * high-performance key/value store interface. Most application-relevant functions are provided by Transaction
 * and WriteTransaction, which can be obtined from this class
 */
class KeyValueStore : public KeyValueStoreBase
{
  friend class kv::Transaction;
  friend class kv::ExclusiveReadTransaction;
  friend class kv::WriteTransaction;
  template <typename T> friend class kv::ClassCursor;

  //backward mapping from ClassId, used during polymorphic operations
  kv::ObjectProperties objectProperties;
  kv::ObjectClassInfos objectClassInfos;

  //helpers for storing type_infos in a map
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

  std::unordered_map<TypeInfoRef, kv::ClassId, TypeinfoHasher, TypeinfoEqualTo> objectTypeInfos;
  std::unordered_map<kv::ClassId, std::shared_ptr<kv::ObjectCache>> objectCaches;

  template <typename T> inline
  std::shared_ptr<T> putCache(T *obj, kv::object_handler<T> handler)
  {
    std::shared_ptr<T> result = std::shared_ptr<T>(obj, handler);
    objectCaches[handler.classId]->put(handler.objectId, result);
    return result;
  }

  template <typename T> inline
  std::shared_ptr<T> &getCached(kv::ClassId classId, kv::ObjectId objectId)
  {
#ifdef _MSC_VER
    return objectCaches[classId]->ObjectCache::get<T>(objectId);
#else
    return objectCaches[classId]->template ObjectCache::get<T>(objectId);
#endif
  }

  template <typename T> inline
  void removeCached(kv::ClassId classId, kv::ObjectId objectId)
  {
#ifdef _MSC_VER
    return objectCaches[classId]->ObjectCache::erase<T>(objectId);
#else
    objectCaches[classId]->template ObjectCache::erase<T>(objectId);
#endif
  }

protected:
  kv::ClassId m_maxClassId = kv::AbstractClassInfo::MIN_USER_CLSID;
  kv::ObjectId m_maxCollectionId = 0;

public:
  /**
   * create a new store object.
   * <p>A store is identified by a storeId. If multiple databases are to be used in the same process with
   * different but overlapping mapping schema, each must be assigned a 0-based, consecutive ID. A mapping schema conflict
   * occurs when the sequence of putSchema calls differs for classes whose mappings are used in both stores.
   * </p>
   * Say you have classes A and B in header "mappings1.h", and class C in "mappings2.h". Database 1 sees header
   * "mappings1.h", while database 2 sees headers "mappings1.h" and "mappings2.h". Now, if the initialization code for both
   * databases starts with
   * <pre>
   * store->putSchema<A, B>();
   * </pre>
   * all is well, storeIds dont't matter. However, if database 2 was to declare schema like this:
   * <pre>
   * store->putSchema<C>();
   * store->putSchema<A, B>();
   * </pre>
   * schema setup for classes A and B would be different between database 1 and database 2. Now you have the choice to
   * either mend the setup, or assign storeIds 0 and 1 to the databases.</p>
   * <p>
   * There are 2 other reasons for using storeIds:
   * <ul>
   * <li>class configuration, e.g. refcounting + caching. Class configurations will affect all databases that share mapping and
   * storeId</li>
   * <li>objectIds. For shared mappings in databases with identical storeId, objectIds will be started at the maximum value
   * saved for any participating database. Thus, for DB's with low traffic, id's may rise in an "unnatural" way</li>
   * </ul>
   * </p>
   * Note: storeIds are in-memory only. They are not saved in any way, and there is no need to keep them identical across
   * process instances. Store IDs are not required if there are no common mappings between databases. If used, store IDs
   * must be 0-based, consecutive up to a maximum of MAX_DATABASES (kvtraits.h)
   *
   * @param storeId the store ID. StoreIds should be obtained from kv::nextStoreId
   */
  KeyValueStore(kv::StoreId storeId=kv::nextStoreId()) : KeyValueStoreBase(storeId) {}

  /**
   * register and validate the class schema for this store
   * @param requiredCompatibility the compatibility level thta must be reached. Throw an incompatible_schema_error if any
   * schema change violates that requirement
   */
  template <typename... Cls>
  void putSchema(kv::SchemaCompatibility requiredCompatibility=kv::SchemaCompatibility::write)
  {
    std::vector<put_schema::validate_info> vinfos;
    put_schema::register_type<Cls...>::addTypes(id, vinfos);

    schema_compatibility schemaError;
    for(auto &info : vinfos) {
      std::vector<schema_compatibility::Property> errs;
      updateClassSchema(info.classInfo, info.decl_props, info.num_decl_props, errs);
      if(info.classInfo->compatibility != requiredCompatibility) {
        schemaError.put(info.classInfo->name, errs);
      }

      //make sure all propertyaccessors have correct classId
      for(int i=0; i<info.num_decl_props; i++)
        const_cast<kv::PropertyAccessBase *>(*info.decl_props[i])->classId[id] = info.classInfo->data[id].classId;

      //initialize lookup maps
      objectProperties[info.classInfo->data[id].classId] = info.properties;
      objectClassInfos[info.classInfo->data[id].classId] = info.classInfo;
      objectTypeInfos[info.classInfo->typeinfo] = info.classInfo->data[id].classId;
    }
    if(!schemaError.empty()) {
      for(auto &info : vinfos)
        if(info.classInfo->compatibility < requiredCompatibility)
          throw schemaError.make_error();
    }
  }

  /**
   * configure object caching for the given class.
   *
   * This is an owned operation, meaning that once it has been set, it can only be changed by the holder of the ownerId
   * returned from the initial call.
   *
   * @param cache whether caching should be turned on or off
   * @param owner an owner id returned from a previous call to this function
   * @return a non-0 owner id if the operation was performed successfully, or 0 if it was rejected but the
   * setting is already as requested
   * @throw persistence_error if the setting is already owned and not the same as requested
   */
  template <typename T>
  unsigned setCache(bool cache=true, unsigned owner=0) {
    if(kv::ClassTraits<T>::traits_data(id).cacheOwner == owner) {
      if(owner == 0) kv::ClassTraits<T>::traits_data(id).cacheOwner = owner = rand()+1;

      if(cache)
        objectCaches[kv::ClassTraits<T>::traits_data(id).classId] = std::make_shared<kv::TypedObjectCache<T>>();
      else
        objectCaches.erase(kv::ClassTraits<T>::traits_data(id).classId);

      return owner;
    }
    else {
      if(static_cast<bool>(objectCaches.count(kv::ClassTraits<T>::traits_data(id).classId)) != cache)
        throw persistence_error("cache configuration already owned and differs from requested value");
      return 0;
    }
  }

  /**
   * @return true if caching is configured for the given class
   */
  template <typename T> inline
  bool isCache() {
    return (bool)kv::ClassTraits<T>::traits_data(id).cacheOwner;
  }

  /**
   * configure refcounting for the given template parameter class. When refcounting is on, a separate entry will
   * be written for each object which holds the reference count. The reference count will be incremented whenever
   * the object is added to a shared_ptr-mapped container, and decremented when it is removed from the same
   *
   * This is an owned operation, meaning that once it has been set, it can only be changed by the holder of the ownerId
   * returned from the initial call.
   *
   * @param refcount whether refcounting should be turned on or off
   * @param owner an owner id returned from a previous call to this function
   * @return a non-0 owner id if the operation was performed successfully, or 0 if it was rejected but the
   * setting is already as requested
   * @throw persistence_error if the setting is already owned and not the same as requested
   */
  template <typename T>
  unsigned  setRefCounting(bool refcount=true, unsigned owner=0)
  {
    if(kv::ClassTraits<T>::traits_data(id).refcountingOwner == owner) {
      if(owner == 0) kv::ClassTraits<T>::traits_data(id).refcountingOwner = owner = rand()+1;

      kv::ClassTraits<T>::traits_info->setRefCounting(id, refcount);
      kv::ClassId cid = kv::ClassTraits<T>::traits_data(id).classId;
      for(auto &op : objectProperties) {
        if(op.first != cid && op.second->preparesUpdates(id, cid)) {
          if(refcount)
            objectClassInfos[op.first]->data[id].prepareClasses.insert(cid);
          else
            objectClassInfos[op.first]->data[id].prepareClasses.erase(cid);
        }
      }
      return owner;
    }
    else {
      if(kv::ClassTraits<T>::traits_info->getRefCounting(id) != refcount)
        throw persistence_error("refcounting configuration already owned and differs from requested value");
      return 0;
    }
  }

  /**
   * register a substitute type to be used in polymorphic operations where a subclass of T is unknown.
   * <p>
   * Explanation: you have a schema with classes A, B and C, where C is a subclass of B with virtual methods. A has a
   * member "children" which is a vector<B *>. You save an instance of A with 2 "children": B b and C c. Later on,
   * you load A through a schema where class C is deleted. If you configure a substitute, the number of children
   * will be 2, but c will be an instance of the substitute class
   */
  template <typename T, typename Subst>
  void registerSubstitute()
  {
    kv::ClassTraits<T>::traits_info-> template setSubstitute<Subst>();
  }

  /**
   * @return true if the pointer points to a newly created object
   */
  template <typename T> bool isNew(std::shared_ptr<T> &obj)
  {
    return kv::ClassTraits<T>::getObjectKey(obj)->isNew();
  }

  /**
   * @return the ObjectId held inside the given shared_ptr
   */
  template <typename T> kv::ObjectId getObjectId(std::shared_ptr<T> obj)
  {
    return kv::ClassTraits<T>::getObjectKey(obj)->objectId;
  }

  /**
   * @return a transaction object that provides read operations.
   */
  virtual kv::ReadTransactionPtr beginRead() = 0;

  /**
   * @return a transaction object that provides read operations but prevents writing
   */
  virtual kv::ExclusiveReadTransactionPtr beginExclusiveRead() = 0;

  /**
   * @param needsKBs database space required by this transaction. If not set, the default will be used.
   *
   * @return a transaction object that allows reading + writing the database.
   * @throws invalid_argument if another write transaction is running in parralel, or write operations are currently
   * blocked by an exclusive read transaction
   */
  virtual kv::WriteTransactionPtr beginWrite(unsigned needsKBs=0) = 0;
};

namespace kv {

void readChunkHeader(ReadBuf &buf, size_t *dataSize, size_t *startIndex, size_t *elementCount);
void readObjectHeader(ReadBuf &buf, ClassId *classId, ObjectId *objectId, size_t *size=nullptr, bool *deleted=nullptr);

template <typename T>
bool all_predicate(std::shared_ptr<T> t=nullptr) {return true;}

/**
 * read object data polymorphically
 */
template<typename T> void readObject(StoreId storeId, Transaction *tr, ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj)
{
  Properties *props = ClassTraits<T>::getProperties(storeId, classId);
  if(!props) throw persistence_error("unknown classId. Class not registered");

  for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
    const PropertyAccessBase *p = props->get(px);
    if(!p->enabled) continue;

    ClassTraits<T>::load(storeId, tr, buf, classId, objectId, obj, p);
  }
}

/**
 * read object data non-polymorphically
 */
template<typename T> void readObject(StoreId storeId, Transaction *tr, ReadBuf &buf, T &obj,
                                     ClassId classId, ObjectId objectId, StoreMode mode = StoreMode::force_none)
{
  Properties *props = ClassTraits<T>::traits_properties;

  for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
    const PropertyAccessBase *p = props->get(px);
    if(!p->enabled) continue;

    ClassTraits<T>::load(storeId, tr, buf, classId, objectId, &obj, p, mode);
  }
}

/**
 * class that must be subclassed by CollectionIterProperty replacements
 */
class IterPropertyBackend
{
protected:
  ObjectId m_collectionId = 0;

public:
  void setCollectionId(ObjectId id) {m_collectionId = id;}
  ObjectId getCollectionId() {return m_collectionId;}

  virtual void init(WriteTransaction *tr) = 0;
  virtual void load(Transaction *tr) = 0;
};
using IterPropertyBackendPtr = std::shared_ptr<IterPropertyBackend>;

/**
 * data used internally during update/delete preparation
 */
class PrepareData {
public:
  struct Entry {
    bool updatePrepared = false;
    ClassId prepareCid = 0;
    ObjectId prepareOid = 0;

    inline void reset() {updatePrepared = false; prepareCid = 0; prepareOid = 0;}
  };

private:
  std::unordered_map<unsigned, Entry> m_entries;

public:
  Entry &entry(unsigned key) {return m_entries[key];}
};

/**
 * buffer for reading raw object data
 */
class ObjectBuf
{
  template<typename T, typename V> friend struct ValueEmbeddedStorage;
  template<typename T, typename V> friend struct ValueKeyedStorage;

protected:
  const bool makeCopy = false;
  bool dataChecked = false;
  size_t markOffs = 0;
  ReadBuf readBuf;
  virtual void checkData() {}

  void checkData(Transaction *tr, ClassId cid, ObjectId oid);

public:
  ObjectKey key;

  ObjectBuf(bool makeCopy) : makeCopy(makeCopy) {}
  ObjectBuf(byte_t *data, size_t size) {
    dataChecked = true;
    readBuf.start(data, size);
  }
  ObjectBuf(ObjectKey key, bool makeCopy) : key(key), makeCopy(makeCopy) {}
  ObjectBuf(Transaction *tr, ClassId classId, ObjectId objectId, bool makeCopy)
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
  Transaction * const m_txn;

protected:
  void checkData() override;

public:
  LazyBuf(Transaction * txn, const ObjectKey &key, bool makeCopy) : ObjectBuf(key, makeCopy), m_txn(txn) {}
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
class CollectionAppenderBase;
struct CollectionInfo
{
  //unique collection id
  ObjectId collectionId = 0;

  //appenders that will be automatically closed on commit
  std::set<CollectionAppenderBase *> appenders;

  //collection chunks
  std::vector <ChunkInfo> chunkInfos;

  PropertyId nextChunkId = 1;
  size_t nextStartIndex = 0;

  CollectionInfo() {}
  CollectionInfo(ObjectId collectionId) : collectionId(collectionId) {}

  size_t count() {
    size_t cnt = 0;
    for(ChunkInfo &ci : chunkInfos) cnt += ci.elementCount;
    return cnt;
  }
};

class ChunkCursor
{
protected:
  bool m_atEnd;

public:
  using Ptr = std::shared_ptr<ChunkCursor>;

  bool atEnd() const {return m_atEnd;}
  virtual bool next(PropertyId *chunkId = nullptr) = 0;
  virtual void get(ReadBuf &rb) = 0;
  virtual bool seek(PropertyId chunkId) = 0;
  virtual void close() = 0;
};

/**
 * top-level chunked collection cursor base
 */
class CollectionCursorBase
{
protected:
  ChunkCursor::Ptr m_chunkCursor;
  Transaction * const m_tr;
  const StoreId m_storeId;
  CollectionInfo * const m_collectionInfo;

  ReadBuf m_readBuf;
  PropertyId m_chunkId;
  size_t m_dataSize = 0, m_startIndex = 0, m_elementCount = 0, m_curElement = 0;

  bool next();

  void objectBufSeek(size_t position);

  /**
   * @return true if the current entry is valid, e.g. not marked for delete
   */
  virtual bool isValid() {return true;}

  /**
   * seek to the given element position in the current chunk buffer
   * @param position the position to seek to
   */
  virtual void bufSeek(size_t position) = 0;

public:
  CollectionCursorBase(ObjectId collectionId, Transaction *tr, ChunkCursor::Ptr chunkCursor);

  /**
   * @return true if this cursor has reached the end
   */
  bool atEnd();

  /**
   * seek to the given 0-based position
   * @return true if the position is valid and data is available
   */
  bool seek(size_t position);

  /**
   * the number of elements in this collection at the point when this cursor was created
   */
  size_t count();
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
      m_curClassInfo = FIND_CLS(T, m_storeId, cid);

      return m_curClassInfo != nullptr || ClassTraits<T>::traits_info->substitute != nullptr;
    }
    return false;
  }

public:
  using Ptr = std::shared_ptr<ObjectCollectionCursor<T>>;

  ObjectCollectionCursor(StoreId storeId, ObjectId collectionId, Transaction *tr, ChunkCursor::Ptr chunkCursor)
      : CollectionCursorBase(collectionId, tr, chunkCursor), m_declClass(ClassTraits<T>::traits_data(storeId).classId)
  {
  }

  void bufSeek(size_t position) {
    objectBufSeek(position);
  }

  /**
   * @return the next object read from the buffer, or nullptr if the end was reached
   */
  T *get()
  {
    if(!next()) return nullptr;

    ClassId classId;
    ObjectId objectId;
    readObjectHeader(m_readBuf, &classId, &objectId);

    if(m_curClassInfo) {
      T *tp = m_curClassInfo->makeObject(m_storeId, classId);
      readObject<T>(m_storeId, m_tr, m_readBuf, classId, objectId, tp);
      return tp;
    }
    else {
      T *sp = ClassTraits<T>::getSubstitute();
      readObject<T>(m_storeId, m_tr, m_readBuf, *sp, classId, objectId);
      return sp;
    }
  }
};

/**
 * cursor for iterating over a top-level (chunked) value collection
 */
template <typename T>
class ValueCollectionCursor : public CollectionCursorBase
{
public:
  using Ptr = std::shared_ptr<ValueCollectionCursor<T>>;

  ValueCollectionCursor(ObjectId collectionId, Transaction *tr, ChunkCursor::Ptr chunkCursor)
      : CollectionCursorBase(collectionId, tr, chunkCursor)
  {}

  void bufSeek(size_t position) {
    if(position > m_curElement) {
      for(size_t pos=0, epos=position-m_curElement; pos < epos; pos++) {
        T val;
        ValueTraits<T>::getBytes(m_readBuf, val);
      }
    }
    else {
      m_readBuf.reset();
      readChunkHeader(m_readBuf, 0, 0, 0);
      for(size_t pos=0; pos < position; pos++) {
        T val;
        ValueTraits<T>::getBytes(m_readBuf, val);
      }
    }
    m_curElement = position;
  }

  /**
   * read the current value
   * @param val the value to read into
   * @return true if data was available
   */
  bool get(T &val)
  {
    if(!next()) return false;

    ValueTraits<T>::getBytes(m_readBuf, val);
    return true;
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
  KeyValueStore &m_store;
  const bool m_useCache;
  Transaction * const m_tr;
  bool m_hasData;
  ClassInfo<T> *m_classInfo;

  bool validateClass() {
    m_classInfo = FIND_CLS(T, m_store.id, m_helper->currentClassId());
    return m_classInfo != nullptr || ClassTraits<T>::traits_info->substitute != nullptr;
  }

  T *makeObject(ObjectKey &key, ReadBuf &readBuf)
  {
    if(m_classInfo) {
      T *obj = m_classInfo->makeObject(m_store.id, key.classId);
      readObject<T>(m_store.id, m_tr, readBuf, key.classId, key.objectId, obj);
      return obj;
    }
    else {
      T *sp = ClassTraits<T>::getSubstitute();
      readObject<T>(m_store.id, m_tr, readBuf, *sp, key.classId, key.objectId);
      return sp;
    }
  }

public:
  using Ptr = std::shared_ptr<ClassCursor<T>>;

  ClassCursor(CursorHelper *helper, KeyValueStore &store, Transaction *tr)
      : m_helper(helper), m_store(store), m_tr(tr), m_useCache(store.isCache<T>())
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
   * @return true if the cursor has not reached the end
   */
  bool erase(WriteTransactionPtr tr)
  {
    ObjectKey key;

    ReadBuf readBuf;
    m_helper->get(key, readBuf);
    if(readBuf.null()) return m_hasData;
    if(key.refcount > 1) throw persistence_error("removeObject: refcount > 1");

    using Traits = ClassTraits<T>;

    if(Traits::needsPrepare(m_store.id, key.classId)) {
      Properties *props = Traits::getProperties(m_store.id, key.classId);
      ObjectBuf obuf(readBuf.data(), readBuf.size());

      for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
        const PropertyAccessBase *pa = props->get(px);

        if(!pa->enabled) continue;

        obuf.mark();
        size_t psz = ClassTraits<T>::prepareDelete(m_store.id, tr.get(), obuf, pa);
        obuf.unmark(psz);
      }
    }

    //now remove the object proper
    if(m_useCache) m_store.removeCached<T>(key.classId, key.objectId);

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
      size_t psz = pa->storage->size(m_store.id, objectBuf);
      objectBuf.unmark(psz);
    }
  }

  /**
   * @param key (out) the key to be read into
   * @return the instantiated object at the current cursor position. Object caching is ignored
   */
  T *get(ObjectKey &key)
  {
    ReadBuf readBuf;
    m_helper->get(key, readBuf);

    if(readBuf.null()) return nullptr;

    return makeObject(key, readBuf);
  }

  /**
   * @return a persistent pointer to the object at the current cursor position. Object caching is honored
   */
  std::shared_ptr<T> get()
  {
    object_handler<T> handler;
    ReadBuf readBuf;
    m_helper->get(handler, readBuf);

    if(readBuf.null()) return std::shared_ptr<T>();

    if(m_useCache) {
      std::shared_ptr<T> &cached = m_store.getCached<T>(handler.classId, handler.objectId);
      return cached ? cached : m_store.putCache(makeObject(handler, readBuf), handler);
    }
    return std::shared_ptr<T>(makeObject(handler, readBuf), handler);
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
  bool isOwned() {return m_owned;}
  V *data() {return m_data;}
};

#define RAWDATA_API_ASSERT static_assert(TypeTraits<T>::byteSize == sizeof(T), \
"collection data access only supported for fixed-size types with native size equal byteSize");
#define VALUEAPI_ASSERT(_X) static_assert(has_objid<ClassTraits<_X>>::value, \
"class must define an OBJECT_ID mapping to be usable with value-based API");

/**
 * Transaction that allows read operations only. Read transactions can be run concurrently
 */
class Transaction
{
  template<typename T, typename V> friend class ValueEmbeddedStorage;
  template<typename T, typename V> friend class ValueKeyedStorage;
  template<typename T, typename V> friend class ValueVectorKeyedStorage;
  template<typename T, typename V> friend class ValueSetKeyedStorage;
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
  template<typename T> friend class ObjectIdStorage;
  template <typename T> friend class ObjectCollectionCursor;
  template <typename T> friend class ClassCursor;
  friend class CollectionAppenderBase;
  friend class ObjectBuf;

  CollectionInfo *readCollectionInfo(ReadBuf &readBuf);

protected:
  std::unordered_map<ObjectId, CollectionInfo *> m_collectionInfos;

  KeyValueStore &store;
  bool m_blockWrites;

  Transaction(KeyValueStore &store) : store(store) {}

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

    readObject<T>(store.id, this, readBuf, classId, objectId, &obj);

    return true;
  }

  /**
   * load an object from the KV store polymorphically + refcounting (if configured)
   *
   * @param handler the object handler, which contains the key
   * @param reload bypass possibly configured cache and reload the object
   * @return the object pointer, or nullptr if the key is not defined.
   */
  template<typename T> std::shared_ptr<T> loadObject(object_handler<T> &handler, bool reload=false)
  {
    bool doCache = store.isCache<T>();
    if(doCache && !reload) {
      std::shared_ptr<T> &cached = store.getCached<T>(handler.classId, handler.objectId);
      if(cached) return cached;
    }

    ReadBuf readBuf;
    getData(readBuf, handler, ClassTraits<T>::traits_data(store.id).refcounting);

    if(readBuf.null()) return nullptr;

    T *obj = ClassTraits<T>::makeObject(store.id, handler.classId);
    readObject<T>(store.id, this, readBuf, handler.classId, handler.objectId, obj);

    return doCache ? store.putCache(obj, handler) : std::shared_ptr<T>(obj, handler);
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

    readObject<T>(store.id, this, readBuf, subst, missingClassId, objectId);
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
          ClassInfo<T> *ti = FIND_CLS(T, store.id, cid);
          if(!ti) {
            T *sp = ClassTraits<T>::getSubstitute();
            if(sp) {
              readObject<T>(store.id, this, buf, *sp, cid, oid);
              result.push_back(std::shared_ptr<T>(sp));
            }
          }
          else {
            T *obj = ti->makeObject(store.id, cid);
            readObject<T>(store.id, this, buf, cid, oid, obj);
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
   * read scalar collection data
   *
   * @param info collection info
   * @param startIndex start index of data to retrieve
   * @param length number of elements to retrieve
   * @param elementSize size of one element
   * @param data (in, out) where to store the retrieved data. If *data != nullptr, data will be copied
   * to target address. Otherwise, if requested data is within one chunk, *data will be set to the database-owned
   * area where trhe chunk resides, if requested data straddles chunks, memory will be allocated and *owned will
   * be true
   * @param owned (out) set to true if a buffer was allocated to store the data
   */
  virtual bool _getCollectionData(
      CollectionInfo *info, size_t startIndex, size_t length, size_t elementSize, void **data, bool *owned) = 0;

  virtual CursorHelper * _openCursor(const std::vector<ClassId> &classIds) = 0;
  virtual CursorHelper * _openCursor(ClassId classId, ObjectId objectId, PropertyId propertyId) = 0;
  virtual CursorHelper * _openCursor(ClassId classId, ObjectId collectionId) = 0;

  virtual void doReset() = 0;
  virtual void doRenew() = 0;
  virtual void doAbort() = 0;

  virtual uint16_t decrementRefCount(ClassId cid, ObjectId oid) = 0;

  void _abort();

  /**
   * @return a cursor ofer a chunked object (e.g., collection)
   */
  virtual ChunkCursor::Ptr _openChunkCursor(ClassId classId, ObjectId objectId, bool atEnd=false) = 0;

public:
  virtual ~Transaction();

  /**
   * @return the storage id
   */
  StoreId storeId() {return store.id;}

  /**
   * @return true if the given object was not previously saved
   */
  template <typename T> bool isNew(const std::shared_ptr<T> &obj)
  {
    return ClassTraits<T>::getObjectKey(obj)->isNew();
  }

  /**
   * retrieve info about a top-level collection. Only valid until the end of the transaction
   *
   * @param collectionId the collectionId. If 0, and create==true, the new collectionid will be placed here
   * @param create reserve a collectionId if not already present
   * @return the collection info or nullptr
   */
  CollectionInfo *getCollectionInfo(ObjectId &collectionId, bool create=true);

  /**
   * load an object from the store using the key generated by a previous call to WriteTransaction::putObject().
   * Non-polymorphical, T must be the exact type of the object. The object is allocated on the heap.
   *
   * @param key the key generated by a previous call to WriteTransaction::putObject()
   * @return the address of the newly allocated + populated object, or nullptr if the key is not defined.
   */
  template<typename T> T *getObject(ObjectKey &key)
  {
    ReadBuf readBuf;
    getData(readBuf, key, ClassTraits<T>::traits_data(store.id).refcounting);

    if(readBuf.null()) return nullptr;

    T *tp = new T();
    readObject<T>(store.id, this, readBuf, *tp, key.classId, key.objectId);

    return tp;
  }

  /**
   * load an object from the store, using the key generated by a previous call to WriteTransaction::putObject()
   * Non-polymorphical, T must be the exact type of the object. The object is allocated on the heap.
   *
   * @return a shared pointer to the object, or an empty shared_ptr if the key does not exist. The shared_ptr contains
   * the ObjectId and can thus be handed to other API that requires a KV-valid shared_ptr
   */
  template<typename T> std::shared_ptr<T> getObject(ObjectId objectId)
  {
    object_handler<T> handler(ClassTraits<T>::traits_data(store.id).classId, objectId);
    return loadObject<T>(handler);
  }

  /**
   * reload an object from the store, Non-polymorphical, T must be the exact type of the object.
   * The new object is allocated on the heap.
   *
   * @return a shared pointer to the object, or an empty shared_ptr if the object does not exist anymore. The shared_ptr contains
   * the ObjectId and can thus be handed to other API that requires a KV-valid shared_ptr
   */
  template<typename T> std::shared_ptr<T> reloadObject(std::shared_ptr<T> &obj)
  {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);
    object_handler<T> handler(*key);
    return loadObject<T>(handler, true);
  }

  /**
   * @return a cursor over all instances of the given class
   */
  template <typename T> typename ClassCursor<T>::Ptr openCursor() {
    using Traits = ClassTraits<T>;
    std::vector<ClassId> classIds = Traits::traits_info->allClassIds(store.id);

    return typename ClassCursor<T>::Ptr(new ClassCursor<T>(_openCursor(classIds), store, this));
  }

  /**
   * @param objectId a valid object ID
   * @param propertyId the propertyId (1-based index into declared properties, obtainable through PROPERTY_ID macro)
   *
   * @return a cursor over the contents of a vector-valued, lazy-loading object property. The cursor will be empty if the
   * given property is not vector-valued
   */
  template <typename T, typename V> typename ClassCursor<V>::Ptr openCursor(ObjectId objectId, PropertyId propertyId) {
    ClassId t_classId = ClassTraits<T>::traits_data(store.id).classId;

    return typename ClassCursor<V>::Ptr(new ClassCursor<V>(_openCursor(t_classId, objectId, propertyId), store, this));
  }

  /**
   * @param obj the object that holds the property
   * @param propertyId the propertyId (1-based index into declared properties, obtainable through PROPERTY_ID macro)
   *
   * @return a cursor over the contents of a vector-valued, lazy-loading object property. The cursor will be empty if the
   * given property is not vector-valued
   */
  template <typename T, typename V> typename ClassCursor<V>::Ptr openCursor(std::shared_ptr<T> obj, PropertyId propertyId) {
    ClassId cid = ClassTraits<T>::traits_data(store.id).classId;
    ObjectId oid = ClassTraits<T>::getObjectKey(obj)->objectId;

    return typename ClassCursor<V>::Ptr(new ClassCursor<V>(_openCursor(cid, oid, propertyId), store, this));
  }

  /**
   * @param collectionId the id of a top-level object collection
   * @return a cursor over the contents of the top-level collection
   */
  template <typename V> typename ObjectCollectionCursor<V>::Ptr openCursor(ObjectId collectionId) {
    return typename ObjectCollectionCursor<V>::Ptr(
        new ObjectCollectionCursor<V>(store.id, collectionId, this, _openChunkCursor(COLLECTION_CLSID, collectionId)));
  }

  /**
   * @param collectionId the id of a top-level collection
   *
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
   * @param vect (out)the contents of the attached collection.
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

      ClassInfo<V> *vi = FIND_CLS(V, store.id, hdl.classId);
      if(!vi) {
        V *vp = ClassTraits<V>::getSubstitute();
        if(vp) {
          loadSubstitute<V>(*vp, hdl.classId, hdl.objectId);
          vect.push_back(std::shared_ptr<V>(vp, hdl));
        }
      }
      else {
        std::shared_ptr<V> obj = loadObject<V>(hdl);
        if(!obj) throw persistence_error("collection object not found");
        vect.push_back(obj);
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
   * load a top-level (chunked) value collection
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
   * Note that the raw data API is only usable for floating point (float, double) and for integral data types that
   * conform to the LP64 data model. This precludes the long data type on Windows platforms
   *
   * @param collectionId the ID of the collection
   * @param startIndex the start index of the data
   * @param length number of elements to retrieve
   * @param data pointer to the memory location of the returned data. If *data is 0, the memory will be either
   * allocated or point into the store-owned memory map, depending on whether the data straddles chunks. If data
   * was allocated, *owned will be set to true. If *data is != 0, it is assumed to be pointing to a data area big enough
   * to receive a copy of the data.
   * @param owned set to true if memory was allocated and should be released by the caller
   *
   * @return the number of bytes actually read
   */
  template <typename T>
  size_t getDataCollection(ObjectId collectionId, size_t startIndex, size_t length, T* &data, bool *owned)
  {
    RAWDATA_API_ASSERT
    CollectionInfo *ci = getCollectionInfo(collectionId);
    if(!ci) return 0;

    if(_getCollectionData(ci, startIndex, length, TypeTraits<T>::byteSize, (void **)&data, owned)) {
      return length;
    }
    return 0;
  }
/**
   * Note that the raw data API is only usable for floating point (float, double) and for integral data types that
   * conform to the LP64 data model. This precludes the long data type on Windows platforms
   *
   * @param collectionId the ID of the collection
   * @param startIndex the start index of the data
   * @param length number of elements to retrieve
   * @param data the memory location to copy to
   *
   * @return the number of bytes actually read
   */  template <typename T>
  size_t getDataCollection(ObjectId collectionId, size_t startIndex, size_t length, T* data)
  {
    return getDataCollection(collectionId, startIndex, length, data, nullptr);
  }

  /**
   * load a member variable of the given, already persistent object. This is only useful for members which are configured
   * as lazy (only Object* properties)
   *
   * @param objectId an Id obtained from a previous put
   * @param obj the persistent object
   * @param pa the property accessor. Note that the template type parameter must refer to the (super-)class which declares the member
   */
  template <typename T>
  void loadMember(ObjectId objectId, T &obj, const PropertyAccessBase *pa)
  {
    using Traits = ClassTraits<T>;

    ReadBuf rb;
    ClassTraits<T>::load(store.id, this, rb, Traits::traits_data(store.id).classId, objectId, &obj, pa, StoreMode::force_all);
  }

  /**
   * load a member variable of the given, already persistent object. This is only useful for members which are configured
   * as lazy (only Object* properties)
   *
   * @param obj the persistent object pointer. Note that this pointer must have been obtained from the KV store
   * @param pa the property accessor. Note that the template type parameter must refer to the (super-)class which declares the member
   */
  template <typename T>
  void loadMember(std::shared_ptr<T> &obj, const PropertyAccessBase *pa)
  {
    using Traits = ClassTraits<T>;

    ObjectKey *objKey = ClassTraits<T>::getObjectKey(obj);

    ReadBuf rb;
    ClassTraits<T>::load(store.id, this, rb, objKey->classId, objKey->objectId, &obj, pa, StoreMode::force_all);
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
};

class ReadTransaction : public virtual Transaction {
public:
  ReadTransaction(KeyValueStore &store) : Transaction(store) {}

  /**
   * end (close) this transaction. Internally, this performs an abort().
   * The transaction must not be used afterward
   */
  void end();
};

/**
 * Transaction for exclusive read and operations. Opening write transactions while an exclusive read is open
 * will fail with an exception. Likewise creating an exclusive read transcation while a write is ongoing. The
 * reason for this is is the fact that the functions in this interface may return pointers to database-owned
 * memory. With LMDB, this memory will be invalidated upon the next write call.
 */
class ExclusiveReadTransaction : public ReadTransaction
{
protected:
  ExclusiveReadTransaction(KeyValueStore &store) : Transaction(store), ReadTransaction(store) {}

public:
  /**
   * Note that the raw data API is only usable for floating point (float, double) and for integral data types that
   * conform to the LP64 data model. This precludes the long data type on Windows platforms
   *
   * @param collectionId the ID of the collection
   * @param startIndex the start index of the data
   * @param length number of elements to retrieve
   *
   * @return a pointer to a memory chunk containing the raw collection data. The memory chunk may be
   * database-owned or copied, depending on whether start and end lie within the same chunk.
   */
  template <typename T>
  typename CollectionData<T>::Ptr getDataCollection(ObjectId collectionId, size_t startIndex, size_t length)
  {
    RAWDATA_API_ASSERT
    void *data = nullptr;
    bool owned = false;
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
  CollectionInfo *m_collectionInfo = nullptr;

protected:
  ObjectId &m_collectionId;
  const size_t m_chunkSize;
  WriteTransaction * const m_tr;

  WriteBuf &m_writeBuf;
  size_t m_elementCount;

  CollectionAppenderBase(WriteTransaction *wtxn, ObjectId &collectionId, size_t chunkSize);

  CollectionInfo *collectionInfo();
  void startChunk(size_t size);

public:
  void close();
};

/**
 * calculate shallow byte size, i.e. the size of the buffer required for properties that dont't get saved under
 * an individual key
 */
template<typename T>
static size_t calculateBuffer(StoreId storeId, T *obj, Properties *properties)
{
  if(properties->fixedSize) return properties->fixedSize;

  size_t size = 0;
  for(unsigned i=0, sz=properties->full_size(); i<sz; i++) {
    auto pa = properties->get(i);

    if(!pa->enabled) continue;

    //calculate variable size
    ClassTraits<T>::addSize(storeId, obj, pa, size);
  }
  return size;
}

/**
 * Transaction for read and write operations. Only one write transaction can be active at a time, and it should
 * be accessed from one thread only
 */
class WriteTransaction : public virtual Transaction
{
  template<typename T, typename V> friend class ValueEmbeddedStorage;
  template<typename T, typename V> friend class ValueKeyedStorage;
  template<typename T, typename V> friend class SimplePropertyStorage;
  template<typename T, typename V> friend class ValueVectorKeyedStorage;
  template<typename T, typename V> friend class ValueSetKeyedStorage;
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

  WriteTransaction(KeyValueStore &store, bool append=false) : Transaction(store), m_append(append) {
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
    Properties *props = Traits::getProperties(store.id, classId);

    if(Traits::needsPrepare(store.id, classId)) {
      ObjectBuf prepBuf(this, classId, objectId, true);
      if(!prepBuf.null()) {
        for(unsigned px=0, sz=props->full_size(); px < sz; px++) {
          const PropertyAccessBase *pa = props->get(px);

          if(!pa->enabled) continue;

          prepBuf.mark();
          size_t psz = ClassTraits<T>::prepareDelete(store.id, this, prepBuf, pa);
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
  void writeObject(ClassId classId, ObjectId objectId, T &obj, PrepareData &pd, Properties *properties, bool shallow)
  {
    //put data into buffer
    for(unsigned px=0, sz=properties->full_size(); px < sz; px++) {
      const PropertyAccessBase *pa = properties->get(px);
      if(!pa->enabled) continue;

      ClassTraits<T>::save(store.id, this, classId, objectId, &obj, pd, pa, shallow ? StoreMode::force_buffer : StoreMode::force_none);
    }
  }

  template<typename T>
  void prepareUpdate(ObjectKey &key, T *obj, PrepareData &pd, Properties *properties)
  {
    LazyBuf prepBuf(this, key, false);

    for(unsigned i=0, sz=properties->full_size(); i<sz; i++) {
      auto pa = properties->get(i);

      if(!pa->enabled) continue;

      prepBuf.mark();
      size_t psz = ClassTraits<T>::prepareUpdate(store.id, prepBuf, pd, obj, pa);
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
  ObjectId save_valueobject(ObjectId objectId, T &obj, bool shallow=false)
  {
    auto &cdata = ClassTraits<T>::traits_data(store.id);
    Properties *properties = ClassTraits<T>::traits_properties;

    size_t size = calculateBuffer(store.id, &obj, properties);
    if(!objectId) objectId = ++cdata.maxObjectId;

    PrepareData pd;
    writeBuf().start(size);
    writeObject(cdata.classId, objectId, obj, pd, properties, shallow);

    if(!putData(cdata.classId, objectId, 0, writeBuf()))
      throw persistence_error("data was not saved");

    writeBuf().reset();
    return objectId;
  }

  /**
   * fully polymorphic, refcounting + caching save
   *
   * @param key the object key
   * @param obj the object to save
   * @param useCache if true, put the object into the cache after saving
   * @param setRefcount if true, set refcount to 1 on this object if a) the object is new and b) refcounting is turned on for the class
   */
  template <typename T>
  void save_object(ObjectKey &key, const std::shared_ptr<T> &obj, bool useCache, bool setRefcount=true)
  {
    if(save_object(key, *obj, setRefcount) && useCache)
      store.objectCaches[key.classId]->put(key.objectId, obj);
  }

  /**
   * fully polymorphic, refcounting save
   *
   * @param key the object key
   * @param obj the object to save
   * @param setRefcount if true, set refcount to 1 on this object if a) the object is new and b) refcounting is turned on for the class
   * @param shallow skip properties that go to separate keys (except pa if present)
   * @param pa property that has changed. If null, everything will be written
   * @return true if the object was newly made persistent
   */
  template <typename T>
  bool save_object(ObjectKey &key, T &obj, bool setRefcount, bool shallow=false, const PropertyAccessBase *pa=nullptr)
  {
    Properties *properties;
    PrepareData pd;

    using Traits = ClassTraits<T>;

    bool poly = Traits::traits_info->isPoly();
    bool isNew = key.isNew();
    if(isNew) {
      if(poly) {
        key.classId = getClassId(typeid(obj));

        AbstractClassInfo *classInfo = store.objectClassInfos.at(key.classId);
        if(!classInfo) throw persistence_error("class not registered");

        if(setRefcount && classInfo->data[store.id].refcounting) key.refcount =  1;

        key.objectId = ++classInfo->data[store.id].maxObjectId;
        properties = store.objectProperties[key.classId];
      }
      else {
        key.classId = Traits::traits_data(store.id).classId;
        key.objectId = ++Traits::traits_data(store.id).maxObjectId;

        if(setRefcount && Traits::traits_data(store.id).refcounting) key.refcount =  1;

        properties = Traits::traits_properties;
      }
    }
    else {
      properties = poly ? store.objectProperties[key.classId] : Traits::traits_properties;
      if(Traits::needsPrepare(store.id, key.classId)) prepareUpdate(key, &obj, pd, properties);
    }

    if(pa && shallow)
      Traits::save(store.id, this, key.classId, key.objectId, &obj, pd, pa, StoreMode::force_property);

    //create the data buffer
    size_t size = calculateBuffer(store.id, &obj, properties);
    writeBuf().start(size);
    writeObject(key.classId, key.objectId, obj, pd, properties, shallow);

    if(!putData(key, writeBuf()))
      throw persistence_error("data was not saved");

    writeBuf().reset();
    return isNew;
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
      ObjectId objectId = ++classInfo->data[store.id].maxObjectId;

      size_t sz = calculateBuffer(store.id, &(*vect[i]), properties) + ObjectHeader_sz;
      helpers[i].set(classId, objectId, sz, properties);
      chunkSize += sz;
    }
    return helpers;
  }

  /**
   * save object collection chunk
   *
   * @param vect the collection
   * @param collectionInfo the collection info
   * @param poly lookup classes dynamically (slight runtime overhead)
   */
  template <typename T, template <typename T> class Ptr>
  void saveChunk(const std::vector<Ptr<T>> &vect, CollectionInfo *collectionInfo, bool poly)
  {
    if(vect.empty()) return;

    PrepareData pd; //dummy, no prepare done for object chunks
    if(poly) {
      size_t chunkSize = 0;
      chunk_helper *helpers = prepare_collection(vect, chunkSize);

      startChunk(collectionInfo, chunkSize, vect.size());

      for(size_t i=0, vectSize = vect.size(); i<vectSize; i++) {
        chunk_helper &helper = helpers[i];

        writeObjectHeader(helper.classId, helper.objectId, helper.size);
        writeObject(helper.classId, helper.objectId, *vect[i], pd, helper.properties, true);
      }
      delete [] helpers;
    }
    else {
      size_t chunkSize = 0;
      size_t *sizes = new size_t[vect.size()];

      for(size_t i=0, vectSize = vect.size(); i<vectSize; i++) {
        sizes[i] = calculateBuffer(store.id, &(*vect[i]), ClassTraits<T>::traits_properties) + ObjectHeader_sz;
        chunkSize += sizes[i];
      }
      startChunk(collectionInfo, chunkSize, vect.size());

      for(size_t i=0, vectSize = vect.size(); i<vectSize; i++) {
        using Traits = ClassTraits<T>;

        ClassData &cdata = Traits::traits_data(store.id);
        ClassId classId = cdata.classId;
        ObjectId objectId = ++cdata.maxObjectId;

        size_t size = sizes[i];
        writeObjectHeader(classId, objectId, size);
        writeObject(classId, objectId, *vect[i], pd, Traits::traits_properties, true);
      }
      delete [] sizes;
    }
  }

  /**
   * save value collection chunk
   *
   * @param vect the collection
   * @param ci the collection info
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
  virtual ~WriteTransaction();

  /**
   * abort this transaction. All changes written since beginnning of the transaction are discarded
   */
  void abort();

  void writeCollections();

  /**
   * commit this transaction. Changes written since beginnning of the transaction are written to persistent storage
   */
  void commit();

  /**
   * put a new object into the KV store. Generate a new ObjectKey and store it inside the returned shared_ptr.
   * The object becomes directly owned by the application.
   *
   * @return a shared_ptr to the input object which contains the ObjectKey
   */
  template <typename T>
  std::shared_ptr<T> putObject(T *obj)
  {
    object_handler<T> handler;
    save_object<T>(handler, *obj, true);
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
    save_object<T>(key, obj, true);
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
    save_object<T>(key, obj, setRefCount);
  }

  /**
   * save object state into the KV store. Use the ObjectKey stored inside the shared_ptr to determine whether
   * a new key will be assigned or an existing key will be overwritten (insert or update).. Update the key accordingly
   *
   * @param obj a persistent object pointer, which must have been obtained from KV
   * @param setRefCount if true and refcounting is configured for T, and the object is new, set refcount to 1
   * @return the object ID
   */
  template <typename T>
  ObjectId saveObject(const std::shared_ptr<T> &obj, bool setRefCount=true)
  {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);
    save_object<T>(*key, obj, store.isCache<T>(), setRefCount);
    return key->objectId;
  }

  /**
   * update a member variable of the given, already persistent object. The current variable state is written to the
   * store
   *
   * @param key a key obtained from a previous put
   * @param obj the persistent object
   * @param pa the member property. Note that the template type parameter must refer to the (super-)class which declares the member
   * @param shallow if true, ignore all mappings that do not target shallow storage
   */
  template <typename T>
  void updateMember(ObjectKey &key, T &obj, const PropertyAccessBase *pa, bool shallow=false)
  {
    switch(pa->storage->layout) {
      case StoreLayout::property: {
        //property goes to a separate key, no need to touch the object buffer
        PrepareData pd;
        if(ClassTraits<T>::needsPrepare(store.id, key.classId)) {
          LazyBuf buf(this, key, false);
          ClassTraits<T>::prepareUpdate(store.id, buf, pd, &obj, pa);
        }
        ClassTraits<T>::save(store.id, this, key.classId, key.objectId, &obj, pd, pa,
                             shallow ? StoreMode::force_buffer : StoreMode::force_all);
        break;
      }
      case StoreLayout::embedded_key:
        //save property value and shallow buffer
        save_object<T>(key, obj, false, true, pa);
        break;
      case StoreLayout::all_embedded:
        //shallow buffer only
        save_object<T>(key, obj, false, true, nullptr);
    }
  }

  /**
   * update a member variable of the given, already persistent object. The current variable state is written to the
   * store
   *
   * @param obj the persistent object pointer. Note that this pointer must have been obtained from the KV store
   * @param pa the member property. Note that the template type parameter must refer to the (super-)class which declares the member
   * @param shallow if true, ignore all mappings that do not target shallow storage
   */
  template <typename T>
  void updateMember(std::shared_ptr<T> &obj, const PropertyAccessBase *pa, bool shallow=false)
  {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);
    updateMember(*key, *obj, pa, shallow);
  }

  /**
   * initialize the given member. Member initialization is required by special property mappings after a new object
   * is created.
   */
  template <typename T>
  void initMember(T &obj, const PropertyAccessBase *pa) {
    ClassTraits<T>::initMember(store.id, this, obj, pa);
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
      if(saveMembers || isNew(v)) {
        pushWriteBuf();
        saveObject(v);
        popWriteBuf();
      }
      writeBuf().append(*ClassTraits<V>::getObjectKey(v));
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
          uint16_t refcount = ClassTraits<V>::traits_data(store.id).refcounting ?
                              decrementRefCount(key.classId, key.objectId) : uint16_t(0);

          if(refcount <= 1) removeObject<V>(key.classId, key.objectId);
        }
      }
    }
    remove(key->classId, key->objectId, propertyId);
  }

  /**
   * delete a top-level (chunked) collection.
   *
   * @param collectionId a collection id. Silently fails if invalid
   * @throw persistence_error if an error occurred
   */
  void deleteCollection(ObjectId collectionId);

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
   * save a top-level (chunked) raw data collection. Note that the raw data API is only usable for
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
   * append to a top-level (chunked) object collection.
   *
   * @param collectionId the id of the collection to apend to
   * @param vect the collection contents
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
   * @param collectionId the id of the collection to apend to
   * @param vect the collection contents
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
   * @param collectionId the id of the collection to apend to
   * @param data the chunk contents
   * @param dataSize size of data
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
   * allocate a new chunk for appending to a top-level (chunked) raw data collection.
   * Note that the raw data API is only usable for floating-point (float, double) and for integral data types that conform to the
   * LP64 data model. This precludes the long data type on Windows platforms
   *
   * Note: the chunk data pointer is only usable until the next update operation, or until the transaction completes
   *
   * @param collectionId the id of the collection to apend to
   * @param data (out) pointer where the address of the chunk ist stored
   * @param dataSize requested chunk data size
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
    if(!key.isValid()) return;
    if(key.refcount > 1) throw persistence_error("removeObject: refcount > 1");
    removeObject<T>(key.classId, key.objectId);
  }

  template <typename T>
  void deleteObject(std::shared_ptr<T> obj) {
    ObjectKey *key = ClassTraits<T>::getObjectKey(obj);
    if(!key->isValid()) return;
    if(key->refcount > 1) throw persistence_error("removeObject: refcount > 1");
    removeObject<T>(key->classId, key->objectId);
  }

  /**
   * clear out refcounting data for all classes in the hierarchy starting at T
   */
  template <typename T>
  void clearRefCounts() {
    clearRefCounts(ClassTraits<T>::traits_info->allClassIds(store.id));
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
      PrepareData pd; //dummy, no prepare in object collections
      if(m_poly) {
        cid = m_tr->getClassId(typeid(obj));

        AbstractClassInfo *classInfo = m_objectClassInfos->at(cid);
        oid = ++classInfo->data[m_tr->store.id].maxObjectId;
        properties = m_objectProperties->at(cid);
      }
      else {
        cid = ClassTraits<T>::traits_data(m_tr->store.id).classId;
        oid = ++ClassTraits<T>::traits_data(m_tr->store.id).maxObjectId;
        properties = ClassTraits<T>::traits_properties;
      }
      size_t size = calculateBuffer(m_tr->store.id, &obj, properties) + ObjectHeader_sz;

      if(collectionInfo()->chunkInfos.empty() || m_writeBuf.avail() < size) startChunk(size);

      m_tr->writeObjectHeader(cid, oid, size);
      m_tr->writeObject(cid, oid, obj, pd, properties, true);

      m_elementCount++;
    }

  public:
    using Ptr = std::shared_ptr<ObjectCollectionAppender>;

    ObjectCollectionAppender(WriteTransaction *wtxn, ObjectId &collectionId,
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

    ValueCollectionAppender(WriteTransaction *wtxn, ObjectId &collectionId, size_t chunkSize)
        : CollectionAppenderBase(wtxn, collectionId, chunkSize)
    {}

    void put(T val)
    {
      size_t sz = TypeTraits<T>::byteSize;
      if(sz == 0) sz = ValueTraits<T>::size(val);

      size_t avail = m_writeBuf.avail();

      if(collectionInfo()->chunkInfos.empty() || avail < sz) startChunk(sz);

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

    DataCollectionAppender(WriteTransaction *wtxn, ObjectId &collectionId, size_t chunkSize)
        : CollectionAppenderBase(wtxn, collectionId, chunkSize)
    {}

    void put(T *val, size_t size)
    {
      size_t avail = m_writeBuf.avail() / sizeof(T);

      if(avail) {
        size_t putsz = avail > size ? size : avail;
        m_writeBuf.append(val, putsz);
        m_elementCount += putsz;
        size -= putsz;
        val += putsz;
      }
      if(collectionInfo()->chunkInfos.empty() || size) {
        size_t dataSize = size * sizeof(T);
        startChunk(dataSize < m_chunkSize ? m_chunkSize : dataSize);
        m_writeBuf.append(val, size);
        m_elementCount += size;
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
      ObjectId &collectionId, size_t chunkSize = DEFAULT_CHUNKSIZE)
  {
    return typename ObjectCollectionAppender<V>::Ptr(new ObjectCollectionAppender<V>(
        this, collectionId, chunkSize, &store.objectClassInfos, &store.objectProperties, ClassTraits<V>::traits_info->isPoly()));
  }

  /**
   * create an appender for the given top-level value collection
   *
   * @param collectionId the id of a top-level collection
   * @param chunkSize the chunk size
   * @return an appender over the contents of the collection.
   */
  template <typename V> typename ValueCollectionAppender<V>::Ptr appendValueCollection(
      ObjectId &collectionId, size_t chunkSize = DEFAULT_CHUNKSIZE)
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
  template <typename T> typename DataCollectionAppender<T>::Ptr appendDataCollection(
      ObjectId &collectionId, size_t chunkSize = DEFAULT_CHUNKSIZE)
  {
    RAWDATA_API_ASSERT
    return typename DataCollectionAppender<T>::Ptr(new DataCollectionAppender<T>(this, collectionId, chunkSize));
  }
};

/**
 * storage class template for scalar types that are saved under an individual key (property id). The type
 * must be supported by a ValueTraits template
 */
template<typename T, typename V>
struct ValueKeyedStorage : public StoreAccessPropertyKey
{
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    WriteBuf propBuf(ValueTraits<V>::size(val));
    ValueTraits<V>::putBytes(propBuf, val);

    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");
  }
  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    ReadBuf readBuf;
    tr->getData(readBuf, classId, objectId, pa->id);

    V val;
    ValueTraits<V>::getBytes(readBuf, val);

    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(tr->store.id, *tp, pa, val);
  }
};

/**
 * storage template for std::vector of scalar values. All values are serialized into one consecutive buffer which is
 * stored under a property key for the given object.
 */
template<typename T, typename V>
struct ValueKeyedStorage<T, std::vector<V>> : public StoreAccessPropertyKey
{
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    size_t psz = 0;
    for(auto &v : val) psz += ValueTraits<V>::size(v);
    if(psz) {
      WriteBuf propBuf(psz);

      for(auto v : val) ValueTraits<V>::putBytes(propBuf, v);

      if(!tr->putData(classId, objectId, pa->id, propBuf))
        throw persistence_error("data was not saved");
    }
  }
  void load(Transaction *tr, ReadBuf &buf,
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
    ClassTraits<T>::get(tr->store.id, *tp, pa, val);
  }
};

/**
 * storage template for std::set of scalar values. All values are serialized into one consecutive buffer which is
 * stored under a property key for the given object.
 */
template<typename T, typename V>
struct ValueKeyedStorage<T, std::set<V>> : public StoreAccessPropertyKey
{
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::set<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    size_t psz = 0;
    for(auto &v : val) psz += ValueTraits<V>::size(v);
    if(psz) {
      WriteBuf propBuf(psz);

      for(auto v : val) ValueTraits<V>::putBytes(propBuf, v);

      if(!tr->putData(classId, objectId, pa->id, propBuf))
        throw persistence_error("data was not saved");
    }
  }
  void load(Transaction *tr, ReadBuf &buf,
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
    ClassTraits<T>::get(tr->store.id, *tp, pa, val);
  }
};

/**
 * storage class template for scalar types that go into the shallow buffer as an opaque value. The type must be
 * supported by a ValueTraits template
 */
template<typename T, typename V>
struct ValueEmbeddedStorage : public StoreAccessBase
{
  ValueEmbeddedStorage() : StoreAccessBase(StoreLayout::all_embedded, TypeTraits<V>::byteSize) {}

  size_t size(StoreId storeId, ObjectBuf &buf) const override
  {
    if(TypeTraits<V>::byteSize) return TypeTraits<V>::byteSize;
    return ValueTraits<V>::size(buf.readBuf);
  }
  size_t size(StoreId storeId, void *obj, const PropertyAccessBase *pa) override
  {
    if(TypeTraits<V>::byteSize) return TypeTraits<V>::byteSize;

    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(storeId, *tp, pa, val);
    return ValueTraits<V>::size(val);
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);
    ValueTraits<V>::putBytes(tr->writeBuf(), val);
  }
  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ValueTraits<V>::getBytes(buf, val);
    ClassTraits<T>::get(tr->store.id, *tp, pa, val);
  }
};

/**
 * storage class template for embedded storage cstring, with dynamic size calculation (type.byteSize is 0).
 * Note that after loading the data store, the pointed-to memory belongs to the datastore and will in all likelihood become
 * invalid by the end of the transaction. It is up to the application to copy the value away (or use std::string)
 */
template<typename T>
struct ValueEmbeddedStorage<T, const char *> : public StoreAccessBase
{
  size_t size(StoreId storeId, ObjectBuf &buf) const override {
    return buf.strlen()+1;
  }
  size_t size(StoreId storeId, void *obj, const PropertyAccessBase *pa) override {
    const char * val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(storeId, *tp, pa, val);
    return strlen(val) + 1;
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    const char * val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);
    ValueTraits<const char *>::putBytes(tr->writeBuf(), val);
  }
  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    const char * val;
    T *tp = reinterpret_cast<T *>(obj);
    ValueTraits<const char *>::getBytes(buf, val);
    ClassTraits<T>::get(tr->store.id, *tp, pa, val);
  }
};

/**
 * storage template class for embedded storage std::string, with dynamic size calculation (type.byteSize is 0)
 */
template<typename T>
struct ValueEmbeddedStorage<T, std::string> : public StoreAccessBase
{
  size_t size(StoreId storeId, ObjectBuf &buf) const override {
    return buf.strlen()+1;
  }
  size_t size(StoreId storeId, void *obj, const PropertyAccessBase *pa) override {
    std::string val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(storeId, *tp, pa, val);
    return val.length() + 1;
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::string val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);
    ValueTraits<std::string>::putBytes(tr->writeBuf(), val);
  }
  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::string val;
    T *tp = reinterpret_cast<T *>(obj);
    ValueTraits<std::string>::getBytes(buf, val);
    ClassTraits<T>::get(tr->store.id, *tp, pa, val);
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

  size_t size(StoreId storeId, ObjectBuf &buf) const override {return 0;}
  size_t size(StoreId storeId, void *obj, const PropertyAccessBase *pa) override {return 0;}

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    //not saved, only loaded
  }
  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(tr->store.id, *tp, pa, objectId);
  }
};

/**
 * storage template for mapped non-pointer object references. Since the object is referenced by value in the enclosing
 * class, storage can only be non-polymorphic. The object is serialized into a separate buffer, but the key is written to the
 * enclosing object's buffer. the referenced object is required to hold an ObjectId-typed member variable which is mapped as
 * OBJECT_ID
 */
template<typename T, typename V> struct ObjectPropertyStorage : public StoreAccessEmbeddedKey
{
  VALUEAPI_ASSERT(V)

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    ClassId childClassId  = ClassTraits<V>::traits_data(tr->store.id).classId;
    auto ida = ClassTraits<V>::objectIdAccess();

    //save the value object
    tr->pushWriteBuf();
    ObjectId childObjectId = tr->save_valueobject(ida->get(val), val);
    ida->set(val, childObjectId);
    tr->popWriteBuf();

    //save the key in this objects write buffer
    tr->writeBuf().append(childClassId, childObjectId);
  }
  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    ClassId cid; ObjectId oid;
    buf.read(cid, oid);

    V v;
    tr->loadObject<V>(cid, oid, v);
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(tr->store.id, *tp, pa, v);
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

  size_t size(StoreId storeId, ObjectBuf &buf) const override {return buf.readInteger<unsigned>(4);}
  size_t size(StoreId storeId, void *obj, const PropertyAccessBase *pa) override {
    size_t sz = ClassTraits<V>::traits_properties->fixedSize;
    if(sz)  return sz + 4;

    V v;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(storeId, *tp, pa, v);
    return calculateBuffer(storeId, &v, ClassTraits<V>::traits_properties) + 4;
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    V val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    ClassId childClassId = ClassTraits<V>::traits_data(tr->store.id).classId;
    size_t sz = ClassTraits<V>::traits_properties->fixedSize;
    if(!sz) sz = calculateBuffer(tr->store.id, &val, ClassTraits<V>::traits_properties);

    tr->writeBuf().appendInteger(sz, 4);
    tr->writeObject(childClassId, 1, val, pd, ClassTraits<V>::traits_properties, true);
  }
  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    ClassId childClassId = ClassTraits<V>::traits_data(tr->store.id).classId;

    V v;
    buf.readInteger<unsigned>(4);
    readObject(tr->store.id, tr, buf, v, childClassId, 1);

    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(tr->store.id, *tp, pa, v);
  }
};

/**
 * storage template for shared_ptr-based mapped object references. Fully polymorphic
 */
template<typename T, typename V>
class ObjectPtrPropertyStorage : public StoreAccessEmbeddedKey
{
protected:
  const bool m_lazy;

public:
  ObjectPtrPropertyStorage(bool lazy=false) : m_lazy(lazy) {}

  bool preparesUpdates(StoreId storeId, ClassId classId) override
  {
    return ClassTraits<V>::traits_info->hasClassId(storeId, classId);
  }
  size_t prepareUpdate(StoreId storeId, ObjectBuf &buf, PrepareData &pd, void *obj, const PropertyAccessBase *pa) override
  {
    if(ClassTraits<V>::traits_data(storeId).refcounting) {
      //read pre-update state
      PrepareData::Entry &pe = pd.entry(pa->id);
      buf.read(pe.prepareCid, pe.prepareOid);
    }
    return size(storeId, buf);
  }
  size_t prepareDelete(StoreId storeId, WriteTransaction *tr, ObjectBuf &buf, const PropertyAccessBase *pa) override {
    if(ClassTraits<V>::traits_data(storeId).refcounting) {
      ClassId cid; ObjectId oid;
      buf.read(cid, oid);
      if(cid && oid && tr->decrementRefCount(cid, oid) <= 1) tr->removeObject<V>(cid, oid);
    }
    return size(storeId, buf);
  }

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::shared_ptr<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    bool refcount = ClassTraits<V>::traits_data(tr->store.id).refcounting;
    PrepareData::Entry &pe = pd.entry(pa->id);
    if(val) {
      //save the pointed-to object
      ObjectKey *childKey = ClassTraits<V>::getObjectKey(val);

      if(mode != StoreMode::force_buffer) {
        if(refcount && pe.prepareOid && pe.prepareOid != childKey->objectId) {
          //previous object reference is about to be overwritten. Delete if refcount == 1
          if(tr->decrementRefCount(pe.prepareCid, pe.prepareOid) <= 1) tr->removeObject<V>(pe.prepareCid, pe.prepareOid);
          childKey->refcount++;
        }
        tr->pushWriteBuf();
        tr->save_object<V>(*childKey, val, tr->store.isCache<T>());
        tr->popWriteBuf();
      }
      if(mode != StoreMode::force_property) {
        //save the key in this objects write buffer
        tr->writeBuf().append(*childKey);
      }
    }
    else {
      if(refcount && mode != StoreMode::force_buffer && pe.prepareOid && tr->decrementRefCount(pe.prepareCid, pe.prepareOid) <= 1)
        tr->removeObject<V>(pe.prepareCid, pe.prepareOid);

      //save a placeholder in this objects write buffer
      if(mode != StoreMode::force_property)
        tr->writeBuf().append(ObjectKey::NIL);
    }
    pe.reset();
  }
  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    if(m_lazy && mode == StoreMode::force_none) {
      buf.read(ObjectKey_sz);
      return;
    }

    object_handler<V> handler;
    buf.read(handler);

    std::shared_ptr<V> vp;
    ClassInfo<V> *vi = FIND_CLS(V, tr->store.id, handler.classId);
    if(!vi) {
      V *v = ClassTraits<V>::getSubstitute();
      if(v) {
        tr->loadSubstitute<V>(*v, handler.classId, handler.objectId);
        vp = std::shared_ptr<V>(v, handler);
      }
    }
    else {
      vp = tr->loadObject<V>(handler);
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(tr->store.id, *tp, pa, vp);
  }
};

/**
 * storage template for shared_ptr-based object references which are directly serialized into the enclosing object's
 * buffer. Storage is shallow-only, i.e. properties of the referred-to object which are not themselves shallow-serializable
 * are ignored. Fully polymorphic
 */
template<typename T, typename V> struct ObjectPtrPropertyStorageEmbedded : public StoreAccessBase
{
  size_t size(StoreId storeId, ObjectBuf &buf) const override {
    buf.read(ClassId_sz);
    unsigned objSize = buf.readInteger<unsigned>(4);
    return ClassId_sz + 4 + objSize;
  }
  size_t size(StoreId storeId, void *obj, const PropertyAccessBase *pa) override {
    std::shared_ptr<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(storeId, *tp, pa, val);

    return ClassTraits<V>::bufferSize(storeId, &(*val)) + ClassId_sz + 4;
  }
  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::shared_ptr<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    ClassId childClassId = 0;
    size_t sz = val ? ClassTraits<V>::bufferSize(tr->store.id, &(*val), &childClassId) : 0;

    tr->writeBuf().appendInteger(childClassId, ClassId_sz);
    tr->writeBuf().appendInteger(sz, 4);

    if(val) tr->writeObject(childClassId, 1, *val, pd, ClassTraits<V>::getProperties(tr->store.id, childClassId), true);
  }
  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::shared_ptr<V> val;
    ClassId childClassId = buf.readInteger<ClassId>(ClassId_sz);
    unsigned sz = buf.readInteger<unsigned>(4);
    if(sz) {
      ClassInfo<V> *vi = FIND_CLS(V, tr->store.id, childClassId);
      if(!vi) {
        buf.mark();
        V *vp = ClassTraits<V>::getSubstitute();
        if(vp) {
          readObject<V>(tr->store.id, tr, buf, *vp, childClassId, 1, StoreMode::force_buffer);
          val.reset(vp);
        }
        buf.unmark(sz);
      }
      else {
        V *vp = vi->makeObject(tr->store.id, childClassId);
        readObject<V>(tr->store.id, tr, buf, childClassId, 1, vp);
        val.reset(vp);
      }
      T *tp = reinterpret_cast<T *>(obj);
      ClassTraits<T>::get(tr->store.id, *tp, pa, val);
    }
  }
};

/**
 * storage template for mapped object vectors. Value-based, therefore not polymorphic. Collection objects are serialized
 * into a secondary buffer which is stored under the PropertyId
 */
template<typename T, typename V> class ObjectVectorPropertyStorage : public StoreAccessPropertyKey
{
  bool m_lazy;

public:
  ObjectVectorPropertyStorage(bool lazy) : m_lazy(lazy) {}

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    if(m_lazy && mode == StoreMode::force_none) return;

    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    size_t psz = sizeof(ClassId) + sizeof(size_t);
    Properties *properties = ClassTraits<V>::traits_properties;

    tr->pushWriteBuf();
    for(V &v : val) psz += calculateBuffer(tr->store.id, &v, properties);
    tr->writeBuf().start(psz);

    ClassId childClassId = ClassTraits<V>::traits_data(tr->store.id).classId;
    tr->writeBuf().appendRaw(childClassId);
    tr->writeBuf().appendRaw((size_t)val.size());

    //write vector
    for(V &v : val) tr->writeObject(childClassId, 0, v, pd, properties, false);

    //put vector into a separate property key
    if(!tr->putData(classId, objectId, pa->id, tr->writeBuf()))
      throw persistence_error("data was not saved");

    tr->popWriteBuf();
  }

  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    if(m_lazy && mode == StoreMode::force_none) return;

    std::vector<V> val;

    //load vector base (array of object keys) data from property key
    ReadBuf readBuf;
    tr->getData(readBuf, classId, objectId, pa->id);

    if(!readBuf.null()) {
      ClassId cid = readBuf.readRaw<ClassId>();
      size_t count = readBuf.readRaw<size_t>();
      for(int i=0; i<count;i++) {
        V v;
        readObject<V>(tr->store.id, tr, readBuf, cid, 0, &v);
        val.push_back(v);
      }
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(tr->store.id, *tp, pa, val);
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
  size_t size(StoreId storeId, ObjectBuf &buf) const override {
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
  size_t size(StoreId storeId, void *obj, const PropertyAccessBase *pa) override {
    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(storeId, *tp, pa, val);

    size_t sz = ClassTraits<V>::traits_properties->fixedSize;
    if(sz) return val.size() * (sz + 4) + 4;

    for(V &v : val)
      sz += calculateBuffer(storeId, &v, ClassTraits<V>::traits_properties) + 4;
    return sz + 4;
  }

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<V> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    tr->writeBuf().appendInteger((unsigned)val.size(), 4);
    ClassId childClassId = ClassTraits<V>::traits_data(tr->store.id).classId;
    PropertyId childObjectId = 0;
    size_t fsz = ClassTraits<V>::traits_properties->fixedSize;
    for(V &v : val) {
      size_t sz = fsz ? fsz : calculateBuffer(tr->store.id, &v, ClassTraits<V>::traits_properties);

      tr->writeBuf().appendInteger(sz, 4);
      tr->writeObject(childClassId, ++childObjectId, v, pd, ClassTraits<V>::traits_properties, true);
    }
  }

  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<V> val;

    ClassId childClassId = ClassTraits<V>::traits_data(tr->store.id).classId;
    PropertyId childObjectId = 0;

    unsigned sz = buf.readInteger<unsigned>(4);
    for(size_t i=0; i< sz; i++) {
      V v;
      buf.readInteger<unsigned>(4);
      readObject(tr->store.id, tr, buf, v, childClassId, ++childObjectId);
      val.push_back(v);
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(tr->store.id, *tp, pa, val);
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
  const bool m_lazy;
  const PropertyAccessBase *inverse = nullptr;

  bool preparesUpdates(StoreId storeId, ClassId classId) override
  {
    return ClassTraits<V>::traits_info->hasClassId(storeId, classId);
  }
  size_t prepareUpdate(StoreId storeId, ObjectBuf &buf, PrepareData &pd, void *obj, const PropertyAccessBase *pa) override
  {
    PrepareData::Entry &pe = pd.entry(pa->id);
    pe.updatePrepared = ClassTraits<V>::traits_data(storeId).refcounting;
    return size(storeId, buf);
  }
  size_t prepareDelete(StoreId storeId, WriteTransaction *tr, ObjectBuf &buf, const PropertyAccessBase *pa) override
  {
    if(ClassTraits<V>::traits_data(storeId).refcounting) {
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
          if(tr->decrementRefCount(key.classId, key.objectId) <= 1) tr->removeObject<V>(key.classId, key.objectId);
        }
      }
    }
    tr->remove(buf.key.classId, buf.key.objectId, pa->id);
    return size(storeId, buf);
  }

  void loadStoredKeys(Transaction *tr, ClassId cid, ObjectId oid, PropertyId pid, std::set<ObjectKey> &keys)
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
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    if(m_lazy && mode == StoreMode::force_none) return;

    std::vector<std::shared_ptr<V>> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    std::set<ObjectKey> oldKeys;

    PrepareData::Entry &pe = pd.entry(pa->id);
    if (pe.updatePrepared && mode != StoreMode::force_buffer)
      loadStoredKeys(tr, classId, objectId, pa->id, oldKeys);
    pe.reset();

    size_t psz = ObjectKey_sz * val.size();
    WriteBuf propBuf(psz);

    bool useCache = tr->store.isCache<T>();

    tr->pushWriteBuf();
    for(std::shared_ptr<V> &v : val) {
      ObjectKey *childKey = ClassTraits<V>::getObjectKey(v);

      if(mode != StoreMode::force_buffer) {
        if(ClassTraits<V>::traits_data(tr->store.id).refcounting && !childKey->isNew()) {
          if(oldKeys.empty() || !oldKeys.erase(*childKey)) childKey->refcount++;
        }
        tr->save_object<V>(*childKey, v, useCache);
      }
      propBuf.append(*childKey);
    }
    tr->popWriteBuf();

    //vector goes into a separate property key
    if(!tr->putData(classId, objectId, pa->id, propBuf))
      throw persistence_error("data was not saved");

    //cleanup orphaned objects
    for(auto &key : oldKeys) {
      if(tr->decrementRefCount(key.classId, key.objectId) <= 1) tr->removeObject<V>(key.classId, key.objectId);
    }
  }

  void load(Transaction *tr, ReadBuf &buf,
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
        ClassInfo<V> *vi = FIND_CLS(V, tr->store.id, handler.classId);
        if(!vi) {
          V *vp = ClassTraits<V>::getSubstitute();
          if(vp) {
            tr->loadSubstitute<V>(*vp, handler.classId, handler.objectId);
            val.push_back(std::shared_ptr<V>(vp, handler));
          }
        }
        else {
          std::shared_ptr<V> obj = tr->loadObject<V>(handler);
          if(obj) {
            if(inverse) ClassTraits<V>::get(tr->store.id, *obj, inverse, tp);
            val.push_back(obj);
          }
        }
      }
    }
    ClassTraits<T>::get(tr->store.id, *tp, pa, val);
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
  size_t size(StoreId storeId, ObjectBuf &buf) const override {
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
  size_t size(StoreId storeId, void *obj, const PropertyAccessBase *pa) override {
    std::vector<std::shared_ptr<V>> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(storeId, *tp, pa, val);

    size_t sz = ClassTraits<V>::traits_properties->fixedSize;
    if(sz) return val.size() * (sz + ClassId_sz + 4) + 4;

    for(std::shared_ptr<V> &v : val) {
      sz += ClassTraits<V>::bufferSize(storeId, &(*v)) + ClassId_sz + 4;
    }
    return sz + 4;
  }

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<std::shared_ptr<V>> val;
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::put(tr->store.id, *tp, pa, val);

    tr->writeBuf().appendInteger((unsigned)val.size(), 4);
    PropertyId childObjectId = 0;
    for(std::shared_ptr<V> &v : val) {
      ClassId childClassId;
      size_t sz = ClassTraits<V>::bufferSize(tr->store.id, &(*v), &childClassId);

      tr->writeBuf().appendInteger(childClassId, ClassId_sz);
      tr->writeBuf().appendInteger(sz, 4);
      tr->writeObject(childClassId, ++childObjectId, *v, pd, ClassTraits<V>::getProperties(tr->store.id, childClassId), true);
    }
  }

  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    std::vector<std::shared_ptr<V>> val;

    PropertyId childObjectId = 0;

    unsigned len = buf.readInteger<unsigned>(4);
    for(size_t i=0; i<len; i++) {
      ClassId childClassId = buf.readInteger<ClassId>(ClassId_sz);
      unsigned sz = buf.readInteger<unsigned>(4);

      ClassInfo<V> *vi = FIND_CLS(V, tr->store.id, childClassId);
      if(!vi) {
        buf.mark();
        V *vp = ClassTraits<V>::getSubstitute();
        if(vp) {
          readObject<V>(tr->store.id, tr, buf, *vp, childClassId, ++childObjectId, StoreMode::force_buffer);
          val.push_back(std::shared_ptr<V>(vp));
        }
        buf.unmark(sz);
      }
      else {
        V *vp = vi->makeObject(tr->store.id, childClassId);
        readObject<V>(tr->store.id, tr, buf, childClassId, ++childObjectId, vp);
        val.push_back(std::shared_ptr<V>(vp));
      }
    }
    T *tp = reinterpret_cast<T *>(obj);
    ClassTraits<T>::get(tr->store.id, *tp, pa, val);
  }
};

/**
 * storage template for collection iterator members. the top-level collection id is saved under a separate PropertyId.
 */
template<typename T, typename V, typename KVIter, typename Iter>
struct CollectionIterPropertyStorage : public StoreAccessPropertyKey
{
  static_assert(std::is_base_of<IterPropertyBackend, KVIter>::value, "KVIter must subclass IterPropertyBackend");
  static_assert(std::is_base_of<Iter, KVIter>::value, "KVIter must subclass Iter");

  CollectionIterPropertyStorage() : StoreAccessPropertyKey() {}

  void initMember(WriteTransaction *tr, void *obj, const PropertyAccessBase *pa) override
  {
    T *tp = reinterpret_cast<T *>(obj);
    std::shared_ptr<KVIter> iter;
    ClassTraits<T>::put(tr->storeId(), *tp, pa, iter);

    if(!iter) {
      KVIter *it = new KVIter();
      iter = std::shared_ptr<KVIter>(it);
      ClassTraits<T>::get(tr->storeId(), *tp, pa, iter);

      it->init(tr);
    }
  }

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId, void *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode) override
  {
    T *tp = reinterpret_cast<T *>(obj);
    std::shared_ptr<KVIter> iter;
    ClassTraits<T>::put(tr->storeId(), *tp, pa, iter);

    if(iter && iter->getCollectionId()) {
      WriteBuf wb(sizeof(ObjectId));
      wb.appendRaw(iter->getCollectionId());
      tr->putData(classId, objectId, pa->id, wb);
    }
  }

  void load(Transaction *tr, ReadBuf &buf,
            ClassId classId, ObjectId objectId, void *obj, const PropertyAccessBase *pa, StoreMode mode) override
  {
    T *tp = reinterpret_cast<T *>(obj);

    ReadBuf rb;
    tr->getData(rb, classId, objectId, pa->id);
    ObjectId collectionId = rb.empty() ? 0 : rb.readRaw<ObjectId>();

    std::shared_ptr<KVIter> it = std::make_shared<KVIter>();
    it->setCollectionId(collectionId);

    it->load(tr);
    ClassTraits<T>::get(tr->store.id, *tp, pa, it);
  }
};

/**
 * mapping configuration for simple types
 */
template <typename O, typename P, template <typename Ox, typename Px> class S, P O::*p>
struct ValuePropertyAssign : public PropertyAssign<O, P, p> {
  ValuePropertyAssign(const char * name)
      : PropertyAssign<O, P, p>(name, new S<O, P>(), PROPERTY_TYPE(P)) {}
};

/**
 * mapping configuration for simple types that are stored under individual property keys
 */
template <typename O, typename P, P O::*p>
struct ValuePropertyKeyedAssign : public ValuePropertyAssign<O, P, ValueKeyedStorage, p> {
  ValuePropertyKeyedAssign(const char * name)
      : ValuePropertyAssign<O, P, ValueKeyedStorage, p>(name) {}
};

/**
 * mapping configuration for simple types that are stored directly into the shallow buffer
 */
template <typename O, typename P, P O::*p>
struct ValuePropertyEmbeddedAssign : public ValuePropertyAssign<O, P, ValueEmbeddedStorage, p> {
  ValuePropertyEmbeddedAssign(const char * name)
      : ValuePropertyAssign<O, P, ValueEmbeddedStorage, p>(name) {}
};

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
/**
 * mapping configuration for a property which holds an object iterator. An object iterator has access to a collectionId which refers to
 * a top-level object collection.
 */
template <typename O, typename P, typename KVIter, typename Iter, std::shared_ptr<Iter> O::*p>
struct ValueCollectionIterPropertyAssign : public PropertyAssign<O, std::shared_ptr<Iter>, p> {
  ValueCollectionIterPropertyAssign(const char * name)
      : PropertyAssign<O, std::shared_ptr<Iter>, p>(name, new CollectionIterPropertyStorage<O, P, KVIter, Iter>(), PROPERTY_TYPE_VECT(P)) {}
};

} //kv
} //persistence
} //flexis

#endif //FLEXIS_FLEXIS_KVSTORE_H

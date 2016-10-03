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

#ifndef FLEXIS_FLEXIS_KVTRAITS_H
#define FLEXIS_FLEXIS_KVTRAITS_H

#include <string>
#include <vector>
#include <set>
#include <sstream>
#include <cstring>
#include <typeinfo>
#include <stdexcept>
#include <stdint.h>
#include <type_traits>
#include "kvbuf.h"

namespace flexis {
namespace persistence {
namespace kv {

/** constant designating the maximum number of databases allowed within one process */
unsigned const MAX_DATABASES = 10;

#define ARRAY_SZ(x) unsigned(sizeof(x) / sizeof(decltype(*x)))

#if _MSC_VER && _MSC_VER < 1900
#define LO_NOEXCEPT
#else
#define LO_NOEXCEPT noexcept
#endif

class error : public std::exception
{
  std::string m_msg, m_detail;

public:
  explicit error(const std::string& msg, const std::string& detail="")
      : m_msg(msg), m_detail(detail) {};

  virtual ~error() LO_NOEXCEPT {};

  const char* what() const LO_NOEXCEPT {return m_msg.c_str();}
  const char* detail() const LO_NOEXCEPT {return m_detail.c_str();}
};

class invalid_pointer_error : public error
{
public:
  invalid_pointer_error() : error("invalid pointer argument: not created by KV store", "") {}
};
class invalid_classid_error : public error
{
  std::string mk(const char *msg, ClassId cid) {
    std::stringstream ss;
    ss << msg << cid;
    return ss.str();
  }
public:
  invalid_classid_error(ClassId cid) : error(mk("invalid classid: ", cid), "is class registered?") {}
};

struct PropertyType
{
  //predefined base type id, irrelevant if className is set
  const ClassId id;

  //it's a vector
  const bool isVector;

  //number of bytes, 0 if variable size (e.g. string). For a vector, this is the byteSize of the elements
  const unsigned byteSize;

  //name of the mapped type if this is an object type
  const char *className;

  PropertyType(unsigned id, unsigned byteSize, bool isVector=false)
      : id(id), isVector(isVector), byteSize(byteSize), className(nullptr) {}
  PropertyType(const char *clsName, bool isVector=false) :
      id(0), isVector(isVector), byteSize(ObjectKey_sz), className(clsName) {}

  bool operator == (const PropertyType &other) const {
    return id == other.id
           && isVector == other.isVector
           && className == other.className;
  }
};

#define RAWDATA_API_ASSERT(_T) static_assert(TypeTraits<_T>::byteSize == sizeof(_T), \
"collection data access only supported for fixed-size types with native size equal byteSize");

template <typename T> struct TypeTraits;

/**
 * macro for declaring basic types with static id. Also declares corresponding vector and set container types.
 */
#define KV_TYPEDEF_SV(__type, __id, __bytes) template <> struct TypeTraits<__type> {\
static const ClassId id=__id; static const unsigned byteSize=__bytes; static const bool isVect=false;\
}; \
template <> struct TypeTraits<std::vector<__type>> {\
static const ClassId id=__id; static const unsigned byteSize=__bytes; static const bool isVect=true;\
}; \
template <> struct TypeTraits<std::set<__type>> {\
static const ClassId id=__id; static const unsigned byteSize=__bytes; static const bool isVect=true;\
};

/*
 * predefined basic types with static ids. Static ids must stay below 100
 */
KV_TYPEDEF_SV(short, 			         1, 2)
KV_TYPEDEF_SV(unsigned short, 	   2, 2)
KV_TYPEDEF_SV(int, 			           3, 4)
KV_TYPEDEF_SV(unsigned int, 		   4, 4)
KV_TYPEDEF_SV(long, 			         5, 8)
KV_TYPEDEF_SV(unsigned long, 		   6, 8)
KV_TYPEDEF_SV(long long, 		       7, 8)
KV_TYPEDEF_SV(unsigned long long,	 8, 8)
KV_TYPEDEF_SV(bool, 			         9, 1)
KV_TYPEDEF_SV(float, 			        10, 4)
KV_TYPEDEF_SV(double, 			      11, 8)
KV_TYPEDEF_SV(const char *, 		  13, 0)
KV_TYPEDEF_SV(std::string, 		    13, 0)

static const ClassId MIN_VALUETYPE = 100;

//these assertions must hold because certain elmements are written/read natively
static_assert(sizeof(ClassId) == TypeTraits<ClassId>::byteSize, "ClassId: byteSize must match native size");
static_assert(sizeof(ObjectId) == TypeTraits<ObjectId>::byteSize, "ObjectId: byteSize must match native size");
static_assert(sizeof(PropertyId) == TypeTraits<PropertyId>::byteSize, "PropertyId: byteSize must match native size");
static_assert(sizeof(size_t) == TypeTraits<size_t>::byteSize, "size_t: byteSize must match native size");

class Transaction;
class WriteTransaction;
struct PropertyAccessBase;
class ObjectBuf;
class PrepareData;

enum class StoreMode {force_none, force_all, force_buffer, force_property};

enum class StoreLayout {all_embedded, embedded_key, property, none};

/**
 * base information about a property store
 */
struct StoreInfo
{
  const StoreLayout layout;
  size_t fixedSize;

  /**
   * called at schema initialization. Override if applicable
   */
  virtual void init(const PropertyAccessBase *pa) {}

  /**
   * determine whether this storage participates in update/delete preparation
   *
   * @return whether this mapping participates in update/delete preparation
   */
  virtual bool preparesUpdates(StoreId storeId, ClassId classId) {return false;}

  /**
   * determine the size from a serialized buffer. The buffer's read position is at the start of this object's data
   *
   * @return the size of this object
   */
  virtual size_t size(StoreId storeId, ObjectBuf &buf) const = 0;

protected:
  StoreInfo(StoreLayout layout=StoreLayout::all_embedded, size_t fixedSize=0)
    : layout(layout), fixedSize(fixedSize) {}
};

/**
 * abstract superclass for classes that handle serializing mapped values to the datastore
 */
template <typename T>
struct StoreAccessBase : public StoreInfo
{
  StoreAccessBase(StoreLayout layout=StoreLayout::all_embedded, size_t fixedSize=0)
      : StoreInfo(layout, fixedSize) {}

  /**
   * determine the size from a live object
   *
   * @return the buffer size required to save the given property value
   */
  virtual size_t size(StoreId storeId, T *obj, const PropertyAccessBase *pa) const {return 0;}

  /**
   * prepare an update for the given object property. This function is called on dependent objects (objects which
   * are referenced through a pointer or a collection) whenever the parent object is updated
   *
   * @param storeId the store id
   * @param buf the object data as currently saved
   * @param pd prepare data cache used by storage objects
   * @param obj the object about to be saved
   * @param pa the property about to be saved
   */
  virtual void prepareUpdate(StoreId storeId, ObjectBuf &buf, PrepareData &pd, T *obj, const PropertyAccessBase *pa) const {
  }

  /**
   * prepare a delete for the given object property. This function is called on dependent objects (objects which
   * are referenced through a pointer or a collection) whenever the parent object is deleted
   *
   * @param storeId the store id
   * @param tr the write transaction
   * @param buf the object data as currently saved
   * @param pa the property represented by this storage
   */
  virtual void prepareDelete(StoreId storeId, WriteTransaction *tr, ObjectBuf &buf, const PropertyAccessBase *pa) const {
  }

  /** default member intialization does nothing*/
  virtual void initMember(WriteTransaction *tr, T *obj, const PropertyAccessBase *pa) const {
  }

  virtual void save(WriteTransaction *tr,
                    ClassId classId, ObjectId objectId,
                    T *obj,
                    PrepareData &pd,
                    const PropertyAccessBase *pa,
                    StoreMode mode=StoreMode::force_none) const = 0;

  virtual void load(Transaction *tr,
                    ReadBuf &buf,
                    ClassId classId, ObjectId objectId,
                    T *obj, const PropertyAccessBase *pa,
                    StoreMode mode=StoreMode::force_none) const = 0;
};

/**
 * storage access class that stores to dev/null
 */
struct NullStorage : public StoreAccessBase<void> {
  NullStorage() : StoreAccessBase<void>(StoreLayout::none, 0) { }

  size_t size(StoreId storeId, ObjectBuf &buf) const override { return 0; }

  void save(WriteTransaction *tr,
            ClassId classId, ObjectId objectId,
            void *obj,
            PrepareData &pb,
            const PropertyAccessBase *pa,
            StoreMode mode = StoreMode::force_none) const override { }

  void load(Transaction *tr,
            ReadBuf &buf,
            ClassId classId, ObjectId objectId,
            void *obj, const PropertyAccessBase *pa,
            StoreMode mode = StoreMode::force_none) const override { }
};

/**
 * abstract superclass for all Store Access classes that represent mapped-object valued properties where
 * the referred-to object is saved individually and key value saved in the enclosing object's buffer
 */
template <typename T>
struct StoreAccessEmbeddedKey : public StoreAccessBase<T>
{
  StoreAccessEmbeddedKey() : StoreAccessBase<T>(StoreLayout::embedded_key, ObjectKey_sz) {}

  size_t size(StoreId storeId, ObjectBuf &buf) const override {return ObjectKey_sz;}
  size_t size(StoreId storeId, T *obj, const PropertyAccessBase *pa) const override {return ObjectKey_sz;}
};

/**
 * abstract superclass for all Store Access classes that represent mapped-object valued properties that are saved
 * under a property key, with nothing saved in the enclosing object's buffer
 */
template <typename T>
struct StoreAccessPropertyKey: public StoreAccessBase<T>
{
  StoreAccessPropertyKey() : StoreAccessBase<T>(StoreLayout::property) {}

  size_t size(StoreId storeId, ObjectBuf &buf) const override {return 0;}
  size_t size(StoreId storeId, T *obj, const PropertyAccessBase *pa) const override {return 0;}
};

/**
 * base class for value handler templates.
 */
template <bool Fixed>
struct ValueTraitsBase
{
  const bool fixed;
  ValueTraitsBase() : fixed(Fixed) {}
};

/**
 * base class for single-byte value handlers
 */
template <typename T>
struct ValueTraitsByte : public ValueTraitsBase<true>
{
  static size_t size(ReadBuf &buf) {
    return 1;
  }
  static size_t size(const T &val) {
    return 1;
  }
  static void getBytes(ReadBuf &buf, T &val) {
    const byte_t *data = buf.read(1);
    val = (T)*data;
  }
  static void putBytes(WriteBuf &buf, T val) {
    byte_t *data = buf.allocate(1);
    *data = (byte_t)val;
  }
};

/**
 * base template value handler for fixed size values
 */
template <typename T>
struct ValueTraits : public ValueTraitsBase<true>
{
  static size_t size(ReadBuf &buf) {
    return TypeTraits<T>::byteSize;
  }
  static size_t size(const T &val) {
    return TypeTraits<T>::byteSize;
  }
  static void getBytes(ReadBuf &buf, T &val) {
    size_t byteSize = TypeTraits<T>::byteSize;
    const byte_t *data = buf.read(byteSize);
    val = read_integer<T>(data, byteSize);
  }
  static void putBytes(WriteBuf &buf, T &val) {
    size_t byteSize = TypeTraits<T>::byteSize;
    byte_t *data = buf.allocate(byteSize);
    write_integer(data, val, byteSize);
  }
};

/**
 * base template value handler for enums
 */
template <typename E>
struct ValueTraitsEnum : public ValueTraitsBase<true>
{
  using underlying_t = typename std::underlying_type<E>::type;
  RAWDATA_API_ASSERT(underlying_t)

  static size_t size(ReadBuf &buf) {
    return TypeTraits<E>::byteSize;
  }
  static size_t size(const E &val) {
    return TypeTraits<E>::byteSize;
  }
  static void getBytes(ReadBuf &buf, E &val) {
    val = static_cast<E>(buf.readRaw<underlying_t>());
  }
  static void putBytes(WriteBuf &buf, E &val) {
    buf.appendRaw(static_cast<underlying_t>(val));
  }
};

/**
 * value handler specialization for boolean values
 */
template <>
struct ValueTraits<bool> : public ValueTraitsBase<true>
{
  static size_t size(ReadBuf &buf) {
    return TypeTraits<bool>::byteSize;
  }
  static size_t size(const bool &val) {
    return TypeTraits<bool>::byteSize;
  }
  static void getBytes(ReadBuf &buf, bool &val) {
    const byte_t *data = buf.read(1);
    val = *data != 0;
  }
  static void putBytes(WriteBuf &buf, bool val) {
    byte_t *data = buf.allocate(1);
    *data = byte_t(val ? 1 : 0);
  }
};

/**
 * value handler specialization for string values
 */
template <>
struct ValueTraits<std::string> : public ValueTraitsBase<false>
{
  static size_t size(ReadBuf &buf) {
    return buf.strlen() +1;
  }
  static size_t size(const std::string &val) {
    return val.length() + 1;
  }
  static void getBytes(ReadBuf &buf, std::string &val) {
    val = (const char *)buf.read(0);
    buf.read(val.length() +1); //move the pointer
  }
  static void putBytes(WriteBuf &buf, std::string &val) {
    buf.append(val.data(), val.length()+1);
  }
};

/**
 * value handler specialization for C string values
 */
template <>
struct ValueTraits<const char *> : public ValueTraitsBase<false>
{
  static size_t size(ReadBuf &buf) {
    return buf.strlen() +1;
  }
  static size_t size(const char * const &val) {
    return strlen(val) + 1;
  }
  static void getBytes(ReadBuf &buf, const char *&val) {
    val = buf.readCString();
  }
  static void putBytes(WriteBuf &buf, const char *val) {
    buf.appendCString(val);
  }
};

/**
 * value handler base class for float values
 */
template <typename T>
struct ValueTraitsFloat : public ValueTraitsBase<true>
{
  static size_t size(ReadBuf &buf) {
    return TypeTraits<T>::byteSize;
  }
  static size_t size(const T &val) {
    return TypeTraits<T>::byteSize;
  }
  static void getBytes(ReadBuf &buf, T &val) {
    size_t byteSize = TypeTraits<T>::byteSize;
    const byte_t *data = buf.read(byteSize);
    val = *reinterpret_cast<const T *>(data);
  }
  static void putBytes(WriteBuf &buf, T val) {
    size_t byteSize = TypeTraits<T>::byteSize;
    byte_t *data = buf.allocate(byteSize);
    *reinterpret_cast<T *>(data) = val;
  }
};

#define PROPERTY_TYPE(P) PropertyType(TypeTraits<P>::id, TypeTraits<P>::byteSize, TypeTraits<P>::isVect)
#define PROPERTY_TYPE_VECT(P) PropertyType(TypeTraits<P>::id, TypeTraits<P>::byteSize, true)

/**
 * value handler specialization for float values
 */
template <>
struct ValueTraits<float> : public ValueTraitsFloat<float> {};
/**
 * value handler specialization for double values
 */
template <>
struct ValueTraits<double> : public ValueTraitsFloat<double> {};

class Properties;

/**
 * non-templated base class for property accessors
 */
struct PropertyAccessBase
{
  const char * const name;
  bool enabled = true;
  ClassId classId[MAX_DATABASES];
  PropertyId id = 0;
  StoreInfo *storeinfo;
  const PropertyType type;
  const char *inverse_name;

  PropertyAccessBase(const char * name, StoreInfo *storage, const PropertyType &type)
      : name(name), storeinfo(storage), type(type), inverse_name(nullptr) {}

  PropertyAccessBase(const char * name, const char * inverse, const PropertyType &type)
      : name(name), storeinfo(new NullStorage()), type(type), inverse_name(inverse) {}

  virtual ~PropertyAccessBase() {delete storeinfo;}

  virtual void setup(Properties *props) const {}
};

/**
 * templated abstract superclass for property accessors
 */
template <typename O, typename P>
struct PropertyAccess : public PropertyAccessBase {
  PropertyAccess(const char * name, StoreAccessBase<O> *storage, const PropertyType &type)
      : PropertyAccessBase(name, storage, type) {}
  PropertyAccess(const char * name, const char * inverse, const PropertyType &type)
      : PropertyAccessBase(name, inverse, type) {}
  virtual void set(O &o, P val) const = 0;
  virtual P get(O &o) const = 0;
  virtual bool same(O *obj, ObjectId oid) {return false;}
};

/**
 * property accessor that performs direct assignment
 */
template <typename O, typename P, P O::*p> struct PropertyAssign : public PropertyAccess<O, P> {
  PropertyAssign(const char * name, StoreAccessBase<O> *storage, const PropertyType &type)
      : PropertyAccess<O, P>(name, storage, type) {}
  PropertyAssign(const char * name, const char * inverse, const PropertyType &type)
      : PropertyAccess<O, P>(name, inverse, type) {}
  void set(O &o, P val) const override { o.*p = val;}
  P get(O &o) const override { return o.*p;}
};

template <typename T> struct ClassTraits;

/**
 * dummy class
 */
struct EmptyClass
{
};

/**
 * iterates over class property mappings. In an inheritance context, the iteration will start with the topmost
 * class and  run down the hierarchy so that all properties are covered. Single-inheritance only
 */
class Properties
{
protected:
  const PropertyAccessBase * keyProperty = nullptr;
  const unsigned numProps;
  const PropertyAccessBase *** const decl_props;
  Properties * superIter = nullptr;
  unsigned startPos = 0;

  Properties(const PropertyAccessBase ** decl_props[], unsigned numProps)
      : decl_props(decl_props), numProps(numProps), fixedSize(0)
  {}

  Properties(const Properties& mit) = delete;
public:
  size_t fixedSize;

  virtual void init() = 0;

  template <typename O>
  PropertyAccess<O, ObjectId> *objectIdAccess()
  {
    if(keyProperty)
      return (PropertyAccess<O, ObjectId> *)keyProperty;
    else
      return superIter ? superIter->objectIdAccess<O>() : nullptr;
  }

  bool preparesUpdates(StoreId storeId, ClassId classId) {
    for(unsigned i=0; i<numProps; i++)
      if((*decl_props[i])->storeinfo->preparesUpdates(storeId, classId))
        return true;
    return false;
  }

  inline unsigned full_size() {
    return superIter ? superIter->full_size() + numProps : numProps;
  }

  const PropertyAccessBase * get(unsigned index) {
    return index >= startPos ? *decl_props[index-startPos] : superIter->get(index);
  }

  void setKeyProperty(const PropertyAccessBase *prop) {
    keyProperty = prop;
  }
};

template <typename S>
class PropertiesImpl : public Properties
{
  PropertiesImpl(const PropertyAccessBase ** decl_props[], unsigned numProps)
      : Properties(decl_props, numProps)
  {
    for(unsigned i=0; i<numProps; i++) {
      const PropertyAccessBase *pa = *decl_props[i];
      pa->setup(this);
    }
  }
public:
  template <typename T>
  static Properties *mk()
  {
    Properties *p = new PropertiesImpl<S>(
        ClassTraits<T>::decl_props,
        ClassTraits<T>::num_decl_props);

    //assign consecutive IDs, starting at 2 (0 and 1 are reserved)
    for(unsigned i=0; i<p->full_size(); i++)
      const_cast<PropertyAccessBase *>(p->get(i))->id = i+2;

    return p;
  }

  void init() override
  {
    //determine superclass and property start position
    superIter = ClassTraits<S>::traits_properties;
    startPos = superIter ? superIter->full_size() : 0;

    //general storage initialization
    for(unsigned i=0; i<numProps; i++) {
      const PropertyAccessBase *pa = *decl_props[i];
      if (pa->enabled && pa->storeinfo) {
        pa->storeinfo->init(pa);
      }
    }

    //see if we're fixed size
    fixedSize = 0;
    if(superIter) {
      fixedSize = superIter->fixedSize;
      if(!fixedSize) return;
    }
    for(unsigned i=0; i<numProps; i++) {
      const PropertyAccessBase *pa = *decl_props[i];
      if(pa->enabled) {
        switch(pa->storeinfo->layout) {
          case StoreLayout::all_embedded: {
            if (!pa->storeinfo->fixedSize) {
              fixedSize = 0;
              return;
            }
            fixedSize += pa->storeinfo->fixedSize;
            break;
          }
          case StoreLayout::embedded_key:
            fixedSize += ObjectKey_sz;
            break;
          case StoreLayout::property:
            break;
        }
      }
    }
  }
};

enum class SchemaCompatibility {none, read, write};

/**
 * mutable class metadata that is maintained per-database
 */
struct ClassData
{
  ClassData(const ClassData &other) = delete;
  ClassData() : classId(0), maxObjectId(0), refcounting(false) {}

  ClassId classId = 0;
  ObjectId maxObjectId = 0;
  bool refcounting = false;
  long cacheOwner = 0;
  long refcountingOwner = 0;

  std::set<ClassId> prepareClasses;
};

/**
 * metadata for mapped classes. Non-templated superclass for ClassInfo
 */
struct AbstractClassInfo
{
  /**
   * minimum id for user-mapped classes. Values below are reserved for stuff like collections
   */
  static const ClassId MIN_USER_CLSID = 10;

  AbstractClassInfo(const AbstractClassInfo &other) = delete;

  SchemaCompatibility compatibility = SchemaCompatibility::write;

  const char *name;
  const std::type_info &typeinfo;

  //mutable metadata
  ClassData data[MAX_DATABASES];

  std::vector<AbstractClassInfo *> subs;

  AbstractClassInfo(const char *name, const std::type_info &typeinfo) : name(name), typeinfo(typeinfo) {}

  void addSub(AbstractClassInfo *rsub) {
    subs.push_back(rsub);
  }

  bool isPoly() {
    return !subs.empty();
  }

  bool hasClassId(StoreId storeId, ClassId cid) {
    if(data[storeId].classId == cid) return true;
    for(auto &sub : subs)
      if(sub->hasClassId(storeId, cid)) return true;
    return false;
  }

  void setRefCounting(StoreId storeId, bool refcount) {
    data[storeId].refcounting = refcount;
    for(auto &sub : subs) {
      sub->setRefCounting(storeId, refcount);
    }
  }

  bool getRefCounting(StoreId storeId) {
    return data[storeId].refcounting;
  }

  bool isInstance(StoreId storeId, ClassId _classId) {
    if(data[storeId].classId == _classId) return true;
    for(auto s : subs) {
      if(s->isInstance(storeId, _classId)) return true;
    }
    return false;
  }

  AbstractClassInfo *resolve(StoreId storeId, ClassId otherClassId)
  {
    if(otherClassId == data[storeId].classId) {
      return this;
    }
    for(auto res : subs) {
      AbstractClassInfo *r = res->resolve(storeId, otherClassId);
      if(r) return r;
    }
    return nullptr;
  }

  AbstractClassInfo *resolve(const std::type_info &ti)
  {
      const char *n1 = ti.name();
      const char *n2 = typeinfo.name();
    if(ti == typeinfo) {
      return this;
    }
    for(auto res : subs) {
      AbstractClassInfo *r = res->resolve(ti);
      if(r) return r;
    }
    return nullptr;
  }

  /**
   * search the inheritance tree rooted in this object for a class info with the given classId
   * @return the classinfo
   * @throw error if not found
   */
  AbstractClassInfo *doresolve(StoreId storeId, ClassId otherClassId)
  {
    AbstractClassInfo *resolved = resolve(storeId, otherClassId);
    if(!resolved) {
      throw error("unknow classId. Class missing from registry");
    }
    return resolved;
  }

  /**
   * search the inheritance tree rooted in this object for a class info with the given typeid
   * @return the classinfo
   * @throw error if not found
   */
  AbstractClassInfo *doresolve(const std::type_info &ti)
  {
    AbstractClassInfo *resolved = resolve(ti);
    if(!resolved) {
      throw error("unknow typeid. Class missing from registry");
    }
    return resolved;
  }

  std::vector<ClassId> allClassIds(StoreId storeId) {
    std::vector<ClassId> ids;
    addClassIds(storeId, ids);
    return ids;
  }
  void addClassIds(StoreId storeId, std::vector<ClassId> &ids) {
    ids.push_back(data[storeId].classId);
    for(auto &sub : subs) sub->addClassIds(storeId, ids);
  }
};

namespace sub {

/**
 * a group of structs that resolve the variadic template list used by the ClassInfo#subclass function
 * the list is expanded and the ClassTraits for each type are notified about the subclass
 */

//this one does the real work by adding S as subtype to T
template<typename T, typename S>
struct resolve_impl
{
  bool publish(AbstractClassInfo *res) {
    ClassTraits<S>::traits_info->addSub(res);
    return true;
  }
};

//primary template
template<typename T, typename... Sargs>
struct resolve;

//helper that removes one type arg from the list
template<typename T, typename S, typename... Sargs>
struct resolve_helper
{
  bool publish(AbstractClassInfo *res) {
    if(resolve_impl<T, S>().publish(res)) return true;
    return resolve<T, Sargs...>().publish(res);
  }
};

//template specialization for non-empty list
template<typename T, typename... Sargs>
struct resolve
{
  bool publish(AbstractClassInfo *res) {
    return resolve_helper<T, Sargs...>().publish(res);
  }
};

//template specialization for empty list
template<typename T>
struct resolve<T>
{
  bool publish(AbstractClassInfo *res) {return false;}
};

template <typename T>
struct Substitute {
  virtual T *getPtr() = 0;
};

template <typename T, typename S>
struct SubstituteImpl : public Substitute<T> {
  T *getPtr() override {return new S();}
};

} //sub

/**
 * peer object for the ClassTraitsBase below which contains class metadata
 */
template <typename T, typename ... Sup>
struct ClassInfo : public AbstractClassInfo
{
  T *(* const getSubstitute)();
  size_t (* const size)(StoreId storeId, T *obj);
  bool (* const initMember)(StoreId storeId, WriteTransaction *tr, T &obj, const PropertyAccessBase *pa, unsigned flags);
  T * (* const makeObject)(StoreId storeId, ClassId classId);
  bool (* const needs_prepare)(StoreId storeId, ClassId classId, bool &result, unsigned flags);
  Properties * (* const getProperties)(StoreId storeId, ClassId classId);
  bool (* const addSize)(StoreId storeId, T *obj, const PropertyAccessBase *pa, size_t &size, unsigned flags);
  bool (* const get_objectkey)(const std::shared_ptr<T> &obj, ObjectKey *&key, unsigned flags);
  bool (* const prep_delete)(StoreId storeId, WriteTransaction *tr, ObjectBuf &buf, const PropertyAccessBase *pa, unsigned flags);
  bool (* const prep_update)(StoreId storeId, ObjectBuf &buf, PrepareData &pd, T *obj, const PropertyAccessBase *pa, unsigned flags);
  bool (* const save)(StoreId storeId, WriteTransaction *wtr,
                      ClassId classId, ObjectId objectId, T *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode, unsigned flags);
  bool (* const load)(StoreId storeId, Transaction *tr, ReadBuf &buf,
                      ClassId classId, ObjectId objectId, T *obj, const PropertyAccessBase *pa, StoreMode mode, unsigned flags);

  sub::Substitute<T> *substitute = nullptr;

  ClassInfo(const char *name, const std::type_info &typeinfo)
      : AbstractClassInfo(name, typeinfo),
        getSubstitute(&ClassTraits<T>::getSubstitute),
        size(&ClassTraits<T>::size),
        initMember(&ClassTraits<T>::initMember),
        makeObject(&ClassTraits<T>::makeObject),
        getProperties(&ClassTraits<T>::getProperties),
        needs_prepare(&ClassTraits<T>::needs_prepare),
        addSize(&ClassTraits<T>::addSize),
        get_objectkey(&ClassTraits<T>::get_objectkey),
        prep_delete(&ClassTraits<T>::prep_delete),
        prep_update(&ClassTraits<T>::prep_update),
        save(&ClassTraits<T>::save),
        load(&ClassTraits<T>::load) {}

  ~ClassInfo() {if(substitute) delete substitute;}

  template <typename S>
  void setSubstitute() {
    substitute = new sub::SubstituteImpl<T, S>();
  }

  template <typename ... Sup2>
  static ClassInfo<T, Sup...> *subclass(const char *name, const std::type_info &typeinfo)
  {
    //create a classinfo
    return new ClassInfo<T, Sup2...>(name, typeinfo);
  }

  void publish() {
    //make it known to superclasses
    sub::resolve<T, Sup...>().publish(this);
  }
};

#define FIND_CLS(__Tpl, __sid, __cid) static_cast<ClassInfo<__Tpl> *>(ClassTraits<__Tpl>::traits_info->resolve(__sid, __cid))
#define RESOLVE_SUB(__cid) static_cast<ClassInfo<T> *>(ClassTraits<T>::traits_info->doresolve(storeId, __cid))
#define RESOLVE_SUB_TI(__ti) static_cast<ClassInfo<T> *>(ClassTraits<T>::traits_info->doresolve(__ti))

/**
 * base class for class/inheritance resolution infrastructure. Every mapped class is represented by a templated
 * subclass of this class. All calls to access/update mapped object properties should go through here and will be
 * dispatched to the correct location. The correct location is determined by the classId which is uniquely assigned
 * to each mapped class. Many calls here will first determine the correct ClassTraits instance, and from there
 * hand over to specific processing. This ensures that the cast operation at handover from the non-templated PropertyAccessBase
 * to the templated StoreAccessBase happens on the exact type level, i.e., at handover, the ClassTraits template parameter
 * type T is always the exact type of the handed-over object. Thus the cast is without issues.
 *
 * Heres an illustration:
 *
 * - mapped type hierarchy: S <- T (T subclasses S)
 * - persistent operation is executed with template parameter type S and an operand of type T
 * - the operation needs to hand over to a property mapping resp. StoreAccessBase<T>. For this purpose, the
 *   StoreInfo inside the property mapping must be cast down.
 * - we therefore perform lookup by classId to determine the exact match, which is ClassTraits<T>. Here the cast
 *   StoreInfo* -> StoreAccessBase<T>* is harmless
 */
template <typename T, typename SUP=EmptyClass>
class ClassTraitsBase
{
  template <typename, typename ...> friend struct ClassInfo;

  static const unsigned FLAG_UP = 0x1;
  static const unsigned FLAG_DN = 0x2;
  static const unsigned FLAG_HR = 0x4;
  static const unsigned FLAGS_ALL = FLAG_UP | FLAG_DN | FLAG_HR;

#define DN flags & FLAG_DN
#define UP flags & FLAG_UP

  /**
   * determine the buffer size for the given object. Non-polymorpic
   */
  static size_t size(StoreId storeId, T *obj)
  {
    if(traits_properties->fixedSize) return traits_properties->fixedSize;

    size_t size = 0;
    for(unsigned i=0, sz=traits_properties->full_size(); i<sz; i++) {
      auto pa = traits_properties->get(i);

      if(!pa->enabled) continue;

      addSize(storeId, obj, pa, size);
    }
    return size;
  }

  /**
   * helper function which does the cast to the specific store accessor
   * @param pa a property mapping
   * @return pa->storeinfo cast to the correct StorageAccessBase type
   */
  static inline const StoreAccessBase<T>* storeaccess(const PropertyAccessBase *pa) {
    return static_cast<const StoreAccessBase<T> *>(pa->storeinfo);
  }

public:
  static bool traits_initialized;
  static const char *traits_classname;
  static ClassInfo<T, SUP> *traits_info;
  static Properties * traits_properties;
  static const PropertyAccessBase ** decl_props[];
  static const unsigned num_decl_props;

  static inline ClassData &traits_data(StoreId storeId) {
    return traits_info->data[storeId];
  }

  /**
   * perform lazy initialization of static structures (only once).
   */
  static void init() {
    if(!traits_initialized) {
      traits_initialized = true;

      traits_properties->init();
      traits_info->publish();
    }
  }

  /**
   * @return the objectid accessor for this class
   */
  static PropertyAccess<T, ObjectId> *objectIdAccess() {
    return traits_properties->objectIdAccess<T>();
  }

  static size_t bufferSize(StoreId storeId, T *obj, ClassId *clsId=nullptr)
  {
    const std::type_info &ti = typeid(*obj);
    if(ti == traits_info->typeinfo) {
      if(clsId) *clsId = traits_data(storeId).classId;
      return size(storeId, obj);
    }
    else {
      ClassInfo<T> *sub = RESOLVE_SUB_TI(ti);
      if(clsId) *clsId = sub->data[storeId].classId;
      return sub->size(storeId, obj);
    }
  }

  static bool needsPrepare(StoreId storeId, ClassId classId) {
    bool result;
    if(!needs_prepare(storeId, classId, result)) throw invalid_classid_error(classId);
    return result;
  }
  static bool needs_prepare(StoreId storeId, ClassId classId, bool &result, unsigned flags=FLAGS_ALL)
  {
    if(classId == traits_data(storeId).classId) {
      result = !traits_data(storeId).prepareClasses.empty();
      return true;
    }
    else if(classId) {
      if(UP && ClassTraits<SUP>::needs_prepare(storeId, classId, result, FLAG_UP))
        return true;
      return DN && RESOLVE_SUB(classId)->needs_prepare(storeId, classId, result, FLAG_DN);
    }
    return false;
  }

  static T *getSubstitute()
  {
    if(traits_info->substitute) return traits_info->substitute->getPtr();

    for(auto &sub : traits_info->subs) {
      ClassInfo<T> * si = static_cast<ClassInfo<T> *>(sub);
      T *subst = si->getSubstitute();
      if(subst != nullptr) return subst;
    }
    return nullptr;
  }

  static const PropertyAccessBase *getInverseAccess(const PropertyAccessBase *pa)
  {
    for(unsigned i=0; i<num_decl_props; i++) {
      const char *inverse = (*decl_props[i])->inverse_name;
      if (inverse && !strcmp(inverse, pa->name)) {
        return *decl_props[i];
      }
    }
    return nullptr;
  }

  static Properties * getProperties(StoreId storeId, ClassId classId)
  {
    if(classId == traits_data(storeId).classId)
      return traits_properties;
    else if(classId)
      return RESOLVE_SUB(classId)->getProperties(storeId, classId);
    return nullptr;
  }

  static bool addSize(StoreId storeId, T *obj, const PropertyAccessBase *pa, size_t &size, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId[storeId] == traits_data(storeId).classId) {
      size += storeaccess(pa)->size(storeId, obj, pa);
      return true;
    }
    else if(pa->classId[storeId]) {
      if(UP && ClassTraits<SUP>::addSize(storeId, obj, pa, size, FLAG_UP))
        return true;
      return DN && RESOLVE_SUB(pa->classId[storeId])->addSize(storeId, obj, pa, size, FLAG_DN);
    }
    return false;
  }

  static ObjectKey *getObjectKey(const std::shared_ptr<T> &obj, bool force=true)
  {
    ObjectKey *key = nullptr;
    if(!get_objectkey(obj, key) && force) throw invalid_pointer_error();
    return key;
  }
  static bool get_objectkey(const std::shared_ptr<T> &obj, ObjectKey *&key, unsigned flags=FLAGS_ALL)
  {
    object_handler<T> *handler = std::get_deleter<object_handler<T>>(obj);
    if(handler) {
      key = handler;
      return true;
    }
    else {
      if(UP && ClassTraits<SUP>::get_objectkey(obj, key, FLAG_UP)) return true;
      if(DN) {
        for(auto &sub : traits_info->subs) {
          ClassInfo<T> * si = static_cast<ClassInfo<T> *>(sub);
          if(si->get_objectkey(obj, key, FLAG_DN)) return true;
        }
      }
    }
    return false;
  }

  static bool initMember(StoreId storeId, WriteTransaction *tr, T &obj, const PropertyAccessBase *pa, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId[storeId] == traits_data(storeId).classId) {
      storeaccess(pa)->initMember(tr, &obj, pa);
      return true;
    }
    else if(pa->classId[storeId]) {
      if(UP && ClassTraits<SUP>::initMember(storeId, tr, obj, pa, FLAG_UP))
        return true;
      return DN && RESOLVE_SUB(pa->classId[storeId])->initMember(storeId, tr, obj, pa, FLAG_DN);
    }
    return false;
  }

  static void prepareUpdate(StoreId storeId, ObjectBuf &buf, PrepareData &pd, T *obj, const PropertyAccessBase *pa)
  {
    if(!prep_update(storeId, buf, pd, obj, pa)) throw invalid_classid_error(pa->classId[storeId]);
  }
  static bool prep_update(StoreId storeId, ObjectBuf &buf, PrepareData &pd, T *obj, const PropertyAccessBase *pa, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId[storeId] == traits_data(storeId).classId) {
      storeaccess(pa)->prepareUpdate(storeId, buf, pd, obj, pa);
      return true;
    }
    else if(pa->classId[storeId]) {
      if(UP && ClassTraits<SUP>::prep_update(storeId, buf, pd, obj, pa, FLAG_UP))
        return true;
      return DN && RESOLVE_SUB(pa->classId[storeId])->prep_update(storeId, buf, pd, obj, pa, FLAG_DN);
    }
    return false;
  }

  static void prepareDelete(StoreId storeId, WriteTransaction *tr, ObjectBuf &buf, const PropertyAccessBase *pa)
  {
    if(!prep_delete(storeId, tr, buf, pa)) throw invalid_classid_error(pa->classId[storeId]);
  }
  static bool prep_delete(StoreId storeId, WriteTransaction *tr, ObjectBuf &buf, const PropertyAccessBase *pa, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId[storeId] == traits_data(storeId).classId) {
      storeaccess(pa)->prepareDelete(storeId, tr, buf, pa);
      return true;
    }
    else if(pa->classId[storeId]) {
      if(UP && ClassTraits<SUP>::prep_delete(storeId, tr, buf, pa, FLAG_UP))
        return true;
      return DN && RESOLVE_SUB(pa->classId[storeId])->prep_delete(storeId, tr, buf, pa, FLAG_DN);
    }
    return false;
  }

  static bool save(StoreId storeId, WriteTransaction *tr,
                   ClassId classId, ObjectId objectId, T *obj, PrepareData &pd, const PropertyAccessBase *pa, StoreMode mode, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId[storeId] == traits_data(storeId).classId) {
      storeaccess(pa)->save(tr, classId, objectId, obj, pd, pa, mode);
      return true;
    }
    else if(pa->classId[storeId]) {
      if(UP && ClassTraits<SUP>::save(storeId, tr, classId, objectId, obj, pd, pa, mode, FLAG_UP))
        return true;
      return DN && RESOLVE_SUB(pa->classId[storeId])->save(storeId, tr, classId, objectId, obj, pd, pa, mode, FLAG_DN);
    }
    return false;
  }

  static bool load(StoreId storeId, Transaction *tr,
                   ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj, const PropertyAccessBase *pa,
                   StoreMode mode=StoreMode::force_none, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId[storeId] == traits_data(storeId).classId) {
      storeaccess(pa)->load(tr, buf, classId, objectId, obj, pa, mode);
      return true;
    }
    else if(pa->classId[storeId]) {
      if(UP && ClassTraits<SUP>::load(storeId, tr, buf, classId, objectId, obj, pa, mode, FLAG_UP))
        return true;
      return DN && RESOLVE_SUB(pa->classId[storeId])->load(storeId, tr, buf, classId, objectId, obj, pa, mode, FLAG_DN);
    }
    return false;
  }

  template <typename TV>
  static void put(StoreId storeId, T &d, const PropertyAccessBase *pa, TV &value, unsigned flags=FLAGS_ALL) {
    if(pa->classId[storeId] != traits_data(storeId).classId)
      throw error("internal error: type mismatch");

    const PropertyAccess <T, TV> *acc = (const PropertyAccess <T, TV> *) pa;
    value = acc->get(d);
  }

  /**
   * update the given property using value. Must only be called after type resolution, such
   * that pa->classId[storeId] == info->classId
   */
  template <typename TV>
  static void get(StoreId storeId, T &d, const PropertyAccessBase *pa, TV &value, unsigned flags=FLAGS_ALL) {
    if(pa->classId[storeId] != traits_data(storeId).classId)
      throw error("internal error: type mismatch");

    const PropertyAccess<T, TV> *acc = (const PropertyAccess<T, TV> *)pa;
    acc->set(d, value);
  }
};

/**
 * ClassTraits extension for abstract (non-instantiable) classes
 */
template <typename T> struct ClassTraitsAbstract
{
  static const bool isAbstract = true;

  static T *makeObject(StoreId storeId, ClassId classId)
  {
    if(classId == ClassTraits<T>::traits_data(storeId).classId) {
      throw error("abstract class cannot be instantiated");
    }
    else if(classId)
      return RESOLVE_SUB(classId)->makeObject(storeId, classId);
    return nullptr;
  }
};

/**
 * ClassTraits extension for concrete (instantiable) classes
 */
template <typename T> struct ClassTraitsConcrete
{
  static const bool isAbstract = false;

  static T *makeObject(StoreId storeId, ClassId classId)
  {
    if(classId == ClassTraits<T>::traits_data(storeId).classId) {
      return new T();
    }
    else if(classId)
      return RESOLVE_SUB(classId)->makeObject(storeId, classId);
    return nullptr;
  }
};

/**
 * ClassTraits extension for concrete (instantiable) classes with replacement
 */
template <typename T, typename R> struct ClassTraitsConcreteRepl
{
  static const bool isAbstract = false;

  static T *makeObject(StoreId storeId, ClassId classId)
  {
    if(classId == ClassTraits<T>::traits_data(storeId).classId) {
      return new R();
    }
    else if(classId)
      return RESOLVE_SUB(classId)->makeObject(storeId, classId);
    return nullptr;
  }
};

/**
 * represents a non-class, e.g. where a mapped superclass must be defined but does not exist
 */
template <>
struct ClassTraits<EmptyClass>
{
  static const bool isAbstract = true;
  static ClassInfo<EmptyClass> *traits_info;
  static Properties * traits_properties;
  static const unsigned num_decl_props = 0;
  static const PropertyAccessBase ** decl_props[0];

  static EmptyClass *makeObject(StoreId storeId, ClassId classId) {return nullptr;}
  static Properties * getProperties(StoreId storeId, ClassId classId) {return nullptr;}

  static inline ClassData &traits_data(StoreId storeId) {
    return traits_info->data[storeId];
  }

  static void init() {}

  template <typename T>
  static T *getSubstitute() {
    return nullptr;
  }
  template <typename T>
  static size_t bufferSize(StoreId storeId, T *obj, ClassId *cid=nullptr) {
    return 0;
  }
  static bool needsPrepare(StoreId storeId, ClassId classId) {
    return false;
  }
  static bool needs_prepare(StoreId storeId, ClassId classId, bool &result, unsigned flags=0) {
    return false;
  }
  template <typename T>
  static size_t size(StoreId storeId, T *obj) {
    return 0;
  }
  template <typename T>
  static PropertyAccess<T, ObjectId> *objectIdAccess() {
    return nullptr;
  }
  static const PropertyAccessBase *getInverseAccess(const PropertyAccessBase *pa) {
    return nullptr;
  }
  template <typename T>
  static bool addSize(StoreId storeId, T *obj, const PropertyAccessBase *pa, size_t &size, unsigned flags=0) {
    return false;
  }
  template <typename T>
  static ObjectKey *getObjectKey(const std::shared_ptr<T> &obj, bool force=true) {
    return nullptr;
  }
  template <typename T>
  static bool initMember(StoreId storeId, WriteTransaction *tr, T &obj, const PropertyAccessBase *pa, unsigned flags=0) {
    return false;
  }
  template <typename T>
  static bool get_objectkey(const std::shared_ptr<T> &obj, ObjectKey *&key, unsigned flags) {
    return false;
  }
  static void prepareDelete(StoreId storeId, WriteTransaction *tr, ObjectBuf &buf, const PropertyAccessBase *pa) {
  }
  static bool prep_delete(StoreId storeId, WriteTransaction *tr, ObjectBuf &buf, const PropertyAccessBase *pa, unsigned flags=0) {
    return false;
  }
  template <typename T>
  static void prepareUpdate(StoreId storeId, ObjectBuf &buf, PrepareData &pd, T *obj, const PropertyAccessBase *pa) {
  }
  template <typename T>
  static bool prep_update(StoreId storeId, ObjectBuf &buf, PrepareData &pd, T *obj, const PropertyAccessBase *pa, unsigned flags=0) {
    return false;
  }
  template <typename T>
  static bool save(StoreId storeId, WriteTransaction *wtr, ClassId classId, ObjectId objectId, T *obj, PrepareData &pd,
                   const PropertyAccessBase *pa, StoreMode mode=StoreMode::force_none, unsigned flags=0) {
    return false;
  }
  template <typename T>
  static bool load(StoreId storeId, Transaction *tr, ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj,
                   const PropertyAccessBase *pa, StoreMode mode=StoreMode::force_none, unsigned flags=0) {
    return false;
  }
  template <typename T, typename TV>
  static void put(StoreId storeId, T &d, const PropertyAccessBase *pa, TV &value, unsigned flags=0) {
  }
  template <typename T, typename TV>
  static void get(StoreId storeId, T &d, const PropertyAccessBase *pa, TV &value, unsigned flags=0) {
  }
};

template<typename T>
static PropertyType object_vector_t() {
  using Traits = ClassTraits<T>;
  return PropertyType(Traits::traits_classname, true);
}

template<typename T>
static PropertyType object_t() {
  using Traits = ClassTraits<T>;
  return PropertyType(Traits::traits_classname);
}

} //kv
} //persistence
} //flexis

using NO_SUPERCLASS = flexis::persistence::kv::EmptyClass;

#endif //FLEXIS_FLEXIS_KVTRAITS_H

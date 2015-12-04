//
// Created by cse on 10/9/15.
//

#ifndef FLEXIS_FLEXIS_KVTRAITS_H
#define FLEXIS_FLEXIS_KVTRAITS_H

#include <string>
#include <vector>
#include <set>
#include <cstring>
#include <typeinfo>
#include <stdexcept>
#include <stdint.h>
#include <typeinfo>
#include "kvbuf.h"
#include "FlexisPersistence_Export.h"

namespace flexis {
namespace persistence {
namespace kv {

#define ARRAY_SZ(x) unsigned(sizeof(x) / sizeof(decltype(*x)))

struct PropertyType
{
  //predefined base type id, irrelevant if className is set
  const unsigned id;

  //it's a vector
  const bool isVector;

  //number of bytes, 0 if variable size (e.g. string). For a vector, this is the byteSize of the elements
  const unsigned byteSize;

  //name of the mapped type if this is an object type
  const char *className;

  PropertyType(unsigned id, unsigned byteSize, bool isVector=false)
      : id(id), isVector(isVector), byteSize(byteSize), className(nullptr) {}
  PropertyType(const char *clsName, bool isVector=false) :
      id(0), isVector(isVector), byteSize(StorageKey::byteSize), className(clsName) {}

  bool operator == (const PropertyType &other) const {
    return id == other.id
           && isVector == other.isVector
           && className == other.className;
  }
};

template <typename T> struct TypeTraits;
#define TYPETRAITS template <> struct TypeTraits
#define TYPETRAITSV template <> struct TypeTraits<std::vector
#define TYPETRAITSS template <> struct TypeTraits<std::set

#define TYPEDEF(_id, _sz) static const unsigned id=_id; static const unsigned byteSize=_sz; static const bool isVect=false;
#define TYPEDEFV(_id, _sz) static const unsigned id=_id; static const unsigned byteSize=_sz; static const bool isVect=true;

TYPETRAITS<short>             {TYPEDEF(1, 2);};
TYPETRAITS<unsigned short>    {TYPEDEF(2, 2);};
TYPETRAITS<int>               {TYPEDEF(3, 4);};
TYPETRAITS<unsigned int>      {TYPEDEF(4, 4);};
TYPETRAITS<long>              {TYPEDEF(5, 8);};
TYPETRAITS<unsigned long>     {TYPEDEF(6, 8);};
TYPETRAITS<long long>         {TYPEDEF(7, 8);};
TYPETRAITS<unsigned long long>{TYPEDEF(8, 8);};
TYPETRAITS<bool>              {TYPEDEF(9, 1);};
TYPETRAITS<float>             {TYPEDEF(10, 4);};
TYPETRAITS<double>            {TYPEDEF(11, 8);};
TYPETRAITS<const char *>      {TYPEDEF(12, 0);};
TYPETRAITS<std::string>       {TYPEDEF(13, 0);};

TYPETRAITSV<short>>             {TYPEDEFV(1, 2);};
TYPETRAITSV<unsigned short>>    {TYPEDEFV(2, 2);};
TYPETRAITSV<int>>               {TYPEDEFV(3, 4);};
TYPETRAITSV<unsigned int>>      {TYPEDEFV(4, 4);};
TYPETRAITSV<long>>              {TYPEDEFV(5, 8);};
TYPETRAITSV<unsigned long>>     {TYPEDEFV(6, 8);};
TYPETRAITSV<long long>>         {TYPEDEFV(7, 8);};
TYPETRAITSV<unsigned long long>>{TYPEDEFV(8, 8);};
TYPETRAITSV<bool>>              {TYPEDEFV(9, 1);};
TYPETRAITSV<float>>             {TYPEDEFV(10, 4);};
TYPETRAITSV<double>>            {TYPEDEFV(11, 8);};
TYPETRAITSV<const char *>>      {TYPEDEFV(12, 0);};
TYPETRAITSV<std::string>>       {TYPEDEFV(13, 0);};

TYPETRAITSS<short>>             {TYPEDEFV(1, 2);};
TYPETRAITSS<unsigned short>>    {TYPEDEFV(2, 2);};
TYPETRAITSS<int>>               {TYPEDEFV(3, 4);};
TYPETRAITSS<unsigned int>>      {TYPEDEFV(4, 4);};
TYPETRAITSS<long>>              {TYPEDEFV(5, 8);};
TYPETRAITSS<unsigned long>>     {TYPEDEFV(6, 8);};
TYPETRAITSS<long long>>         {TYPEDEFV(7, 8);};
TYPETRAITSS<unsigned long long>>{TYPEDEFV(8, 8);};
TYPETRAITSS<bool>>              {TYPEDEFV(9, 1);};
TYPETRAITSS<float>>             {TYPEDEFV(10, 4);};
TYPETRAITSS<double>>            {TYPEDEFV(11, 8);};
TYPETRAITSS<const char *>>      {TYPEDEFV(12, 0);};
TYPETRAITSS<std::string>>       {TYPEDEFV(13, 0);};

//these assertions must hold because certain elmements are written/read natively
static_assert(sizeof(ClassId) == TypeTraits<ClassId>::byteSize, "ClassId: byteSize must match native size");
static_assert(sizeof(ObjectId) == TypeTraits<ObjectId>::byteSize, "ObjectId: byteSize must match native size");
static_assert(sizeof(PropertyId) == TypeTraits<PropertyId>::byteSize, "PropertyId: byteSize must match native size");
static_assert(sizeof(size_t) == TypeTraits<size_t>::byteSize, "size_t: byteSize must match native size");

class ReadTransaction;
class WriteTransaction;
class PropertyAccessBase;

enum class StoreMode {force_none, force_all, force_buffer, force_property};

enum class StoreLayout {all_embedded, embedded_key, property};

/**
 * abstract superclass for all Store Access classes
 */
struct StoreAccessBase
{
  const StoreLayout layout;

  StoreAccessBase(StoreLayout layout=StoreLayout::all_embedded) : layout(layout) {}

  virtual size_t size(const byte_t *buf) const = 0;
  virtual size_t size(void *obj, const PropertyAccessBase *pa) {return 0;}

  virtual void save(WriteTransaction *tr,
                    ClassId classId, ObjectId objectId,
                    void *obj, const PropertyAccessBase *pa,
                    StoreMode mode=StoreMode::force_none) = 0;

  virtual void load(ReadTransaction *tr,
                    ReadBuf &buf,
                    ClassId classId, ObjectId objectId,
                    void *obj, const PropertyAccessBase *pa,
                    StoreMode mode=StoreMode::force_none) = 0;
};

/**
 * abstract superclass for all Store Access classes that represent mapped-object valued properties where
 * the referred-to object is saved individually and key value saved in the enclosing object's buffer
 */
struct StoreAccessEmbeddedKey : public StoreAccessBase
{
  StoreAccessEmbeddedKey() : StoreAccessBase(StoreLayout::embedded_key) {}

  size_t size(const byte_t *buf) const override {return StorageKey::byteSize;}
  size_t size(void *obj, const PropertyAccessBase *pa) override {return StorageKey::byteSize;}
};

/**
 * abstract superclass for all Store Access classes that represent mapped-object valued properties that are saved
 * under a property key, with nothing saved in the enclosing object's buffer
 */
struct StoreAccessPropertyKey: public StoreAccessBase
{
  StoreAccessPropertyKey() : StoreAccessBase(StoreLayout::property) {}

  size_t size(const byte_t *buf) const override {return 0;}
  size_t size(void *obj, const PropertyAccessBase *pa) override {return 0;}
};

template<typename T, typename V> struct PropertyStorage : public StoreAccessBase {};

struct ValueTraitsBase {
  const bool fixed;
  ValueTraitsBase(bool fixed) : fixed(fixed) {}
  virtual size_t data_size(const byte_t *) = 0;
};

template <bool Fixed>
struct ValueTraitsFixed : public ValueTraitsBase
{
  ValueTraitsFixed() : ValueTraitsBase(Fixed) {}
};

template <typename T>
struct ValueTraitsByte : public ValueTraitsFixed<true>
{
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

template <typename T>
struct ValueTraits : public ValueTraitsFixed<true>
{
  size_t data_size(const byte_t *) override {
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
  static void putBytes(WriteBuf &buf, T val) {
    size_t byteSize = TypeTraits<T>::byteSize;
    byte_t *data = buf.allocate(byteSize);
    write_integer(data, val, byteSize);
  }
};

template <>
struct ValueTraits<bool> : public ValueTraitsFixed<true>
{
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

template <>
struct ValueTraits<std::string> : public ValueTraitsFixed<false>
{
  size_t data_size(const byte_t *data) override {
    return strlen((const char *)data) + 1;
  }
  static size_t size(const std::string &val) {
    return val.length() + 1;
  }
  static void getBytes(ReadBuf &buf, std::string &val) {
    val = (const char *)buf.read(0);
    buf.read(val.length() +1); //move the pointer
  }
  static void putBytes(WriteBuf &buf, std::string val) {
    buf.append(val.c_str(), val.length()+1);
  }
};

template <>
struct ValueTraits<const char *> : public ValueTraitsFixed<false>
{
  size_t data_size(const byte_t *data) override {
    return strlen((const char *)data) + 1;
  }
  static size_t size(const char * const &val) {
    return strlen(val) + 1;
  }
  static void getBytes(ReadBuf &buf, const char *&val) {
    val = buf.readCString();
  }
  static void putBytes(WriteBuf &buf, const char *&val) {
    buf.appendCString(val);
  }
};

template <typename T>
struct ValueTraitsFloat : public ValueTraitsFixed<true>
{
  size_t data_size(const byte_t *data) override {
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

template <>
struct ValueTraits<float> : public ValueTraitsFloat<float> {};
template <>
struct ValueTraits<double> : public ValueTraitsFloat<double> {};

struct PropertyAccessBase
{
  const char * const name;
  bool enabled = true;
  ClassId classId;
  unsigned id = 0;
  StoreAccessBase *storage;
  const PropertyType type;
  PropertyAccessBase(const char * name, StoreAccessBase *storage, const PropertyType &type)
      : name(name), storage(storage), type(type) {}
  virtual bool same(void *obj, ObjectId oid) {return false;}
  virtual ~PropertyAccessBase() {delete storage;}
};

template <typename O, typename P>
struct PropertyAccess : public PropertyAccessBase {
  PropertyAccess(const char * name, StoreAccessBase *storage, const PropertyType &type)
      : PropertyAccessBase(name, storage, type) {}
  virtual void set(O &o, P val) const = 0;
  virtual P get(O &o) const = 0;
};

template <typename O, typename P, P O::*p> struct PropertyAssign : public PropertyAccess<O, P> {
  PropertyAssign(const char * name, StoreAccessBase *storage, const PropertyType &type)
      : PropertyAccess<O, P>(name, storage, type) {}
  void set(O &o, P val) const override { o.*p = val;}
  P get(O &o) const override { return o.*p;}
};

template <typename O, typename P, P O::*p>
struct BasePropertyAssign : public PropertyAssign<O, P, p> {
  BasePropertyAssign(const char * name)
      : PropertyAssign<O, P, p>(name, new PropertyStorage<O, P>(), PROPERTY_TYPE(P)) {}
};

template <typename T> struct ClassTraits;

struct EmptyClass
{
};

class Properties
{
  const unsigned keyStorageId;
  const unsigned numProps;
  PropertyAccessBase ** const properties;
  Properties * const superIter;
  const unsigned startPos;

  Properties(PropertyAccessBase * properties[], unsigned numProps, Properties *superIter, unsigned keyStorageId)
      : properties(properties),
        numProps(numProps),
        superIter(superIter),
        startPos(superIter ? superIter->full_size() : 0),
        keyStorageId(keyStorageId)
  {}

  Properties(const Properties& mit) = delete;
public:
  template <typename T, typename S=EmptyClass>
  static Properties *mk()
  {
    Properties *p = new Properties(
        ClassTraits<T>::decl_props,
        ARRAY_SZ(ClassTraits<T>::decl_props),
        ClassTraits<S>::properties,
        ClassTraits<T>::keyPropertyId);

    for(unsigned i=0; i<p->full_size(); i++)
      p->get(i)->id = i+1;

    return p;
  }

  template <typename O>
  PropertyAccess<O, ObjectId> *objectIdAccess()
  {
    if(keyStorageId)
      return (PropertyAccess<O, ObjectId> *)properties[keyStorageId-1];
    else
      return superIter ? superIter->objectIdAccess<O>() : nullptr;
  }

  inline unsigned full_size() {
    return superIter ? superIter->full_size() + numProps : numProps;
  }

  PropertyAccessBase * get(unsigned index) {
    return index >= startPos ? properties[index-startPos] : superIter->get(index);
  }
};

struct ClassInfo {
  static const ClassId MIN_USER_CLSID = 10; //ids below are reserved

  ClassInfo(const ClassInfo &other) = delete;

  const char *name;
  const std::type_info &typeinfo;
  ClassId classId = 0;
  ObjectId maxObjectId = 0;

  ClassInfo(const char *name, const std::type_info &typeinfo, ClassId classId=MIN_USER_CLSID)
      : name(name), typeinfo(typeinfo), classId(classId) {}
};

namespace sub {
//this namespace contains a group of templates that resolve the variadic template list
//on ClassTraits which contains the subclasses. Each subclass is checked against the classId
//stored in a propertyaccessor. If the class matches, the target object is dynamic_cast to the
//target type and then accessed

//this one does the real work
template<typename T, typename S>
struct resolve_impl
{
  static ObjectId get_id(const std::shared_ptr<T> &obj) {
    auto s = std::dynamic_pointer_cast<S>(obj);
    return s ? ClassTraits<S>::get_id(s, 0x2) : 0;
  }
  static bool add(T *obj, PropertyAccessBase *pa, size_t &size) {
    S *s = dynamic_cast<S *>(obj);
    return s ? ClassTraits<S>::add(s, pa, size, 0x2) : false;
  }
  static bool save(WriteTransaction *wtr, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode) {
    S *s = dynamic_cast<S *>(obj);
    return s ? ClassTraits<S>::save(wtr, classId, objectId, s, pa, mode, 0x2) : false;
  }
  static bool load(ReadTransaction *tr, ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa) {
    S *s = dynamic_cast<S *>(obj);
    return s ? ClassTraits<S>::load(tr, buf, classId, objectId, s, pa, 0x2) : false;
  }
};

//primary template
template<typename T, typename... Sargs>
struct resolve;

//helper that removes one type arg from the list
template<typename T, typename S, typename... Sargs>
struct resolve_helper
{
  static ObjectId  get_id(const std::shared_ptr<T> &obj) {
    ObjectId objId = resolve_impl<T, S>().get_id(obj);
    return objId ? objId : resolve<T, Sargs...>().get_id(obj);
  }
  static bool add(T *obj, PropertyAccessBase *pa, size_t &size) {
    if(resolve_impl<T, S>().add(obj, pa, size)) return true;
    return resolve<T, Sargs...>().add(obj, pa, size);
  }
  static bool save(WriteTransaction *wtr, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode) {
    if(resolve_impl<T, S>().save(wtr, classId, objectId, obj, pa, mode)) return true;
    return resolve<T, Sargs...>().save(wtr, classId, objectId, obj, pa, mode);
  }
  static bool load(ReadTransaction *tr, ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa) {
    if(resolve_impl<T, S>().load(tr, buf, classId, objectId, obj, pa)) return true;
    return resolve<T, Sargs...>().load(tr, buf, classId, objectId, obj, pa);
  }
};

//template specialization for non-empty list
template<typename T, typename... Sargs>
struct resolve
{
  static ObjectId get_id(const std::shared_ptr<T> &obj) {
    return resolve_helper<T, Sargs...>().get_id(obj);
  }
  static bool add(T *obj, PropertyAccessBase *pa, size_t &size) {
    return resolve_helper<T, Sargs...>().add(obj, pa, size);
  }
  static bool save(WriteTransaction *wtr, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode) {
    return resolve_helper<T, Sargs...>().save(wtr, classId, objectId, obj, pa, mode);
  }
  static bool load(ReadTransaction *tr, ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa) {
    return resolve_helper<T, Sargs...>().load(tr, buf, classId, objectId, obj, pa);
  }
};

//template specialization for empty list
template<typename T>
struct resolve<T>
{
  static bool get_id(const std::shared_ptr<T> &obj) {
    return 0;
  }
  static bool add(T *obj, PropertyAccessBase *pa, size_t &size) {
    return false;
  }
  static bool save(WriteTransaction *wtr, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode) {
    return false;
  }
  static bool load(ReadTransaction *tr, ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa) {
    return false;
  }
};

} //sub

template <typename T, typename SUP=EmptyClass, typename ... SUBS>
struct ClassTraitsBase
{
  static const unsigned FLAG_UP = 0x1;
  static const unsigned FLAG_DN = 0x2;
  static const unsigned FLAG_HR = 0x4;
  static const unsigned FLAGS_ALL = FLAG_UP | FLAG_DN | FLAG_HR;

#define DN flags & FLAG_DN
#define UP flags & FLAG_UP

  static const unsigned keyPropertyId = 0;

  static ClassInfo info;
  static Properties * properties;
  static PropertyAccessBase * decl_props[];
  static const unsigned decl_props_sz;

  /**
   * @return the objectid accessor for this class
   */
  static PropertyAccess<T, ObjectId> *objectIdAccess() {
    return properties->objectIdAccess<T>();
  }

  template <typename V>
  static bool same(T &t, ObjectId id, std::shared_ptr<V> &val)
  {
    ObjectId oid = get_objectid(val);
    return decl_props[id-1]->same(&t, oid);
  }

  static bool add(T *obj, PropertyAccessBase *pa, size_t &size, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId == info.classId) {
      size += pa->storage->size(obj, pa);
      return true;
    }
    else if(pa->classId) {
      if(UP && ClassTraits<SUP>::info.classId != info.classId && ClassTraits<SUP>::add(obj, pa, size, FLAG_UP))
        return true;
      return DN && sub::resolve<T, SUBS...>().add(obj, pa, size);
    }
    return false;
  }

  static ObjectId get_id(const std::shared_ptr<T> &obj, unsigned flags=FLAGS_ALL)
  {
    ObjectId objId = get_objectid(obj, false);
    if(objId) return objId;
    else {
      objId = UP ? ClassTraits<SUP>::get_id(obj, FLAG_UP) : 0;
      return objId ? objId : (DN ? sub::resolve<T, SUBS...>().get_id(obj) : 0);
    }
  }

  static bool save(WriteTransaction *wtr,
                   ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId == info.classId) {
      pa->storage->save(wtr, classId, objectId, obj, pa, mode);
      return true;
    }
    else if(pa->classId) {
      if(UP && ClassTraits<SUP>::info.classId != info.classId && ClassTraits<SUP>::save(wtr, classId, objectId, obj, pa, mode, FLAG_UP))
        return true;
      return DN && sub::resolve<T, SUBS...>().save(wtr, classId, objectId, obj, pa, mode);
    }
    return false;
  }

  static bool load(ReadTransaction *tr,
                   ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId == info.classId) {
      pa->storage->load(tr, buf, classId, objectId, obj, pa);
      return true;
    }
    else if(pa->classId) {
      if(UP && ClassTraits<SUP>::info.classId != info.classId && ClassTraits<SUP>::load(tr, buf, classId, objectId, obj, pa, FLAG_UP))
        return true;
      return DN && sub::resolve<T, SUBS...>().load(tr, buf, classId, objectId, obj, pa);
    }
    return false;
  }

  /**
   * put (copy) the value of the given property into value
   */
  template <typename TV>
  static void put(T &d, const PropertyAccessBase *pa, TV &value, unsigned flags=FLAGS_ALL) {
    if(pa->classId == info.classId) {
      const PropertyAccess <T, TV> *acc = (const PropertyAccess <T, TV> *) pa;
      value = acc->get(d);
    }
    else if(UP && pa->classId)
      ClassTraits<SUP>::put(d, pa, value, FLAG_UP);
  }

  /**
   * update the given property using value
   */
  template <typename TV>
  static void get(T &d, const PropertyAccessBase *pa, TV &value, unsigned flags=FLAGS_ALL) {
    if(pa->classId == info.classId) {
      const PropertyAccess<T, TV> *acc = (const PropertyAccess<T, TV> *)pa;
      acc->set(d, value);
    }
    else if(UP && pa->classId)
      ClassTraits<SUP>::get(d, pa, value, FLAG_UP);
  }
};

template <>
struct ClassTraits<EmptyClass> {
  static FlexisPersistence_EXPORT ClassInfo info;
  static FlexisPersistence_EXPORT Properties * properties;
  static FlexisPersistence_EXPORT const unsigned decl_props_sz = 0;
  static FlexisPersistence_EXPORT PropertyAccessBase * decl_props[0];

  template <typename T>
  static bool add(T *obj, PropertyAccessBase *pa, size_t &size, unsigned flags) {
    return false;
  }
  template <typename T>
  static ObjectId get_id(const std::shared_ptr<T> &obj, unsigned flags) {
    return 0;
  }
  template <typename T>
  static bool save(WriteTransaction *wtr, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode, unsigned flags) {
    return false;
  }
  template <typename T>
  static bool load(ReadTransaction *tr, ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, unsigned flags) {
    return false;
  }
  template <typename T, typename TV>
  static void put(T &d, const PropertyAccessBase *pa, TV &value, unsigned flags) {
  }
  template <typename T, typename TV>
  static void get(T &d, const PropertyAccessBase *pa, TV &value, unsigned flags) {
  }
};

template<typename T>
static PropertyType object_vector_t() {
  using Traits = ClassTraits<T>;
  return PropertyType(Traits::info.name, true);
}

template<typename T>
static PropertyType object_t() {
  using Traits = ClassTraits<T>;
  return PropertyType(Traits::info.name);
}

template <typename T> bool is_new(std::shared_ptr<T> obj) {
  return ClassTraits<T>::get_id(obj) == 0;
}

} //kv
} //persistence
} //flexis

using NO_SUPERCLASS = flexis::persistence::kv::EmptyClass;

//convenience macros for manually defining mappings

/**
 * start the mapping header.
 * @param cls the fully qualified class name
 */
#define START_MAPPINGHDR(cls) template <> struct ClassTraits<cls> : public ClassTraitsBase<cls>{

/**
 * start the mapping header with inheritance.
 * @param cls the fully qualified class name
 */
#define START_MAPPINGHDR_INH(cls, base) template <> struct ClassTraits<cls> : public base{

#endif //FLEXIS_FLEXIS_KVTRAITS_H

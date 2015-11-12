//
// Created by cse on 10/9/15.
//

#ifndef FLEXIS_FLEXIS_KVTRAITS_H
#define FLEXIS_FLEXIS_KVTRAITS_H

#include <string>
#include <vector>
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

namespace basetypes {

#define DEF_BASETYPE(TNAME, NUM, SZ) static const PropertyType TNAME ## _t = {NUM, SZ}; \
static const PropertyType TNAME ## _array_t = {NUM, SZ, true};

DEF_BASETYPE(short, 1, 2)
DEF_BASETYPE(ushort, 2, 2)
DEF_BASETYPE(int, 3, 4)
DEF_BASETYPE(uint, 4, 4)
DEF_BASETYPE(long, 5, 8)
DEF_BASETYPE(ulong, 6, 8)
DEF_BASETYPE(long_long, 7, 16)
DEF_BASETYPE(ulong_long, 8, 16)
DEF_BASETYPE(bool, 9, 1)
DEF_BASETYPE(float, 10, 4)
DEF_BASETYPE(double, 11, 8)

//variable size types
DEF_BASETYPE(string, 12, 0)
DEF_BASETYPE(cstring, 13, 0)

} //basetypes

template <typename T> struct TypeTraits;
#define TYPETRAITS template <> struct TypeTraits
#define TYPETRAITSV template <> struct TypeTraits<std::vector

TYPETRAITS<short>             {static const PropertyType &pt(){return basetypes::short_t;}};
TYPETRAITS<unsigned short>    {static const PropertyType &pt(){return basetypes::ushort_t;}};
TYPETRAITS<int>               {static const PropertyType &pt(){return basetypes::int_t;}};
TYPETRAITS<unsigned int>      {static const PropertyType &pt(){return basetypes::uint_t;}};
TYPETRAITS<long>              {static const PropertyType &pt(){return basetypes::long_t;}};
TYPETRAITS<unsigned long>     {static const PropertyType &pt(){return basetypes::ulong_t;}};
TYPETRAITS<long long>         {static const PropertyType &pt(){return basetypes::long_long_t;}};
TYPETRAITS<unsigned long long>{static const PropertyType &pt(){return basetypes::ulong_long_t;}};
TYPETRAITS<float>             {static const PropertyType &pt(){return basetypes::float_t;}};
TYPETRAITS<double>            {static const PropertyType &pt(){return basetypes::double_t;}};
TYPETRAITS<bool>              {static const PropertyType &pt(){return basetypes::bool_t;}};
TYPETRAITS<const char *>      {static const PropertyType &pt(){return basetypes::cstring_t;}};
TYPETRAITS<std::string>       {static const PropertyType &pt(){return basetypes::string_t;}};

TYPETRAITSV<short>>             {static const PropertyType &pt(){return basetypes::short_array_t;}};
TYPETRAITSV<unsigned short>>    {static const PropertyType &pt(){return basetypes::ushort_array_t;}};
TYPETRAITSV<int>>               {static const PropertyType &pt(){return basetypes::int_array_t;}};
TYPETRAITSV<unsigned int>>      {static const PropertyType &pt(){return basetypes::uint_array_t;}};
TYPETRAITSV<long>>              {static const PropertyType &pt(){return basetypes::long_array_t;}};
TYPETRAITSV<unsigned long>>     {static const PropertyType &pt(){return basetypes::ulong_array_t;}};
TYPETRAITSV<long long>>         {static const PropertyType &pt(){return basetypes::long_long_array_t;}};
TYPETRAITSV<unsigned long long>>{static const PropertyType &pt(){return basetypes::ulong_long_array_t;}};
TYPETRAITSV<float>>             {static const PropertyType &pt(){return basetypes::float_array_t;}};
TYPETRAITSV<double>>            {static const PropertyType &pt(){return basetypes::double_array_t;}};
TYPETRAITSV<bool>>              {static const PropertyType &pt(){return basetypes::bool_array_t;}};
TYPETRAITSV<const char *>>       {static const PropertyType &pt(){return basetypes::cstring_array_t;}};
TYPETRAITSV<std::string>>       {static const PropertyType &pt(){return basetypes::string_array_t;}};

class ReadTransaction;
class WriteTransaction;
class PropertyAccessBase;

/**
 * abstract superclass for all Store Access classes
 */
struct StoreAccessBase {
  virtual size_t size(const byte_t *buf) const = 0;
  virtual size_t size(void *obj, const PropertyAccessBase *pa) {return 0;};
  virtual void save(WriteTransaction *tr,
                    ClassId classId, ObjectId objectId,
                    void *obj, const PropertyAccessBase *pa,
                    bool force=false) = 0;
  virtual void load(ReadTransaction *tr,
                    ReadBuf &buf,
                    ClassId classId, ObjectId objectId,
                    void *obj, const PropertyAccessBase *pa,
                    bool force=false) = 0;
};

/**
 * abstract superclass for all Store Access classes that represent property values that are saved
 * under individual object keys, with the key value saved in the enclosing object's buffer
 */
struct StoreAccessEmbeddedKey: public StoreAccessBase
{
  size_t size(const byte_t *buf) const override {return StorageKey::byteSize;}
  size_t size(void *obj, const PropertyAccessBase *pa) override {return StorageKey::byteSize;}
};

/**
 * abstract superclass for all Store Access classes that represent property values that are saved
 * under a property key, with nothing saved in the enclosing object's buffer
 */
struct StoreAccessPropertyKey: public StoreAccessBase
{
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
struct ValueTraits : public ValueTraitsFixed<true>
{
  size_t data_size(const byte_t *) override {
    return TypeTraits<T>::pt().byteSize;
  }
  static size_t size(const T &val) {
    return TypeTraits<T>::pt().byteSize;
  }
  static void getBytes(ReadBuf &buf, T &val) {
    size_t byteSize = TypeTraits<T>::pt().byteSize;
    const byte_t *data = buf.read(byteSize);
    val = read_integer<T>(data, byteSize);
  }
  static void putBytes(WriteBuf &buf, T val) {
    size_t byteSize = TypeTraits<T>::pt().byteSize;
    byte_t *data = buf.allocate(byteSize);
    write_integer(data, val, byteSize);
  }
};

template <>
struct ValueTraits<bool> : public ValueTraitsFixed<true>
{
  static size_t size(const bool &val) {
    return TypeTraits<bool>::pt().byteSize;
  }
  static void getBytes(ReadBuf &buf, bool &val) {
    const byte_t *data = buf.read(1);
    val = *data != 0;
  }
  static void putBytes(WriteBuf &buf, bool val) {
    byte_t *data = buf.allocate(1);
    *data = char(val ? 1 : 0);
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
    return TypeTraits<T>::pt().byteSize;
  }
  static size_t size(const T &val) {
    return TypeTraits<T>::pt().byteSize;
  }
  static void getBytes(ReadBuf &buf, T &val) {
    size_t byteSize = TypeTraits<T>::pt().byteSize;
    const byte_t *data = buf.read(byteSize);
    val = *reinterpret_cast<const T *>(data);
  }
  static void putBytes(WriteBuf &buf, T val) {
    size_t byteSize = TypeTraits<T>::pt().byteSize;
    byte_t *data = buf.allocate(byteSize);
    *reinterpret_cast<T *>(data) = val;
  }
};

template <>
struct ValueTraits<float> : public ValueTraitsFloat<float> {};
template <>
struct ValueTraits<double> : public ValueTraitsFloat<double> {};

struct PropertyAccessBase
{
  const char * const name;
  bool enabled = true;
  unsigned id = 0;
  StoreAccessBase *storage;
  const PropertyType type;
  PropertyAccessBase(const char * name, StoreAccessBase *storage, const PropertyType &type)
      : name(name), storage(storage), type(type) {}
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
      : PropertyAssign<O, P, p>(name, new PropertyStorage<O, P>(), TypeTraits<P>::pt()) {}
};

template <typename T> struct ClassTraits;

struct EmptyClass
{
};


class Properties
{
  const unsigned numProps;
  PropertyAccessBase ** const properties;
  Properties * const superIter;
  const unsigned startPos;

  Properties(PropertyAccessBase * properties[], unsigned numProps, Properties *superIter)
      : numProps(numProps), properties(properties), superIter(superIter), startPos(superIter ? superIter->full_size() : 0)
  {}

  Properties(const Properties& mit) = delete;
public:
  template <typename T, typename S=EmptyClass>
  static Properties *mk() {
    Properties *p = new Properties(
        ClassTraits<T>::decl_props,
        ARRAY_SZ(ClassTraits<T>::decl_props),
        ClassTraits<S>::properties);

    for(unsigned i=0; i<p->full_size(); i++)
      p->get(i)->id = i+1;

    return p;
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
  ClassId classId;
  ObjectId maxObjectId = 0;

  ClassInfo(const char *name, const std::type_info &typeinfo, ClassId classId=MIN_USER_CLSID)
      : name(name), typeinfo(typeinfo), classId(classId) {}
};
template <typename T>
struct ClassTraitsBase
{
  static ClassInfo info;
  static Properties * properties;
  static PropertyAccessBase * decl_props[];

  /**
   * put (copy) the value of the given property into value
   */
  template <typename TV>
  static void put(T &d, const PropertyAccessBase *pa, TV &value) {
    const PropertyAccess<T, TV> *acc = (const PropertyAccess<T, TV> *)pa;
    value = acc->get(d);
  }

  /**
   * update the given property using value
   */
  template <typename TV>
  static void get(T &d, const PropertyAccessBase *pa, TV &value) {
    const PropertyAccess<T, TV> *acc = (const PropertyAccess<T, TV> *)pa;
    acc->set(d, value);
  }
};

template <>
struct ClassTraits<EmptyClass> {
  static FlexisPersistence_EXPORT Properties * properties;
  static PropertyAccessBase * decl_props[0];
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

} //kv
} //persistence
} //flexis


#endif //FLEXIS_FLEXIS_KVTRAITS_H

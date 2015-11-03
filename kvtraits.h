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
#include "FlexisPersistence_Export.h"

namespace flexis {
namespace persistence {
namespace kv {

#define ARRAY_SZ(x) unsigned(sizeof(x) / sizeof(decltype(*x)))

using ClassId = uint16_t;
using ObjectId = uint32_t;
using PropertyId = uint16_t;

//faster than pow()
static long byte_facts[] = {
    1,
    256,
    65536,
    16777216,
    4294967296,
    1099511627776,
    281474976710656,
    72057594037927936
};

/*
 * save an integral value to a fixed size of bytes (max. 8)
 */
template<typename T>
inline void write_integer(char *ptr, T val, unsigned bytes)
{
  ptr += bytes-1;
  for(int i=0; i<bytes; i++) {
    T tval = (T)(val / byte_facts[bytes-1-i]);
    *ptr = (unsigned char)(tval);
    val -= tval * byte_facts[bytes-1-i];
    ptr--;
  }
}

/*
 * read an integral value from a fixed size of bytes (max. 8)
 */
template<typename T>
inline T read_integer(const char *ptr, unsigned bytes)
{
  T val = 0;
  for(int i=0; i<bytes; i++, ptr++) {
    val += (T)*ptr * (T) byte_facts[i];
  }
  return val;
}

/**
 * a storage key. This structure must not be changed (lest db files become unreadable)
 */
struct StorageKey
{
  static const unsigned byteSize = sizeof(ClassId) + sizeof(ObjectId) + sizeof(PropertyId);

  ClassId classId;
  ObjectId objectId;
  PropertyId propertyId; //will be 0 is this is an object key

  StorageKey() : classId(0), objectId(0), propertyId(0) {}
  StorageKey(ClassId classId, ObjectId objectId, PropertyId propertyId) : classId(classId), objectId(objectId), propertyId(propertyId) {}
};

/**
 * a read buffer. Note: with LMDB, this points into mapped memory. Neither the buffer itself nor pointers returned
 * from read() should be kept around. Longer-lived data should be copied away
 */
class ReadBuf
{
  ReadBuf(const ReadBuf &other) = delete;

protected:
  char *m_data = nullptr;
  char *m_readptr = nullptr;
  size_t m_size = 0;

public:
  ReadBuf() {}

  void start(char *data, size_t size) {
    m_size = size;
    m_readptr = m_data = data;
  }

  bool empty() {return m_size == 0;}

  /**
   * @return a read-only pointer into the store-owned memory
   */
  const char *read(size_t sz) {
    char *ret = m_readptr;
    m_readptr += sz;
    return ret;
  }

  const char *readCString() {
    const char *cstr = m_readptr;
    while(*m_readptr) m_readptr++;
    m_readptr++;
    return cstr;
  }

  bool read(StorageKey &key)
  {
    if(m_size - (m_readptr - m_data) < 8)
      return false;

    key.classId = read_integer<ClassId>(m_readptr, sizeof(ClassId));
    m_readptr += sizeof(ClassId);
    key.objectId = read_integer<ObjectId>(m_readptr, sizeof(ObjectId));
    m_readptr += sizeof(ObjectId);
    key.propertyId = read_integer<PropertyId>(m_readptr, sizeof(PropertyId));
    m_readptr += sizeof(PropertyId);

    return true;
  }
};

class WriteBuf
{
  WriteBuf(const WriteBuf &other) = delete;

  char *m_appendptr = nullptr;
  char * m_data = nullptr;
  size_t m_size = 0;

  WriteBuf *prev = nullptr, *next = nullptr;

public:
  WriteBuf() {}
  WriteBuf(WriteBuf *_prev) : prev(_prev) {}
  WriteBuf(size_t sz) {start(sz);}

  ~WriteBuf() {
    if(m_data) free(m_data);
    m_appendptr = m_data = nullptr;
  }

  WriteBuf *push() {
    if(!next)
      next = new WriteBuf(this);
    return next;
  }

  WriteBuf *pop() {
    return prev;
  }

  void deleteChain() {
    if(next) next->deleteChain();
    delete next;
  }

  void start(size_t newSize)
  {
    if(newSize > m_size) {
      m_size = newSize;
      if(m_data)
        m_data = (char *)realloc(m_data, m_size);
      else
        m_data = (char *)malloc(m_size);
    }
    m_appendptr = m_data;
  }

  void reset() {
    m_appendptr = m_data;
  }

  char *allocate(size_t size) {
    char *ret = m_appendptr;
    m_appendptr += size;
    return ret;
  }

  void append(const char *data, size_t sz) {
    memcpy(m_appendptr, data, sz);
    m_appendptr += sz;
  }

  void appendCString(const char *data) {
    while(*data) {
      *m_appendptr = *data;
      m_appendptr++;
      data++;
    }
    *m_appendptr = 0;
    ++m_appendptr;
  }

  void append(ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    write_integer(m_appendptr, classId, sizeof(ClassId));
    m_appendptr += sizeof(ClassId);
    write_integer(m_appendptr, objectId, sizeof(ObjectId));
    m_appendptr += sizeof(ObjectId);
    write_integer(m_appendptr, propertyId, sizeof(PropertyId));
    m_appendptr += sizeof(PropertyId);
  }

  const char * data() {
    return m_data;
  }

  size_t size() {
    return m_size;
  }
};

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
DEF_BASETYPE(long, 5, 4)
DEF_BASETYPE(ulong, 6, 4)
DEF_BASETYPE(long_long, 7, 8)
DEF_BASETYPE(ulong_long, 8, 8)
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

struct StoreAccessBase {
  virtual size_t size(const char *buf) {return 0;};
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
template<typename T, typename V> struct PropertyStorage : public StoreAccessBase {};

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

template <typename T>
struct ValueTraits {
  static void getBytes(ReadBuf &buf, T &val) {
    size_t byteSize = TypeTraits<T>::pt().byteSize;
    const char *data = buf.read(byteSize);
    val = read_integer<T>(data, byteSize);
  }
  static void putBytes(WriteBuf &buf, T val) {
    size_t byteSize = TypeTraits<T>::pt().byteSize;
    char *data = buf.allocate(byteSize);
    write_integer(data, val, byteSize);
  }
};

template <>
struct ValueTraits<bool> {
  static void getBytes(ReadBuf &buf, bool &val) {
    const char *data = buf.read(1);
    val = *data != 0;
  }
  static void putBytes(WriteBuf &buf, bool val) {
    char *data = buf.allocate(1);
    *data = val ? 1 : 0;
  }
};

template <>
struct ValueTraits<std::string>
{
  static void getBytes(ReadBuf &buf, std::string &val) {
    val = buf.read(0);
    buf.read(val.length() +1); //move the pointer
  }
  static void putBytes(WriteBuf &buf, std::string val) {
    buf.append(val.c_str(), val.length()+1);
  }
};

template <>
struct ValueTraits<const char *>
{
  static void getBytes(ReadBuf &buf, const char *&val) {
    val = buf.readCString();
  }
  static void putBytes(WriteBuf &buf, const char *&val) {
    buf.appendCString(val);
  }
};

template <typename T>
struct ValueTraitsFloat
{
  static void getBytes(ReadBuf &buf, T &val) {
    size_t byteSize = TypeTraits<T>::pt().byteSize;
    const char *data = buf.read(byteSize);
    val = *reinterpret_cast<const T *>(data);
  }
  static void putBytes(WriteBuf &buf, T val) {
    size_t byteSize = TypeTraits<T>::pt().byteSize;
    char *data = buf.allocate(byteSize);
    *reinterpret_cast<T *>(data) = val;
  }
};

template <>
struct ValueTraits<float> : public ValueTraitsFloat<float> {};
template <>
struct ValueTraits<double> : public ValueTraitsFloat<double> {};

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
  ClassInfo(const ClassInfo &other) = delete;

  const char *name;
  const std::type_info &typeinfo;
  ClassId classId = 0;
  ObjectId maxObjectId = 0;

  ClassInfo(const char *name, const std::type_info &typeinfo) : name(name), typeinfo(typeinfo) {}
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

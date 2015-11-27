//
// Created by cse on 11/6/15.
//

#ifndef FLEXIS_KVWRITEBUF_H
#define FLEXIS_KVWRITEBUF_H

#include <persistence_error.h>
#include <string.h>
#include <memory>

namespace flexis {
namespace persistence {
namespace kv {

using ClassId = uint16_t;
static const size_t ClassId_sz = 2; //max. 65535 classes
using ObjectId = uint32_t;
static const size_t ObjectId_sz = 4; //max 2^32 objects per class
using PropertyId = uint16_t;
static const size_t PropertyId_sz = 2; //max. 65535 properies per object

static const size_t ObjectHeader_sz = ClassId_sz + ObjectId_sz + 4;
static const size_t ChunkHeader_sz = 4 * 3;

using byte_t = unsigned char;

/**
 * custom deleter used to store the objectId inside a std::shared_ptr
 */
template <typename T> struct object_handler
{
  ObjectId objectId;

  object_handler(ObjectId objectId) : objectId(objectId) {}

  void operator () (T *t) {
    delete t;
  }
};

/**
 * create an object wrapped by a shared_ptr ready to be input into the KV API. All shared_ptr objects passed
 * to the KV API must have been created through this method (or KV itself)
 */
template <typename T> std::shared_ptr<T> make_obj()
{
  return std::shared_ptr<T>(new T(), object_handler<T>(0));
}

/**
 * create a shared_ptr ready to be input into the KV API. All shared_ptr objects passed to the KV API
 * must have been created through this method (or KV itself)
 */
template <typename T> std::shared_ptr<T> make_ptr(T *t)
{
  return std::shared_ptr<T>(t, object_handler<T>(0));
}

/**
 * @return the ObjectId which was stored inside the pointer
 * @throws persistence_error if the shared_ptr was not created via make_shared_ptr
 */
template<typename T> ObjectId get_objectid(std::shared_ptr<T> obj)
{
  object_handler<T> *ohm = std::get_deleter<object_handler<T>>(obj);
  if(!ohm) throw persistence_error("shared_ptr was not created by KV store");
  return ohm->objectId;
}

/**
 * @param obj a pointer to a mapped object
 * @param oid the ObjectId
 * @throws persistence_error if the shared_ptr was not created via make_shared_ptr
 */
template<typename T> void set_objectid(const std::shared_ptr<T> obj, ObjectId  oid)
{
  object_handler<T> *ohm = std::get_deleter<object_handler<T>>(obj);
  if(!ohm) throw persistence_error("shared_ptr was not created by KV store");
  ohm->objectId = oid;
}

/**
 * a storage key. This structure must not be changed (lest db files become unreadable)
 */
struct StorageKey
{
  static const unsigned byteSize = ClassId_sz + ObjectId_sz + PropertyId_sz;

  ClassId classId;
  ObjectId objectId;
  PropertyId propertyId; //will be 0 if this is an object key

  bool dirty; //used by objects that carry a StorageKey property

  StorageKey(bool dirty=false) : classId(0), objectId(0), propertyId(0), dirty(dirty) {}
  StorageKey(ClassId classId, ObjectId objectId, PropertyId propertyId, bool dirty=false)
      : classId(classId), objectId(objectId), propertyId(propertyId), dirty(dirty) {}
};

/*
 * save an integral value to a fixed size of bytes (max. 8)
 */
template<typename T>
inline void write_integer(byte_t *ptr, T val, size_t bytes)
{
  for(size_t i=0, f=bytes-1; i<bytes; i++, f--)
    ptr[i] = i<sizeof(T) ? (byte_t) (val >> (f * 8)) : (byte_t)0;
}

/*
 * read an integral value from a fixed size of bytes (max. 8)
 */
template<typename T>
inline T read_integer(const byte_t *ptr, size_t bytes)
{
  T val = (T)0;
  for(size_t i=0, f=bytes-1; i<bytes; i++, f--) val += ((T)ptr[i] << (f * 8));
  return val;
}

/**
 * a read buffer. Note: with LMDB, this points into mapped memory. Neither the buffer itself nor pointers returned
 * from read() should be kept around. Longer-lived data must be copied away
 */
class ReadBuf
{
  ReadBuf(const ReadBuf &other) = delete;

protected:
  byte_t *m_data = nullptr;
  byte_t *m_readptr = nullptr;
  size_t m_size = 0;

public:
  ReadBuf() {}

  byte_t *&data() {return m_data;}
  byte_t *&cur() {return m_readptr;}

  void start(byte_t *data, size_t size) {
    m_size = size;
    m_readptr = m_data = data;
  }

  size_t size() {return m_size;}
  bool empty() {return m_size == 0;}

  /**
   * @return a read-only pointer into the store-owned memory
   */
  const byte_t *read(size_t sz) {
    byte_t *ret = m_readptr;
    m_readptr += sz;
    return ret;
  }

  const char *readCString() {
    const char *cstr = (const char *)m_readptr;
    while(*m_readptr) m_readptr++;
    m_readptr++;
    return cstr;
  }

  template <typename T>
  T readInteger(unsigned sz) {
    T ret = read_integer<T>(m_readptr, sz);
    m_readptr += sz;
    return ret;
  }

  //static_assert(TypeTraits<T>::byteSize == sizeof(T), "raw API only usable for types where sizeof matches TypeTraits<T>::byteSize");
  template<typename T>
  T readRaw() {
    T val = *(T *)m_readptr;
    m_readptr += sizeof(T);
    return val;
  }

  bool atEnd() {
    return m_readptr == m_data + m_size;
  }

  bool read(StorageKey &key)
  {
    if(m_size - (m_readptr - m_data) < 8)
      return false;

    key.classId = *(ClassId *)m_readptr;
    m_readptr += ClassId_sz;
    key.objectId = *(ObjectId *)m_readptr;
    m_readptr += ObjectId_sz;
    key.propertyId = *(PropertyId *)m_readptr;
    m_readptr += PropertyId_sz;

    return true;
  }
};

/**
 * a dynamically growing writable buffer
 */
class WriteBuf
{
  static const size_t min_size = 128;

  WriteBuf(const WriteBuf &other) = delete;

  byte_t *m_appendptr = nullptr;
  byte_t * m_data = nullptr;
  size_t m_growsize = 0;
  size_t m_allocsize = 0;

  WriteBuf *prev = nullptr, *next = nullptr;

public:
  WriteBuf() {}
  WriteBuf(WriteBuf *_prev) : prev(_prev) {}
  WriteBuf(size_t sz) {start(sz);}
  WriteBuf(byte_t *data, size_t sz) {start(data, sz);}

  ~WriteBuf() {
    if(m_data && m_growsize) free(m_data);
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

  void start(byte_t * data, size_t offset, size_t newSize)
  {
    if(m_data && m_growsize) free(m_data);

    m_growsize = 0;
    m_allocsize = newSize;
    m_data = data;
    m_appendptr = m_data+offset;
  }

  void start(byte_t * data, size_t newSize)
  {
    start(data, 0, newSize);
  }

  void start(size_t newSize, size_t grow)
  {
    if(!m_growsize) {
      //prevent realloc of external memory
      m_allocsize = 0;
      m_data = nullptr;
    }

    if(newSize > m_allocsize) {
      m_allocsize = newSize;
      m_data = (byte_t *)realloc(m_data, m_allocsize);
    }
    m_growsize = grow;
    m_appendptr = m_data;
  }

  void start(size_t newSize)
  {
    if(newSize < min_size) newSize = min_size;
    start(newSize, newSize);
  }

  void reset() {
    if(m_growsize == 0) {
      m_allocsize = 0;
      m_data = nullptr;
    }
    m_appendptr = m_data;
  }

  inline byte_t *allocate(size_t size)
  {
    byte_t * ret = m_appendptr;
    m_appendptr += size;
#if DEBUG
    size_t sz = m_appendptr - m_data;
    if(sz > m_allocsize) {
      if(m_growsize == 0)
        throw persistence_error("memory exhausted");

      m_allocsize += m_growsize;
      m_data = (byte_t *)realloc(m_data, m_allocsize);
      m_appendptr = m_data + sz;
      ret = m_appendptr - size;
    }
#endif
    return ret;
  }

  void append(const char *data, size_t size)
  {
    byte_t * buf = allocate(size);
    memcpy(buf, data, size);
  }

  void append(byte_t *data, size_t size)
  {
    byte_t * buf = allocate(size);
    memcpy(buf, data, size);
  }

  //static_assert(TypeTraits<T>::byteSize == sizeof(T), "raw API only usable for types where sizeof matches TypeTraits<T>::byteSize");
  template<typename T>
  void appendRaw(T num) {
    *(T *)m_appendptr = num;
    m_appendptr += sizeof(T);
  }

  template<typename T>
  void appendInteger(T num, size_t bytes) {
    byte_t * buf = allocate(bytes);
    write_integer(buf, num, bytes);
  }

  void appendCString(const char *data) {
    size_t len = strlen(data) + 1;
    byte_t * buf = allocate(len);
    memcpy(buf, data, len);
  }

  void append(ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    byte_t * buf = allocate(StorageKey::byteSize);

    *(ClassId *)buf = classId;
    buf += ClassId_sz;
    *(ObjectId *)buf = objectId;
    buf += ObjectId_sz;
    *(PropertyId *)buf = propertyId;
    buf += PropertyId_sz;
  }

  byte_t * data() {
    return m_data;
  }

  size_t allocSize() {
    return m_allocsize;
  }
  size_t size() {
    return m_appendptr - m_data;
  }

  size_t avail() {
    return m_allocsize - (m_appendptr - m_data);
  }
};

} //kv
} //persistence
} //flexis

#endif //FLEXIS_KVWRITEBUF_H

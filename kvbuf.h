//
// Created by cse on 11/6/15.
//

#ifndef FLEXIS_KVWRITEBUF_H
#define FLEXIS_KVWRITEBUF_H

#include <persistence_error.h>
#include <string.h>

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

/**
 * a storage key. This structure must not be changed (lest db files become unreadable)
 */
struct StorageKey
{
  static const unsigned byteSize = ClassId_sz + ObjectId_sz + PropertyId_sz;

  ClassId classId;
  ObjectId objectId;
  PropertyId propertyId; //will be 0 is this is an object key

  StorageKey() : classId(0), objectId(0), propertyId(0) {}
  StorageKey(ClassId classId, ObjectId objectId, PropertyId propertyId) : classId(classId), objectId(objectId), propertyId(propertyId) {}
};

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
inline void write_unsigned(char *ptr, T val, size_t bytes)
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
inline T read_unsigned(const char *ptr, size_t bytes)
{
  T val = 0;
  for(int i=0; i<bytes; i++, ptr++) {
    val += (T)*(unsigned char *)ptr * (T) byte_facts[i];
  }
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
  char *m_data = nullptr;
  char *m_readptr = nullptr;
  size_t m_size = 0;

public:
  ReadBuf() {}

  char *&data() {return m_data;}
  char *&cur() {return m_readptr;}

  void start(char *data, size_t size) {
    m_size = size;
    m_readptr = m_data = data;
  }

  size_t size() {return m_size;}
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

  template <typename T>
  T readInteger(unsigned sz) {
    T ret = read_unsigned<T>(m_readptr, sz);
    m_readptr += sz;
    return ret;
  }

  bool read(StorageKey &key)
  {
    if(m_size - (m_readptr - m_data) < 8)
      return false;

    key.classId = read_unsigned<ClassId>(m_readptr, ClassId_sz);
    m_readptr += ClassId_sz;
    key.objectId = read_unsigned<ObjectId>(m_readptr, ObjectId_sz);
    m_readptr += ObjectId_sz;
    key.propertyId = read_unsigned<PropertyId>(m_readptr, PropertyId_sz);
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

  char *m_appendptr = nullptr;
  char * m_data = nullptr;
  size_t m_growsize = 0;
  size_t m_allocsize = 0;

  WriteBuf *prev = nullptr, *next = nullptr;

public:
  WriteBuf() {}
  WriteBuf(WriteBuf *_prev) : prev(_prev) {}
  WriteBuf(size_t sz) {start(sz);}
  WriteBuf(char *data, size_t sz) {start(data, sz);}

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

  void start(char * data, size_t offset, size_t newSize)
  {
    if(m_data && m_growsize) free(m_data);

    m_growsize = 0;
    m_allocsize = newSize;
    m_data = data;
    m_appendptr = m_data+offset;
  }

  void start(char * data, size_t newSize)
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
      m_data = (char *)realloc(m_data, m_allocsize);
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

  char *allocate(size_t size)
  {
    char * ret = m_appendptr;
    m_appendptr += size;
    size_t sz = m_appendptr - m_data;
    if(sz > m_allocsize) {
      if(m_growsize == 0)
        throw persistence_error("memory exhausted");

      m_allocsize += m_growsize;
      m_data = (char *)realloc(m_data, m_allocsize);
      m_appendptr = m_data + sz;
      ret = m_appendptr - size;
    }
    return ret;
  }

  void append(const char *data, size_t size)
  {
    char * buf = allocate(size);
    memcpy(buf, data, size);
  }

  template<typename T>
  void appendInteger(T num, size_t bytes) {
    char * buf = allocate(bytes);
    write_unsigned(buf, num, bytes);
  }

  void appendCString(const char *data) {
    size_t len = strlen(data) + 1;
    char * buf = allocate(len);
    memcpy(buf, data, len);
  }

  void append(ClassId classId, ObjectId objectId, PropertyId propertyId)
  {
    char * buf = allocate(StorageKey::byteSize);

    write_unsigned(buf, classId, ClassId_sz);
    buf += ClassId_sz;
    write_unsigned(buf, objectId, ObjectId_sz);
    buf += ObjectId_sz;
    write_unsigned(buf, propertyId, PropertyId_sz);
    buf += PropertyId_sz;
  }

  char * data() {
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

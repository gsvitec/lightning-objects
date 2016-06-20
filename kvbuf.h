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

#ifndef FLEXIS_KVWRITEBUF_H
#define FLEXIS_KVWRITEBUF_H

#include <string.h>
#include <memory>

#include "kvkey.h"

namespace flexis {
namespace persistence {
namespace kv {

using ClassId = uint16_t;
static const size_t ClassId_sz = 2; //max. 65535 classes
using ObjectId = uint32_t;
static const size_t ObjectId_sz = 4; //max 2^32 objects per class
using PropertyId = uint16_t;
static const size_t PropertyId_sz = 2; //max. 65535 properies per object

static const size_t ObjectKey_sz = ClassId_sz + ObjectId_sz;

//header preceding each object in collections (classId, ObjectId, size, delete marker)
static const size_t ObjectHeader_sz = ClassId_sz + ObjectId_sz + 4 + 1;

//header preceding each collection chunk
static const size_t ChunkHeader_sz = 4 * 3;

using byte_t = unsigned char;

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
 * a read buffer. Note: with LMDB, this may point into mapped memory. Neither the buffer itself nor pointers returned
 * from read() should be kept around.
 */
class ReadBuf
{
  ReadBuf(const ReadBuf &other) = delete;

protected:
  byte_t *m_data = nullptr;
  byte_t *m_readptr = nullptr;
  byte_t *m_mark = nullptr;
  size_t m_size = 0;
  bool m_owned = false;

public:
  ReadBuf() {}
  ReadBuf(byte_t *data, size_t size) {start(data, size);}
  ~ReadBuf() {
    if(m_data && m_owned) {
      free(m_data);
      m_owned = false;
      m_data = nullptr;
    }
  }

  byte_t *&data() {return m_data;}
  byte_t *&cur() {return m_readptr;}

  byte_t *copyData() {
    if(!m_owned && m_data) {
      m_owned = true;
      byte_t *data = (byte_t *)malloc(m_size);
      memcpy(data, m_data, m_size);
      m_readptr = data + (m_readptr - m_data);
      if(m_mark) m_mark = data + (m_mark - m_data);
      m_data = data;
    }
    return m_data;
  }

  void start(byte_t *data, size_t size) {
    m_size = size;
    m_readptr = m_data = data;
  }

  size_t size() {return m_size;}
  bool empty() {return m_size == 0;}
  bool null() {return m_data == nullptr;}

  void mark() {
    m_mark = m_readptr;
  }

  void unmark(size_t offs=0) {
    m_readptr = m_mark ? m_mark + offs : m_readptr + offs;
  }

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

  template<typename T>
  T readRaw() {
    T val = *(T *)m_readptr;
    m_readptr += sizeof(T);
    return val;
  }

  bool atEnd() {
    return m_readptr == m_data + m_size;
  }

  bool read(ObjectKey &key)
  {
    return read(key.classId, key.objectId);
  }

  bool read(ClassId &classId, ObjectId &objectId)
  {
    if(m_size - (m_readptr - m_data) < ObjectKey_sz)
      return false;

    classId = *(ClassId *)m_readptr;
    m_readptr += ClassId_sz;
    objectId = *(ObjectId *)m_readptr;
    m_readptr += ObjectId_sz;

    return true;
  }

  size_t strlen() {
    return ::strlen((const char *)m_readptr);
  }

  void reset() {
    m_readptr = m_data;
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

  byte_t *cur() {
    return m_appendptr;
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
    return ret;
  }

  template <typename T>
  void append(T *data, size_t size)
  {
    byte_t * buf = allocate(size * sizeof(T));
    memcpy(buf, (byte_t *)data, size * sizeof(T));
  }

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

  void append(const ObjectKey &key)
  {
    byte_t * buf = allocate(ObjectKey_sz);

    *(ClassId *)buf = key.classId;
    buf += ClassId_sz;
    *(ObjectId *)buf = key.objectId;
    buf += ObjectId_sz;
  }

  void append(ClassId classId, ObjectId objectId)
  {
    byte_t * buf = allocate(ClassId_sz + ObjectId_sz);

    *(ClassId *)buf = classId;
    buf += ClassId_sz;
    *(ObjectId *)buf = objectId;
    buf += ObjectId_sz;
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

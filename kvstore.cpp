//
// Created by cse on 10/11/15.
//
#include <vector>
#include <set>
#include "kvstore.h"

namespace flexis {
namespace persistence {

using namespace std;

Properties * ClassTraits<kv::EmptyClass>::properties {nullptr};

inline bool streq(string s1, const char *s2) {
  if(s2 == nullptr) return s1.empty();
  return s1 == s2;
}

void KeyValueStoreBase::updateClassSchema(ClassInfo &classInfo, PropertyAccessBase * properties[], unsigned numProperties)
{
  vector<PropertyMetaInfoPtr> propertyInfos;
  loadSaveClassMeta(classInfo, properties, numProperties, propertyInfos);

  if(!propertyInfos.empty()) {
    //previous class schema found in db. check compatibility
    set<string> piNames;
    for(auto pi : propertyInfos) {
      piNames.insert(pi->name);
      for(auto i=0; i<numProperties; i++) {
        if(pi->name == properties[i]->name) {
          const PropertyAccessBase * pa = properties[i];
          if(pa->type.id != pi->typeId
             || pa->type.byteSize != pi->byteSize || !streq(pi->className, pa->type.className)
             || pi->isVector != pa->type.isVector) {
            string msg = string("data type for property [") + pi->name + "] has changed";
            throw incompatible_schema_error(msg.c_str());
          }
        }
      }
    }
    for(auto i=0; i<numProperties; i++) {
      if(!piNames.count(properties[i]->name)) {
        //property doesn't exist in db. Either migrate db (currently unsupported) or disable locally (questionable)
        properties[i]->enabled = false;
      }
    }
  }
}

namespace kv {

void readObjectHeader(ReadBuf &buf, ClassId *classId, ObjectId *objectId, size_t *size)
{
  ClassId cid = buf.readInteger<ClassId>(ClassId_sz);
  if(classId) *classId = cid;
  ObjectId oid = buf.readInteger<ObjectId>(ObjectId_sz);
  if(objectId) *objectId = oid;
  size_t sz = buf.readInteger<size_t>(4);
  if(size) *size = sz;
}

size_t readChunkStartIndex(const char *data) {
  return read_integer<size_t>(data+4, 4);
}

void readChunkHeader(ReadBuf &buf, size_t *dataSize, size_t *startIndex, size_t *elementCount)
{
  size_t val = buf.readInteger<size_t>(4);
  if(dataSize) *dataSize = val;
  val = buf.readInteger<size_t>(4);
  if(startIndex) *startIndex = val;
  val = buf.readInteger<size_t>(4);
  if(elementCount) *elementCount = val;
}

void WriteTransaction::writeChunkHeader(size_t startIndex, size_t elementCount)
{
  //write to start of buffer. Space is preallocated in startChunk
  write_integer(writeBuf().data(), writeBuf().size(), 4);
  write_integer(writeBuf().data()+4, startIndex, 4);
  write_integer(writeBuf().data()+8, elementCount, 4);
}

void WriteTransaction::writeObjectHeader(ClassId classId, ObjectId objectId, size_t size)
{
  char * hdr = writeBuf().allocate(ObjectHeader_sz);
  write_integer<ClassId>(hdr, classId, ClassId_sz);
  write_integer<ObjectId>(hdr+ClassId_sz, objectId, ObjectId_sz);
  write_integer<size_t>(hdr+ClassId_sz+ObjectId_sz, size, 4);
}

bool WriteTransaction::startChunk(ObjectId collectionId, PropertyId chunkId, size_t chunkSize, size_t startIndex, size_t elementCount)
{
  if(elementCount > 0) writeChunkHeader(startIndex, elementCount);

  //allocate a new chunk
  char * data;
  if(allocData(COLLECTION_CLSID, collectionId, chunkId, chunkSize, &data)) {
    writeBuf().start(data, chunkSize);
    writeBuf().allocate(ChunkHeader_sz); //reserve for writing later
    return true;
  }
  return false;
}

CollectionCursorBase::CollectionCursorBase(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr chunkCursor)
: m_collectionId(collectionId), m_tr(tr), m_chunkCursor(chunkCursor)
{
  if(!m_chunkCursor->atEnd()) {
    m_chunkCursor->get(m_readBuf);
    readChunkHeader(m_readBuf, 0, 0, &m_elementCount);
  }
}

bool CollectionCursorBase::atEnd() {
  return m_curElement >= m_elementCount;
}

bool CollectionCursorBase::next()
{
  if(++m_curElement == m_elementCount && m_chunkCursor->next()) {
    m_chunkCursor->get(m_readBuf);
    readChunkHeader(m_readBuf, 0, 0, &m_elementCount);
    m_curElement = 0;
  }
  return m_curElement < m_elementCount;
}

CollectionAppenderBase::CollectionAppenderBase(ChunkCursor::Ptr chunkCursor, WriteTransaction *wtxn,
                                               ObjectId collectionId, size_t chunkSize)
    : m_chunkCursor(chunkCursor), m_chunkSize(chunkSize), m_wtxn(wtxn), m_collectionId(collectionId)
{
  bool needAlloc = true;
  if(!m_chunkCursor->atEnd()) {
    ReadBuf rb;
    m_chunkCursor->get(rb);

    size_t dataSize;
    readChunkHeader(rb, &dataSize, &m_startIndex, &m_elementCount);
    m_chunkId = m_chunkCursor->chunkId();

    if(m_wtxn->reuseChunkspace() && dataSize < rb.size()) {
      //there's more room. Try use that first
      m_wtxn->writeBuf().start(rb.data(), dataSize, rb.size());
      needAlloc = false;
    }
  }
  else {
    m_wtxn->getNextChunkInfo(collectionId, &m_chunkId, &m_startIndex);
  }

  if(needAlloc) {
    m_elementCount = 0;
    m_wtxn->startChunk(m_collectionId, ++m_chunkId, m_chunkSize, m_startIndex, 0);
  }
}

void CollectionAppenderBase::preparePut(size_t size)
{
  if(m_wtxn->writeBuf().avail() < size) {
    m_startIndex += m_elementCount;
    m_wtxn->startChunk(m_collectionId, ++m_chunkId, m_chunkSize, m_startIndex, m_elementCount);
    m_elementCount = 0;
  }
  m_elementCount++;
}

void CollectionAppenderBase::close() {
  if(m_elementCount) m_wtxn->writeChunkHeader(m_startIndex + m_elementCount, m_elementCount);
  m_chunkCursor->close();
}

}

} //persistence
} //flexis

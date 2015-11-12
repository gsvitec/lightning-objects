//
// Created by cse on 10/11/15.
//
#include <vector>
#include <set>
#include "kvstore.h"

namespace flexis {
namespace persistence {

using namespace std;

namespace kv {

template <>
struct ClassTraits<ChunkInfo> : public ClassTraitsBase<ChunkInfo>{
  enum PropertyIds {chunkId=1, startIndex, elementCount};
};
template<> ClassInfo ClassTraitsBase<ChunkInfo>::info ("flexis::persistence::ChunkInfo", typeid(ChunkInfo), CHUNKINFO_CLSID);
template<> PropertyAccessBase * ClassTraitsBase<ChunkInfo>::decl_props[] = {
    new BasePropertyAssign<ChunkInfo, PropertyId, &ChunkInfo::chunkId>("chunkId"),
    new BasePropertyAssign<ChunkInfo, size_t, &ChunkInfo::startIndex>("startIndex"),
    new BasePropertyAssign<ChunkInfo, size_t, &ChunkInfo::elementCount>("elementCount"),
    new BasePropertyAssign<ChunkInfo, size_t, &ChunkInfo::dataSize>("dataSize")
};
template<> Properties * ClassTraitsBase<ChunkInfo>::properties(Properties::mk<ChunkInfo>());

template <>
struct ClassTraits<CollectionInfo> : public ClassTraitsBase<CollectionInfo>{
  enum PropertyIds {collectionId=1, chunkInfos};
};
template<> ClassInfo ClassTraitsBase<CollectionInfo>::info ("flexis::persistence::CollectionInfo", typeid(CollectionInfo), COLLINFO_CLSID);
template<> PropertyAccessBase * ClassTraitsBase<CollectionInfo>::decl_props[] = {
    new BasePropertyAssign<CollectionInfo, ObjectId, &CollectionInfo::collectionId>("collectionId"),
    new ObjectVectorPropertyAssign<CollectionInfo, ChunkInfo, &CollectionInfo::chunkInfos>("chunkInfos")
};
template<> Properties * ClassTraitsBase<CollectionInfo>::properties(Properties::mk<CollectionInfo>());

}

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

void readChunkHeader(const char *data, size_t *dataSize, size_t *startIndex, size_t *elementCount)
{
  size_t val = read_integer<size_t>(data, 4);
  if(dataSize) *dataSize = val;
  data += 4;
  val = read_integer<size_t>(data, 4);
  if(startIndex) *startIndex = val;
  data += 4;
  val = read_integer<size_t>(data, 4);
  if(elementCount) *elementCount = val;
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
  char *data = writeBuf().data();
  write_integer(data, writeBuf().size(), 4);
  write_integer(data+4, startIndex, 4);
  write_integer(data+8, elementCount, 4);

  size_t tsz, tix, tec;
  tix = read_integer<size_t>(data + 4, 4);
  tec = tix;
}

void WriteTransaction::writeObjectHeader(ClassId classId, ObjectId objectId, size_t size)
{
  char * hdr = writeBuf().allocate(ObjectHeader_sz);
  write_integer<ClassId>(hdr, classId, ClassId_sz);
  write_integer<ObjectId>(hdr+ClassId_sz, objectId, ObjectId_sz);
  write_integer<size_t>(hdr+ClassId_sz+ObjectId_sz, size, 4);
}

void WriteTransaction::putCollectionInfo(CollectionInfo &info, size_t startIndex, size_t elementCount)
{
  ChunkInfo &ci = info.chunkInfos.back();
  ci.startIndex = startIndex;
  ci.elementCount = elementCount;
  ci.dataSize = writeBuf().size();

  size_t sz = calculateBuffer(&info, ClassTraits<CollectionInfo>::properties);
  writeBuf().start(sz);
  writeObject(COLLINFO_CLSID, info.collectionId, info, ClassTraits<CollectionInfo>::properties, false);
  putData(COLLINFO_CLSID, info.collectionId, 0, writeBuf());
}

bool ReadTransaction::getCollectionInfo(ObjectId collectionId, CollectionInfo &info, bool loadNextInfos)
{
  ReadBuf readBuf;
  getData(readBuf, COLLINFO_CLSID, collectionId, 0);
  if(!readBuf.empty()) {
    readObject(readBuf, info, COLLINFO_CLSID, collectionId);
    if(loadNextInfos) {
      getNextChunkInfo(collectionId, &info.nextChunkId, &info.nextStartIndex);
    }
    return true;
  }
  return false;
}

bool WriteTransaction::startChunk(CollectionInfo &collectionInfo,
                                  PropertyId chunkId, size_t chunkSize, size_t startIndex, size_t elementCount)
{
  if(elementCount > 0) {
    ChunkInfo &ci = collectionInfo.chunkInfos.back();
    ci.startIndex = startIndex;
    ci.elementCount = elementCount;
    ci.dataSize = writeBuf().size();

    writeChunkHeader(startIndex, elementCount);
  }

  //allocate a new chunk
  char * data;
  if(allocData(COLLECTION_CLSID, collectionInfo.collectionId, chunkId, chunkSize, &data)) {
    collectionInfo.chunkInfos.push_back(ChunkInfo(chunkId));
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
    : m_chunkCursor(chunkCursor), m_chunkSize(chunkSize), m_wtxn(wtxn)
{
  bool needAlloc = true;
  if(!m_chunkCursor->atEnd()) {
    m_wtxn->getCollectionInfo(collectionId, m_collectionInfo, false);
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
    m_wtxn->getCollectionInfo(collectionId, m_collectionInfo, true);
    m_startIndex = m_collectionInfo.nextStartIndex;
    m_wtxn->startChunk(m_collectionInfo, ++m_chunkId, m_chunkSize, m_startIndex, 0);
  }
}

void CollectionAppenderBase::preparePut(size_t size)
{
  if(m_wtxn->writeBuf().avail() < size) {
    m_startIndex += m_elementCount;
    m_wtxn->startChunk(m_collectionInfo, ++m_chunkId, m_chunkSize, m_startIndex, m_elementCount);
    m_elementCount = 0;
  }
  m_elementCount++;
}

void CollectionAppenderBase::close() {
  if(m_elementCount) m_wtxn->writeChunkHeader(m_startIndex, m_elementCount);
  m_chunkCursor->close();
  m_wtxn->putCollectionInfo(m_collectionInfo, m_startIndex, m_elementCount);
}

}

} //persistence
} //flexis

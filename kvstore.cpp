//
// Created by cse on 10/11/15.
//
#include <vector>
#include <set>
#include <sstream>
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
            stringstream ss;
            ss << "class " << classInfo.name << ": data type for property '" << pi->name << "' has changed";
            throw incompatible_schema_error(ss.str());
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

void readChunkHeader(const byte_t *data, size_t *dataSize, size_t *startIndex, size_t *elementCount)
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
  byte_t *data = writeBuf().data();
  write_integer(data, writeBuf().size(), 4);
  write_integer(data+4, startIndex, 4);
  write_integer(data+8, elementCount, 4);

  size_t tsz, tix, tec;
  tix = read_integer<size_t>(data + 4, 4);
  tec = tix;
}

void WriteTransaction::writeObjectHeader(ClassId classId, ObjectId objectId, size_t size)
{
  byte_t * hdr = writeBuf().allocate(ObjectHeader_sz);
  write_integer<ClassId>(hdr, classId, ClassId_sz);
  write_integer<ObjectId>(hdr+ClassId_sz, objectId, ObjectId_sz);
  write_integer<size_t>(hdr+ClassId_sz+ObjectId_sz, size, 4);
}

void WriteTransaction::putCollectionInfo(CollectionInfo *info, size_t elementCount)
{
  ChunkInfo &ci = info->chunkInfos.back();
  ci.startIndex = info->nextStartIndex;
  ci.elementCount = elementCount;
  ci.dataSize = writeBuf().size();

  info->nextStartIndex += elementCount;
}

void WriteTransaction::commit()
{
  for(auto &it : m_collectionInfos) {
    CollectionInfo *ci = it.second;
    size_t sz = sizeof(size_t) + ci->chunkInfos.size() * (PropertyId_sz + 3 * sizeof(size_t));
    writeBuf().start(sz);
    writeBuf().appendRaw(ci->chunkInfos.size());
    for(auto &ch : ci->chunkInfos) {
      writeBuf().appendRaw(ch.chunkId);
      writeBuf().appendRaw(ch.startIndex);
      writeBuf().appendRaw(ch.elementCount);
      writeBuf().appendRaw(ch.dataSize);
    }
    putData(COLLINFO_CLSID, ci->collectionId, 0, writeBuf());

    delete ci;
  }
  m_collectionInfos.clear();
  doCommit();
}

void ReadTransaction::abort()
{
  for(auto &it : m_collectionInfos) delete it.second;
  m_collectionInfos.clear();
  doAbort();
};

CollectionInfo *ReadTransaction::getCollectionInfo(ObjectId collectionId)
{
  CollectionInfo *info = nullptr;

  if(m_collectionInfos.count(collectionId) == 0) {
    ReadBuf readBuf;
    getData(readBuf, COLLINFO_CLSID, collectionId, 0);
    if(!readBuf.empty()) {
      info = new CollectionInfo(collectionId);
      size_t sz = readBuf.readRaw<size_t>();
      for(size_t i=0; i<sz; i++) {
        PropertyId chunkId = readBuf.readRaw<PropertyId>();
        size_t startIndex = readBuf.readRaw<size_t>();
        size_t elementCount = readBuf.readRaw<size_t>();
        size_t dataSize = readBuf.readRaw<size_t>();
        info->chunkInfos.push_back(ChunkInfo(chunkId, startIndex, elementCount, dataSize));
      }
      m_collectionInfos[collectionId] = info;

      info->init();
    }
  }
  else
    info = m_collectionInfos[collectionId];

  return info;
}

size_t WriteTransaction::startChunk(CollectionInfo *collectionInfo, size_t chunkSize, size_t elementCount)
{
  if(elementCount == 0) {
    //begin chunk sequence - try to append to cached chunk buffer
    if (!collectionInfo->chunkInfos.empty()) {
      ChunkInfo &ci = collectionInfo->chunkInfos.back();
      if (ci.dataSize < ci.chunkSize && ci.chunkData) {
        writeBuf().start(ci.chunkData + ci.dataSize, ci.chunkSize - ci.dataSize);
        return ci.elementCount;
      }
    }
  }
  else {
    //chunk was completed
    ChunkInfo &ci = collectionInfo->chunkInfos.back();
    ci.startIndex = collectionInfo->nextStartIndex;
    ci.elementCount = elementCount;
    ci.dataSize += writeBuf().size();

    writeChunkHeader(collectionInfo->nextStartIndex, elementCount);
  }
  //allocate a new chunk
  byte_t * data = nullptr;
  if(allocData(COLLECTION_CLSID, collectionInfo->collectionId, collectionInfo->nextChunkId, chunkSize, &data)) {
    collectionInfo->chunkInfos.push_back(ChunkInfo(collectionInfo->nextChunkId, chunkSize, data));
    writeBuf().start(data, chunkSize);
    writeBuf().allocate(ChunkHeader_sz); //reserve for writing later

    collectionInfo->nextChunkId++;
  }
  return 0;
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
  //TODO: check for appendability in collectioninfo
  bool needAlloc = true;
  m_collectionInfo = m_wtxn->getCollectionInfo(collectionId);
  if(!m_chunkCursor->atEnd()) {
    ReadBuf rb;
    m_chunkCursor->get(rb);

    size_t dataSize, startIndex;
    readChunkHeader(rb, &dataSize, &startIndex, &m_elementCount);

    if(m_wtxn->reuseChunkspace() && dataSize < rb.size()) {
      //there's more room. Try use that first
      m_wtxn->writeBuf().start(rb.data(), dataSize, rb.size());
      needAlloc = false;
    }
  }

  if(needAlloc) {
    m_elementCount = m_wtxn->startChunk(m_collectionInfo, m_chunkSize, 0);
  }
}

void CollectionAppenderBase::preparePut(size_t size)
{
  if(m_wtxn->writeBuf().avail() < size) {
    m_collectionInfo->nextStartIndex += m_elementCount;
    m_wtxn->startChunk(m_collectionInfo, m_chunkSize, m_elementCount);
    m_elementCount = 0;
  }
  m_elementCount++;
}

void CollectionAppenderBase::close() {
  if(m_elementCount) m_wtxn->writeChunkHeader(m_collectionInfo->nextStartIndex, m_elementCount);
  m_chunkCursor->close();
  m_wtxn->putCollectionInfo(m_collectionInfo, m_elementCount);
}

} //kv

} //persistence
} //flexis

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
ClassInfo<EmptyClass> * ClassTraits<kv::EmptyClass>::info = new ClassInfo<EmptyClass>("empty", typeid(EmptyClass), 0);

inline bool streq(string s1, const char *s2) {
  if(s2 == nullptr) return s1.empty();
  return s1 == s2;
}

void KeyValueStoreBase::updateClassSchema(AbstractClassInfo *classInfo, PropertyAccessBase * properties[], unsigned numProperties)
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
            ss << "class " << classInfo->name << ": data type for property '" << pi->name << "' has changed";
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

void readObjectHeader(ReadBuf &buf, ClassId *classId, ObjectId *objectId, size_t *size, bool *deleted)
{
  ClassId cid = buf.readInteger<ClassId>(ClassId_sz);
  if(classId) *classId = cid;
  ObjectId oid = buf.readInteger<ObjectId>(ObjectId_sz);
  if(objectId) *objectId = oid;
  size_t sz = buf.readInteger<size_t>(4);
  if(size) *size = sz;
  byte_t del = buf.readInteger<byte_t>(1);
  if(deleted) *deleted = del;
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
}

void WriteTransaction::writeObjectHeader(ClassId classId, ObjectId objectId, size_t size)
{
  byte_t * hdr = writeBuf().allocate(ObjectHeader_sz);
  write_integer<ClassId>(hdr, classId, ClassId_sz);
  write_integer<ObjectId>(hdr+ClassId_sz, objectId, ObjectId_sz);
  write_integer<size_t>(hdr+ClassId_sz+ObjectId_sz, size, 4);
  write_integer<byte_t>(hdr+ClassId_sz+ObjectId_sz+4, 0, 1);
}

void WriteTransaction::commit()
{
  for(auto &it : m_collectionInfos) {
    CollectionInfo *ci = it.second;
    size_t sz = ObjectId_sz + sizeof(size_t) + ci->chunkInfos.size() * (PropertyId_sz + 3 * sizeof(size_t));
    writeBuf().start(sz);
    writeBuf().appendRaw(ci->collectionId);
    writeBuf().appendRaw(ci->chunkInfos.size());
    for(auto &ch : ci->chunkInfos) {
      writeBuf().appendRaw(ch.chunkId);
      writeBuf().appendRaw(ch.startIndex);
      writeBuf().appendRaw(ch.elementCount);
      writeBuf().appendRaw(ch.dataSize);
    }
    if(ci->sk.classId)
      putData(ci->sk.classId, ci->sk.objectId, ci->sk.propertyId, writeBuf());
    else
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

CollectionInfo *ReadTransaction::readCollectionInfo(ReadBuf &readBuf)
{
  CollectionInfo *info = new CollectionInfo();
  ClassId collectionId = readBuf.readRaw<ClassId>();
  size_t sz = readBuf.readRaw<size_t>();
  for(size_t i=0; i<sz; i++) {
    PropertyId chunkId = readBuf.readRaw<PropertyId>();
    size_t startIndex = readBuf.readRaw<size_t>();
    size_t elementCount = readBuf.readRaw<size_t>();
    size_t dataSize = readBuf.readRaw<size_t>();
    info->chunkInfos.push_back(ChunkInfo(chunkId, startIndex, elementCount, dataSize));
  }
  //put into transaction cache
  m_collectionInfos[collectionId] = info;

  info->init();
  return info;
}

CollectionInfo *ReadTransaction::getCollectionInfo(ClassId classId, ObjectId objectId, PropertyId propertyId)
{
  for(auto &ci : m_collectionInfos) {
    if(ci.second->sk.classId == classId && ci.second->sk.objectId == objectId && ci.second->sk.propertyId == propertyId) {
      return ci.second;
    }
  }
  ReadBuf readBuf;
  getData(readBuf, classId, objectId, propertyId);
  return readBuf.null() ? nullptr : readCollectionInfo(readBuf);
}

CollectionInfo *ReadTransaction::getCollectionInfo(ObjectId collectionId)
{
  if(m_collectionInfos.count(collectionId)) return m_collectionInfos[collectionId];
  else {
    ReadBuf readBuf;
    getData(readBuf, COLLINFO_CLSID, collectionId, 0);
    return readBuf.null() ? nullptr : readCollectionInfo(readBuf);
  }
}

void WriteTransaction::startChunk(CollectionInfo *collectionInfo, size_t chunkSize, size_t elementCount)
{
  byte_t * data = nullptr;
  chunkSize += ChunkHeader_sz;

  if(!allocData(COLLECTION_CLSID, collectionInfo->collectionId, collectionInfo->nextChunkId, chunkSize, &data))
    throw persistence_error("allocData failed");

  collectionInfo->chunkInfos.push_back(ChunkInfo(
      collectionInfo->nextChunkId, collectionInfo->nextStartIndex, elementCount, chunkSize));

  writeBuf().start(data, chunkSize);
  writeBuf().allocate(ChunkHeader_sz);
  writeChunkHeader(collectionInfo->nextStartIndex, elementCount);

  collectionInfo->nextStartIndex += elementCount;
  collectionInfo->nextChunkId++;
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
  do {
    if(++m_curElement == m_elementCount && m_chunkCursor->next()) {
      m_chunkCursor->get(m_readBuf);
      readChunkHeader(m_readBuf, 0, 0, &m_elementCount);
      m_curElement = 0;
    }
  } while(m_curElement < m_elementCount && !isValid());
  return m_curElement < m_elementCount;
}

CollectionAppenderBase::CollectionAppenderBase(WriteTransaction *wtxn, ObjectId collectionId, size_t chunkSize)
    : m_chunkSize(chunkSize), m_wtxn(wtxn), m_writeBuf(wtxn->writeBuf())
{
  m_collectionInfo = m_wtxn->getCollectionInfo(collectionId);
  m_elementCount = 0;
}

void CollectionAppenderBase::startChunk(size_t size)
{
  if(m_elementCount) {
    //write chunkinfo for current chunk
    ChunkInfo &ci = m_collectionInfo->chunkInfos.back();
    if(!ci.startIndex) ci.startIndex = m_collectionInfo->nextStartIndex;
    ci.dataSize = m_writeBuf.size();
    ci.elementCount = m_elementCount;

    m_wtxn->writeChunkHeader(ci.startIndex, ci.elementCount);
    m_collectionInfo->nextStartIndex += m_elementCount;
  }
  //allocate a new chunk
  byte_t * data = nullptr;
  size_t sz = size + ChunkHeader_sz < m_chunkSize ? m_chunkSize : size;
  if(m_wtxn->allocData(COLLECTION_CLSID, m_collectionInfo->collectionId, m_collectionInfo->nextChunkId, sz, &data)) {
    m_collectionInfo->chunkInfos.push_back(ChunkInfo(m_collectionInfo->nextChunkId));

    m_writeBuf.start(data, sz);
    m_writeBuf.allocate(ChunkHeader_sz); //reserve for writing later

    m_collectionInfo->nextChunkId++;
  }
  else throw persistence_error("allocData failed");

  m_elementCount = 0;
}

void CollectionAppenderBase::close()
{
  ChunkInfo &ci = m_collectionInfo->chunkInfos.back();
  if(!ci.startIndex) ci.startIndex = m_collectionInfo->nextStartIndex;
  ci.elementCount += m_elementCount;
  ci.dataSize = m_writeBuf.size();

  m_wtxn->writeChunkHeader(ci.startIndex, ci.elementCount);
}

} //kv

} //persistence
} //flexis

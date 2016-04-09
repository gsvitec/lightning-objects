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
using namespace kv;

const ObjectKey ObjectKey::NIL;

Properties * ClassTraits<kv::EmptyClass>::traits_properties {nullptr};
ClassInfo<EmptyClass> * ClassTraits<kv::EmptyClass>::traits_info = new ClassInfo<EmptyClass>("empty", typeid(EmptyClass));

string incompatible_schema_error::make_what(const char *className)
{
  stringstream ss;
  ss << "saved class schema for " << className << " is incompatible with application class mapping" << endl;
  return ss.str();
}
string incompatible_schema_error::make_detail(vector<Property> &errs)
{
  stringstream ss;
  ss << errs.size() << " property incompatibilities: ";
  for(auto it=errs.cbegin(); it!=errs.cend();) {
    ss << it->name << "[" << it->position << "]: " << it->description << " runtime: " << it->runtime << " saved: " << it->saved;
    if(++it != errs.cend()) ss << ", ";
    else break;
  }
  return ss.str();
}

inline bool streq(string s1, const char *s2) {
  if(s2 == nullptr) return s1.empty();
  return s1 == s2;
}

inline bool is_integer(unsigned typeId) {
  return typeId == TypeTraits<short>::id ||
      typeId == TypeTraits<unsigned short>::id ||
      typeId == TypeTraits<int>::id ||
      typeId == TypeTraits<unsigned int>::id ||
      typeId == TypeTraits<long>::id ||
      typeId == TypeTraits<unsigned long>::id ||
      typeId == TypeTraits<long long>::id ||
      typeId == TypeTraits<unsigned long long>::id;
}

bool KeyValueStoreBase::updateClassSchema(
    AbstractClassInfo *classInfo, const PropertyAccessBase ** properties[], unsigned numProperties,
    vector<incompatible_schema_error::Property> &errors)
{
  vector<PropertyMetaInfoPtr> propertyInfos;
  loadSaveClassMeta(id, classInfo, properties, numProperties, propertyInfos);

  bool hasSubclasses = !classInfo->subs.empty();

  auto layout_msg = [](StoreLayout l) -> const char * {
    switch(l) {
    case StoreLayout::all_embedded:
      return "all_embedded";
    case StoreLayout::embedded_key:
      return "embedded_key";
    default:
      return "not embedded";
    }
  };

  //if previous class schema found in db, check compatibility
  if(!propertyInfos.empty()) {
    //1. all available properties must still be in the same sequence and of the same type
    size_t index=0;

    for(size_t sz=propertyInfos.size(); index < sz && index < numProperties; index++) {
      PropertyMetaInfoPtr &pi = propertyInfos[index];

      const PropertyAccessBase * pa = *properties[index];

      incompatible_schema_error::Property err(pa->name, index);
      if(hasSubclasses && pa->storage->layout != pi->storeLayout &&
         ((pa->storage->layout == StoreLayout::all_embedded || pa->storage->layout == StoreLayout::embedded_key)
          || (pi->storeLayout == StoreLayout::all_embedded || pi->storeLayout != StoreLayout::embedded_key)))
      {
        err.description = "shallow storage";
        err.runtime = layout_msg(pa->storage->layout);
        err.saved = layout_msg(pi->storeLayout);
      }
      else if(pa->type.id != pi->typeId && (!is_integer(pa->type.id) || !is_integer(pi->typeId))) {
        err.description = "typeId";
        err.runtime = to_string(pa->type.id);
        err.saved = to_string(pi->typeId);
      }
      else if(pa->type.byteSize != pi->byteSize) {
        err.description = "byteSize";
        err.runtime = to_string(pa->type.byteSize);
        err.saved = to_string(pi->byteSize);
      }
      else if(!streq(pi->className, pa->type.className)) {
        err.description = "className";
        err.runtime = pa->type.className;
        err.saved = pi->className;
      }
      else if(pi->isVector != pa->type.isVector) {
        err.description = "isVector";
        err.runtime = to_string(pa->type.isVector);
        err.saved = to_string(pi->isVector);
      }
      if(!err.description.empty()) {
        classInfo->compatibility = SchemaCompatibility::none;
        errors.push_back(err);
      }
    }

    //2. we cannot cope with deleted shallow properties in non-leaf classes
    if(hasSubclasses) {
      for(unsigned i=numProperties; i<propertyInfos.size(); i++) {
        if(propertyInfos[i]->storeLayout == StoreLayout::all_embedded || propertyInfos[i]->storeLayout == StoreLayout::embedded_key){
          incompatible_schema_error::Property err(propertyInfos[i]->name, index);
          err.description = "deleted shallow storage property in non-leaf class";
          err.runtime = "--";
          err.saved = layout_msg(propertyInfos[i]->storeLayout);

          classInfo->compatibility = SchemaCompatibility::none;
          errors.push_back(err);
        }
      }
    }
    //3. properties that were added can safely be disabled (during read, that is)
    if(index < numProperties) {
      classInfo->compatibility = SchemaCompatibility::read;
      for(; index < numProperties; index++) {
        const_cast<PropertyAccessBase *>(*properties[index])->enabled = false;
      }
    }
    return true;
  }
  return false;
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
    putData(COLLINFO_CLSID, ci->collectionId, 0, writeBuf());

    delete ci;
  }
  m_collectionInfos.clear();
  doCommit();
}

ReadTransaction::~ReadTransaction()
{
  for(auto &it : m_collectionInfos) delete it.second;
  m_collectionInfos.clear();
}

void ReadTransaction::abort()
{
  for(auto &it : m_collectionInfos) delete it.second;
  m_collectionInfos.clear();
  doAbort();
}

void ReadTransaction::reset()
{
  //keep collection infos alive
  doReset();
}

void ReadTransaction::renew()
{
  doRenew();
}

CollectionInfo *ReadTransaction::readCollectionInfo(ReadBuf &readBuf)
{
  CollectionInfo *info = new CollectionInfo();
  info->collectionId = readBuf.readRaw<ObjectId>();
  size_t sz = readBuf.readRaw<size_t>();
  for(size_t i=0; i<sz; i++) {
    PropertyId chunkId = readBuf.readRaw<PropertyId>();
    if(chunkId >= info->nextChunkId)
      info->nextChunkId = chunkId + PropertyId(1);

    size_t startIndex = readBuf.readRaw<size_t>();
    size_t elementCount = readBuf.readRaw<size_t>();
    if(startIndex + elementCount > info->nextStartIndex)
      info->nextStartIndex = startIndex + elementCount;

    size_t dataSize = readBuf.readRaw<size_t>();
    info->chunkInfos.push_back(ChunkInfo(chunkId, startIndex, elementCount, dataSize));
  }
  //put into transaction cache
  m_collectionInfos[info->collectionId] = info;

  return info;
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

void ObjectBuf::checkData(ReadTransaction *tr, ClassId cid, ObjectId oid) {
  if(!dataChecked) {
    dataChecked = true;
    tr->getData(readBuf, cid, oid, 0);
    if(makeCopy) readBuf.copyData();
    if(markOffs) readBuf.unmark(markOffs);
  }
}

void LazyBuf::checkData() {
  ObjectBuf::checkData(m_txn, key.classId, key.objectId);
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
  : m_collectionId(collectionId), m_tr(tr), m_chunkCursor(chunkCursor), m_storeId(tr->store.id)
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
    : m_chunkSize(chunkSize), m_tr(wtxn), m_writeBuf(wtxn->writeBuf())
{
  m_collectionInfo = m_tr->getCollectionInfo(collectionId);
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

    m_tr->writeChunkHeader(ci.startIndex, ci.elementCount);
    m_collectionInfo->nextStartIndex += m_elementCount;
  }
  //allocate a new chunk
  byte_t * data = nullptr;
  size_t sz = size + ChunkHeader_sz < m_chunkSize ? m_chunkSize : size;
  if(m_tr->allocData(COLLECTION_CLSID, m_collectionInfo->collectionId, m_collectionInfo->nextChunkId, sz, &data)) {
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

  m_tr->writeChunkHeader(ci.startIndex, ci.elementCount);
}

} //kv
} //persistence
} //flexis

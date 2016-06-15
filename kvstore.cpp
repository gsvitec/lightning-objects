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
#include <map>
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

schema_compatibility::error schema_compatibility::make_error()
{
  stringstream msg;
  msg << "saved class schema is incompatible with application. " << classProperties.size() <<  " class affected";

  auto sc = make_shared<schema_compatibility>(*this);
  return error(msg.str(), sc);
}

void write_prop(const schema_compatibility::Property &prop, const char *prefix, ostream &os)
{
  switch(prop.what) {
    case schema_compatibility::embedded_property_appended:
      os << prefix << "property '" << prop.name << "' appended to shallow buffer" << endl;
      break;
    case schema_compatibility::embedded_property_inserted:
      os << prefix << "property '" << prop.name << "' insertied into shallow buffer" << endl;
      break;
    case schema_compatibility::embedded_property_removed_end:
      os << prefix << "property '" << prop.name << "' removed from end of shallow buffer" << endl;
      break;
    case schema_compatibility::embedded_property_removed_internal:
      os << prefix << "property '" << prop.name << "' removed from middle of shallow buffer" << endl;
      break;
    case schema_compatibility::keyed_property_added:
      os << prefix << "property '" << prop.name << "' added to keyed storage" << endl;
      break;
    case schema_compatibility::keyed_property_removed:
      os << prefix << "property '" << prop.name << "' removed from keyed storage" << endl;
      break;
    case schema_compatibility::property_modified:
      os << prefix << "property '" << prop.name << "': " << prop.description << " modified. Schema: " << prop.saved << " runtime: " << prop.runtime << endl;
      break;
  }
}

void schema_compatibility::error::printDetails(ostream &os)
{
  for(auto &cls : compatibility->classProperties) {
    os << "compatibility issues for " << cls.first << endl;
    for(auto &prop : cls.second) {
      write_prop(prop, "  ", os);
    }
  }
}
unordered_map<string, vector<string>> schema_compatibility::error::getDetails()
{
  unordered_map<string, vector<string>> details;
  for(auto &cls : compatibility->classProperties) {
    vector<string> errs;
    for(auto &prop : cls.second) {
      stringstream ss;
      write_prop(prop, "", ss);
      errs.push_back(ss.str());
    }
    details[cls.first] = errs;
  }
  return details;
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

inline bool is_embedded(StoreLayout layout) {
  return layout == StoreLayout::embedded_key || layout == StoreLayout::all_embedded;
}

void KeyValueStoreBase::compare(vector<schema_compatibility::Property> &errors, unsigned index,
             KeyValueStoreBase::PropertyMetaInfoPtr pi, const PropertyAccessBase *pa)
{
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
  schema_compatibility::Property err(pa->name, index, schema_compatibility::property_modified);
  if(pa->storage->layout != pi->storeLayout &&
     ((pa->storage->layout == StoreLayout::all_embedded || pa->storage->layout == StoreLayout::embedded_key)
      || (pi->storeLayout == StoreLayout::all_embedded || pi->storeLayout != StoreLayout::embedded_key)))
  {
    err.description = "storage layout";
    err.runtime = layout_msg(pa->storage->layout);
    err.saved = layout_msg(pi->storeLayout);
  }
  else if(pa->type.id != pi->typeId && (!is_integer(pa->type.id) || !is_integer(pi->typeId))) {
    err.description = "type";
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
    errors.push_back(err);
  }
}

bool KeyValueStoreBase::updateClassSchema(
    AbstractClassInfo *classInfo, const PropertyAccessBase ** properties[], unsigned numProperties,
    vector<schema_compatibility::Property> &errors)
{
  vector<PropertyMetaInfoPtr> propertyInfos;
  loadSaveClassMeta(id, classInfo, properties, numProperties, propertyInfos);

  //no previous schema in db
  if(propertyInfos.empty()) return false;

  //schema found. Check compatibility
  vector<PropertyMetaInfoPtr> schemaEmbedded;
  map<string, PropertyMetaInfoPtr> schemaKeyed;
  for(auto pmi : propertyInfos) {
    if(is_embedded(pmi->storeLayout))
      schemaEmbedded.push_back(pmi);
    else
      schemaKeyed[pmi->name] = pmi;
  }
  vector<const PropertyAccessBase *> runtimeEmbedded;
  map<string, const PropertyAccessBase *> runtimeKeyed;
  for(int i=0; i<numProperties; i++) {
    const PropertyAccessBase *pa = *properties[i];
    if(is_embedded(pa->storage->layout))
      runtimeEmbedded.push_back(pa);
    else
      runtimeKeyed[pa->name] = pa;
  }

  unsigned rIndex=0;
  for(unsigned s=0, r=0; s<schemaEmbedded.size(); s++, r++)
  {
    unsigned sIndex=s;
    for(; sIndex < schemaEmbedded.size() && r < runtimeEmbedded.size()
        && schemaEmbedded[sIndex]->name != runtimeEmbedded[r]->name; sIndex++) ;
    for(; sIndex < schemaEmbedded.size() && (r==runtimeEmbedded.size() || s<sIndex); sIndex++) {
      schema_compatibility::What w = r==runtimeEmbedded.size() ?
                                      schema_compatibility::embedded_property_removed_end :
                                      schema_compatibility::embedded_property_removed_internal;
      errors.push_back(schema_compatibility::Property(schemaEmbedded[s]->name, schemaEmbedded[s]->id, w));
    }

    rIndex=r;
    for(; rIndex < runtimeEmbedded.size() && s < schemaEmbedded.size()
        && runtimeEmbedded[rIndex]->name != schemaEmbedded[s]->name; rIndex++) ;
    for(; rIndex < runtimeEmbedded.size() && (s==schemaEmbedded.size() || r<rIndex); rIndex++) {
      schema_compatibility::What w = s==schemaEmbedded.size() ?
                                    schema_compatibility::embedded_property_appended :
                                    schema_compatibility::embedded_property_inserted;
      errors.push_back(schema_compatibility::Property(runtimeEmbedded[r]->name, runtimeEmbedded[r]->id, w));
      const_cast<PropertyAccessBase *>(runtimeEmbedded[r])->enabled = false;
    }

    if(sIndex < schemaEmbedded.size() && rIndex < runtimeEmbedded.size())
      compare(errors, s, schemaEmbedded[sIndex], runtimeEmbedded[rIndex]);
  }
  for(unsigned j=rIndex+1; j<runtimeEmbedded.size(); j++) {
    errors.push_back(schema_compatibility::Property(
        runtimeEmbedded[j]->name, runtimeEmbedded[j]->id, schema_compatibility::embedded_property_appended));
  }

  for(auto &schema : schemaKeyed) {
    if(runtimeKeyed.count(schema.first))
      compare(errors, schema.second->id, schema.second, runtimeKeyed[schema.first]);
    else {
      errors.push_back(schema_compatibility::Property(
                         schema.second->name, schema.second->id, schema_compatibility::keyed_property_removed));
    }
  }
  for(auto &runtime : runtimeKeyed) {
    if(!schemaKeyed.count(runtime.first)) {
      errors.push_back(schema_compatibility::Property(
                         runtime.second->name, runtime.second->id, schema_compatibility::keyed_property_added));
    }
  }

  classInfo->compatibility = SchemaCompatibility::write;

  bool hasSubclasses = !classInfo->subs.empty();
  for(auto &err : errors) {
    switch(err.what) {
      case schema_compatibility::property_modified:
        classInfo->compatibility = SchemaCompatibility::none;
        break;
      case schema_compatibility::keyed_property_removed:
        classInfo->compatibility = SchemaCompatibility::read;
        break;
      case schema_compatibility::keyed_property_added:
        classInfo->compatibility = SchemaCompatibility::write;
        break;
      case schema_compatibility::embedded_property_appended:
        classInfo->compatibility = hasSubclasses ? SchemaCompatibility::none : SchemaCompatibility::read;
        break;
      case schema_compatibility::embedded_property_inserted:
        classInfo->compatibility = SchemaCompatibility::none;
        break;
      case schema_compatibility::embedded_property_removed_internal:
        classInfo->compatibility = SchemaCompatibility::none;
        break;
      case schema_compatibility::embedded_property_removed_end:
        classInfo->compatibility = SchemaCompatibility::read;
        break;
    }
  }
  return true;
}

namespace kv {

static StoreId storeId = 0;
StoreId nextStoreId() {
  if(storeId == MAX_DATABASES)
    throw error("maximum number of databases reached");

  StoreId ret = storeId;
  storeId++;
  return ret;
}

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

WriteTransaction::~WriteTransaction()
{
  //assume all was popped
  curBuf->deleteChain();
}

void WriteTransaction::abort()
{
  _abort();
}

void WriteTransaction::writeCollections()
{
  for(auto &it : m_collectionInfos) {
    CollectionInfo *ci = it.second;
    for(auto app : ci->appenders) app->close(false);
    ci->appenders.clear();

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
}

void WriteTransaction::commit()
{
  writeCollections();
  doCommit();
}

Transaction::~Transaction()
{
  for(auto &it : m_collectionInfos) delete it.second;
  m_collectionInfos.clear();
}

void Transaction::_abort()
{
  for(auto &it : m_collectionInfos) delete it.second;
  m_collectionInfos.clear();
  doAbort();
}

void ReadTransaction::end()
{
  _abort();
}

void Transaction::reset()
{
  //keep collection infos alive
  doReset();
}

void Transaction::renew()
{
  doRenew();
}

CollectionInfo *Transaction::readCollectionInfo(ReadBuf &readBuf)
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

CollectionInfo *Transaction::getCollectionInfo(ObjectId &collectionId, bool create)
{
  if(collectionId == 0) {
    if(!create) return nullptr;

    CollectionInfo *ci = new CollectionInfo(++store.m_maxCollectionId);
    m_collectionInfos[ci->collectionId] = ci;
    collectionId = ci->collectionId;
    return ci;
  }
  else if(m_collectionInfos.count(collectionId))
    return m_collectionInfos[collectionId];
  else {
    ReadBuf readBuf;
    getData(readBuf, COLLINFO_CLSID, collectionId, 0);
    return readBuf.null() ? nullptr : readCollectionInfo(readBuf);
  }
}

void ObjectBuf::checkData(Transaction *tr, ClassId cid, ObjectId oid) {
  if(!dataChecked) {
    dataChecked = true;
    tr->getData(readBuf, cid, oid, 0);
    if(makeCopy) readBuf.copyData();
    if(markOffs) readBuf.unmark(markOffs);
  }
}

void WriteTransaction::deleteCollection(ObjectId collectionId)
{
  CollectionInfo *ci = getCollectionInfo(collectionId, false);
  if(ci) {
    m_collectionInfos.erase(collectionId);
    if(!remove(COLLINFO_CLSID, collectionId, 0))
      throw error("error deleting collection info");
    for(auto chunk : ci->chunkInfos)
      if(!remove(COLLECTION_CLSID, collectionId, chunk.chunkId))
        throw error("error deleting collection chunk");
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
    throw error("allocData failed");

  collectionInfo->chunkInfos.push_back(ChunkInfo(
      collectionInfo->nextChunkId, collectionInfo->nextStartIndex, elementCount, chunkSize));

  writeBuf().start(data, chunkSize);
  writeBuf().allocate(ChunkHeader_sz);
  writeChunkHeader(collectionInfo->nextStartIndex, elementCount);

  collectionInfo->nextStartIndex += elementCount;
  collectionInfo->nextChunkId++;
}

CollectionCursorBase::CollectionCursorBase(ObjectId collectionId, Transaction *tr, ChunkCursor::Ptr chunkCursor)
  : m_collectionInfo(tr->getCollectionInfo(collectionId)), m_tr(tr), m_chunkCursor(chunkCursor), m_storeId(tr->store.id)
{
  if(!m_chunkCursor->atEnd()) {
    m_chunkCursor->get(m_readBuf);
    readChunkHeader(m_readBuf, 0, 0, &m_elementCount);
  }
}

bool CollectionCursorBase::atEnd() {
  return m_curElement >= m_elementCount && m_chunkCursor->atEnd();
}

bool CollectionCursorBase::next()
{
  do {
    if(++m_curElement > m_elementCount) {
      m_curElement = m_elementCount = 0;
      if(m_chunkCursor->next(&m_chunkId)) {
        m_curElement = 1;
        m_chunkCursor->get(m_readBuf);
        readChunkHeader(m_readBuf, &m_dataSize, &m_startIndex, &m_elementCount);
      }
    }
  } while(m_curElement && !isValid());
  return (bool)m_curElement;
}

bool CollectionCursorBase::seek(size_t position)
{
  if(position == m_startIndex+m_curElement) return true;

  if(m_startIndex <= position && m_startIndex + m_elementCount > position) {
    bufSeek(position - m_startIndex);
    return true;
  }
  for(auto &ci : m_collectionInfo->chunkInfos) {
    if(ci.startIndex <= position && ci.startIndex + ci.elementCount > position) {
      m_chunkCursor->seek(ci.chunkId);
      m_chunkCursor->get(m_readBuf);
      m_curElement = m_elementCount = 0;
      readChunkHeader(m_readBuf, 0, &m_startIndex, &m_elementCount);
      bufSeek(position - m_startIndex);
      return true;
    }
  }
  return false;
}

size_t CollectionCursorBase::count()
{
  return m_collectionInfo->count();
}

void CollectionCursorBase::objectBufSeek(size_t position)
{
  if(position > m_curElement) {
    for(size_t pos=0, epos=position-m_curElement; pos < epos; ) {
      size_t sz;
      bool deleted;
      readObjectHeader(m_readBuf, 0, 0, &sz, &deleted);
      m_readBuf.read(sz-ObjectHeader_sz);
      if(!deleted) pos++;
    }
  }
  else {
    m_readBuf.reset();
    readChunkHeader(m_readBuf, 0, 0, 0);
    for(size_t pos=0; pos < position; ) {
      size_t sz;
      bool deleted;
      readObjectHeader(m_readBuf, 0, 0, &sz, &deleted);
      m_readBuf.read(sz-ObjectHeader_sz);
      if(!deleted) pos++;
    }
  }
  m_curElement = position;
}

CollectionAppenderBase::CollectionAppenderBase(WriteTransaction *wtxn, ObjectId &collectionId, size_t chunkSize)
    : m_chunkSize(chunkSize ? chunkSize : wtxn->store.getOptimalChunkSize()),
      m_tr(wtxn), m_writeBuf(wtxn->writeBuf()), m_collectionId(collectionId)
{
  m_elementCount = 0;
}

CollectionInfo * CollectionAppenderBase::collectionInfo()
{
  if(!m_collectionInfo) {
    m_collectionInfo = m_tr->getCollectionInfo(m_collectionId);
    m_collectionInfo->appenders.insert(this);
  }
  return m_collectionInfo;
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
  else throw error("allocData failed");

  m_elementCount = 0;
}

void CollectionAppenderBase::close(bool erase)
{
  if(m_collectionInfo && !m_collectionInfo->chunkInfos.empty()) {
    ChunkInfo &ci = m_collectionInfo->chunkInfos.back();
    if(!ci.startIndex) ci.startIndex = m_collectionInfo->nextStartIndex;
    ci.elementCount += m_elementCount;
    ci.dataSize = m_writeBuf.size();

    if(erase) m_collectionInfo->appenders.erase(this);

    m_tr->writeChunkHeader(ci.startIndex, ci.elementCount);
  }
}

} //kv
} //persistence
} //flexis

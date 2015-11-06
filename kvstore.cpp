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

CollectionCursorBase::CollectionCursorBase(ObjectId collectionId, ReadTransaction *tr, ChunkCursor::Ptr chunkCursor)
: m_collectionId(collectionId), m_tr(tr), m_chunkCursor(chunkCursor)
{
  if(!m_chunkCursor->atEnd()) {
    m_chunkCursor->get(m_readBuf);
    m_elementCount = m_readBuf.readInteger<size_t>(4);
  }
}

bool CollectionCursorBase::atEnd() {
  return m_curElement >= m_elementCount;
}

bool CollectionCursorBase::next()
{
  if(++m_curElement == m_elementCount && m_chunkCursor->next()) {
    m_chunkCursor->get(m_readBuf);
    m_elementCount = m_readBuf.readInteger<size_t>(4);
    m_curElement = 0;
  }
  return m_curElement < m_elementCount;
}

CollectionAppenderBase::CollectionAppenderBase(ChunkCursor::Ptr chunkCursor, WriteTransaction *wtxn,
                                               ObjectId collectionId, size_t chunkSize)
    : m_chunkCursor(chunkCursor), m_chunkSize(chunkSize), m_wtxn(wtxn), m_collectionId(collectionId), m_writeBuf(m_wtxn->writeBuf())
{
  m_writeBuf.start(1024 * 1024);
  m_writeBuf.allocate(4);
  m_writePending = false;

  if(!m_chunkCursor->atEnd()) {
    ReadBuf rb;
    m_chunkCursor->get(rb);

    if(rb.size() < m_chunkSize) {
      m_elementCount = rb.readInteger<size_t>(4);
      m_writeBuf.append(rb.cur(), rb.size()-4);
      m_chunkId = m_chunkCursor->chunkId();
    }
    else
      m_chunkId = m_chunkCursor->chunkId()+PropertyId(1);
  }
  else m_chunkId = m_wtxn->getMaxPropertyId(COLLECTION_CLSID, collectionId);
}

void CollectionAppenderBase::finalizePut()
{
  m_elementCount++;

  if(m_writeBuf.size() >= m_chunkSize) {
    write_integer(m_writeBuf.data(), m_elementCount, 4);
    m_wtxn->putData(COLLECTION_CLSID, m_collectionId, m_chunkId, m_writeBuf);

    m_writeBuf.reset();
    m_writeBuf.allocate(4);
    m_chunkId++;
    m_elementCount = 0;
    m_writePending = false;
  }
  else m_writePending = true;
}

void CollectionAppenderBase::close() {
  if(m_writePending) {
    write_integer(m_writeBuf.data(), m_elementCount, 4); //chunk header: size
    m_wtxn->putData(COLLECTION_CLSID, m_collectionId, m_chunkId, m_writeBuf);
  }
  m_chunkCursor->close();
}

}

} //persistence
} //flexis

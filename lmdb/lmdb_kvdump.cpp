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

#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <map>
#include <kvstore.h>
#include <functional>
#include <algorithm>
#include "lmdb_kvstore.h"
#include "liblmdb/lmdb++.h"

#ifdef _WIN32
#include <Windows.h>
static const char separator_char = '\\';
#else
static const char separator_char = '/';
#endif

static const char * CLASSDATA = "classdata";
static const char * CLASSMETA = "classmeta";

namespace lo {
namespace persistence {
namespace lmdb {
int meta_dup_compare(const MDB_val *a, const MDB_val *b);
int key_compare(const MDB_val *a, const MDB_val *b);
}}}

using namespace lo::persistence::kv;

static const unsigned ObjectId_off = lo::persistence::kv::ClassId_sz;
static const unsigned PropertyId_off = lo::persistence::kv::ClassId_sz + lo::persistence::kv::ObjectId_sz;

#define SK_CONSTR(nm, c, o, p) byte_t nm[StorageKey::byteSize]; *(ClassId *)nm = c; *(ObjectId *)(nm+ObjectId_off) = o; \
*(PropertyId *)(nm+PropertyId_off) = p

#define SK_CLASSID(k) *(ClassId *)k
#define SK_OBJID(k) *(ObjectId *)(k+ObjectId_off)
#define SK_PROPID(k) *(PropertyId *)(k+PropertyId_off)

using namespace std;
using namespace lo::persistence::lmdb;

namespace lo {
namespace persistence {
namespace kvdump {

struct PropertyInfo {
  std::string name;
  PropertyId id;
  unsigned typeId;
  bool isVector;
  unsigned byteSize;
  std::string className;
  StoreLayout storeLayout;
};

struct ClassInfo
{
  string name;
  vector<string> subclasses;

  ClassId classId;

  size_t num_objects = 0;
  size_t max_object_size = 0;
  size_t sum_objects_size = 0;
  map<uint16_t, uint16_t> refcounts;

  vector<PropertyInfo> propertyInfos;

  ClassInfo(string name) : name(name) {}
};

void make_propertyinfo(MDB_val *mdbVal, PropertyInfo &pi)
{
  byte_t *readPtr = (byte_t *)mdbVal->mv_data;

  pi.id = read_integer<PropertyId>(readPtr, 2);
  readPtr += 2;
  pi.name = (const char *)readPtr;
  readPtr += pi.name.length() + 1;
  pi.typeId = read_integer<unsigned>(readPtr, 2);
  readPtr += 2;
  pi.isVector = read_integer<char>(readPtr, 1) != 0;
  readPtr += 1;
  pi.byteSize = read_integer<unsigned>(readPtr, 2);
  readPtr += 2;
  pi.storeLayout = static_cast<StoreLayout>(read_integer<unsigned>(readPtr, 2));
  readPtr += 2;
  if(readPtr - (byte_t *)mdbVal->mv_data < mdbVal->mv_size)
    pi.className = (const char *)readPtr;
}

struct TypeInfo {
  string name;
  ClassId typeId;
  PropertyId chunkId;
};

struct DatabaseInfo
{
  string m_dbpath;

  ::lmdb::env m_env;
  ::lmdb::dbi m_dbi_meta = 0;
  ::lmdb::dbi m_dbi_data = 0;

  bool useLockFile = false;

  vector<ClassInfo> classInfos;
  vector<TypeInfo> typeInfos;
  vector<kv::CollectionInfo *> collectionInfos;

  DatabaseInfo(string location, string name, size_t mapsize) : m_env(::lmdb::env::create())
  {
    m_dbpath = location;
    if(m_dbpath.back() != separator_char) m_dbpath += separator_char;
    m_dbpath += (name.empty() ? "kvdata" : name);

    m_env.set_mapsize(mapsize);

    //classmeta + classdata db
    m_env.set_max_dbs(2);

    unsigned flags = MDB_NOSUBDIR;
    if(!useLockFile) flags |= MDB_NOLOCK;
#ifndef _WIN32
    flags |= MDB_RDONLY; //heaven knows why this doesn't work on windows
#endif
    m_env.open(m_dbpath.c_str(), flags, 0664);

    MDB_stat envstat;
    mdb_env_stat(m_env, &envstat);

    auto txn = ::lmdb::txn::begin(m_env, nullptr, MDB_RDONLY);

    //open/create the classmeta database
    m_dbi_meta = ::lmdb::dbi::open(txn, CLASSMETA, MDB_DUPSORT);
    m_dbi_meta.set_dupsort(txn, lmdb::meta_dup_compare);

    //open/create the classdata database
    m_dbi_data = ::lmdb::dbi::open(txn, CLASSDATA);
    m_dbi_data.set_compare(txn, lmdb::key_compare);

    txn.commit();
  }

  ~DatabaseInfo() {
    m_env.close();
  }

  void loadClassMeta()
  {
    auto txn = ::lmdb::txn::begin(m_env, nullptr, MDB_RDONLY);

    ::lmdb::val key, val;
    auto cursor = ::lmdb::cursor::open(txn, m_dbi_meta);
    while(cursor.get(key, val, MDB_NEXT_NODUP)) {
      bool first = true;
      ::lmdb::val dupkey;

      string cname(key.data(), key.size());
      if(cname == "schema_compatibility::ValuetypeInfo") continue;

      classInfos.push_back(ClassInfo(cname));
      ClassInfo &ci = classInfos.back();
      for (bool read = cursor.get(dupkey, val, MDB_FIRST_DUP); read; read = cursor.get(dupkey, val, MDB_NEXT_DUP)) {
        if(first) {
          //first record is [propertyId == 0, classId]
          ReadBuf buf(val.data<byte_t>(), val.size());
          buf.read(PropertyId_sz);
          ci.classId = buf.readInteger<ClassId>(ClassId_sz);
          while(!buf.atEnd()) ci.subclasses.push_back(buf.readCString());
          first = false;
        }
        else {//rest is properties
          ci.propertyInfos.push_back(PropertyInfo());
          make_propertyinfo((MDB_val *) val, ci.propertyInfos.back());
        }
      }
    }
    cursor.close();
    txn.abort();
  }

  void loadClassData(ClassInfo &ci)
  {
    auto txn = ::lmdb::txn::begin(m_env, nullptr, MDB_RDONLY);

    ::lmdb::val key;
    auto cursor = ::lmdb::cursor::open(txn, m_dbi_data);

    SK_CONSTR(sk, ci.classId, 0, 0);
    key.assign(sk, sizeof(sk));

    if(cursor.get(key, MDB_SET_RANGE) && SK_CLASSID(key.data<byte_t>()) == ci.classId) {
      ci.num_objects++;

      ::lmdb::val val;
      cursor.get(key, val, MDB_GET_CURRENT);
      ci.sum_objects_size += val.size();

      while(cursor.get(key, MDB_NEXT) && SK_CLASSID(key.data<byte_t>()) == ci.classId) {
        if(SK_PROPID(key.data<byte_t>()) == 0) ci.num_objects++;

        cursor.get(key, val, MDB_GET_CURRENT);

        if(SK_PROPID(key.data<byte_t>()) == 1) {
          uint16_t refcount = *(uint16_t *)val.data();
          if(refcount > 0) {
            if(ci.refcounts.count(refcount))
              ci.refcounts[refcount]++;
            else
              ci.refcounts[refcount] = 1;
          }
        }
        else ci.sum_objects_size += val.size();
      }
    }
    cursor.close();
    txn.abort();
  }

  void loadCollectionInfos()
  {
    auto txn = ::lmdb::txn::begin(m_env, nullptr, MDB_RDONLY);

    ::lmdb::val key;
    auto cursor = ::lmdb::cursor::open(txn, m_dbi_data);

    SK_CONSTR(sk, COLLINFO_CLSID, 1, 0);
    key.assign(sk, sizeof(sk));

    while(cursor.get(key, MDB_SET_RANGE) && SK_CLASSID(key.data<byte_t>()) == COLLINFO_CLSID) {
      ::lmdb::val val;
      cursor.get(key, val, MDB_GET_CURRENT);

      ReadBuf readBuf((byte_t *)val.data(), val.size());
      CollectionInfo *info = new CollectionInfo();
      collectionInfos.push_back(info);

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

      SK_CONSTR(sk, COLLINFO_CLSID, info->collectionId+1, 0);
      key.assign(sk, sizeof(sk));
    }
    cursor.close();
    txn.abort();
  }

  void loadValueTypeInfos()
  {
    auto txn = ::lmdb::txn::begin(m_env, nullptr, MDB_RDONLY);

    ::lmdb::val key, val;
    key.assign("schema_compatibility::ValuetypeInfo");

    auto cursor = ::lmdb::cursor::open(txn, m_dbi_meta);
    if(cursor.get(key, val, MDB_SET)) {
      do {
        ReadBuf buf;
        buf.start(val.data<byte_t>(), val.size());

        PropertyId chunkId = buf.readInteger<PropertyId>(PropertyId_sz);
        buf.read(ClassId_sz);

        size_t count = buf.readRaw<size_t>();
        for(size_t i=0; i < count; i++) {
          TypeInfo ti;

          ti.name = buf.readCString();
          ti.typeId = buf.readRaw<ClassId>();
          ti.chunkId = chunkId;

          typeInfos.push_back(ti);
        }
      } while(cursor.get(key, val, MDB_NEXT_DUP));
    }
    cursor.close();
    txn.abort();
  }
};

}
}
}

using namespace lo::persistence;
using namespace lo::persistence::kvdump;

std::ifstream::pos_type filesize(const char* filename)
{
  std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
  return in.tellg();
}

void dumpClassesMeta(DatabaseInfo &dbinfo, string opt);
void dumpClassMeta(DatabaseInfo &dbinfo, ClassId classId);
void dumpClassObjects(DatabaseInfo &dbinfo, ClassId classId);
void dumpCollectionInfo(DatabaseInfo &dbinfo, ObjectId collId);
void dumpCollectionInfos(DatabaseInfo &dbinfo);
void dumpValueTypeInfos(DatabaseInfo &dbinfo);

int main(int argc, char* argv[])
{
  string opt = argc > 3 ? argv[3] : "";
  bool found = opt.empty();
  for(auto o : {"c", "n", "m", "o", "ci", "ti"}) {
    if(opt == o) {
      found = true;
      break;
    }
  }
  if(!found || (opt == "o" || opt == "m") && argc < 5 || argc < 3) {
    cout << "usage: lo_dump <path> <name> [c|n|m <classId>|o <classId>] | [ci|ci <collectionId>] | ti" << endl;
    cout << "c: sort by instance count" << endl;
    cout << "n: sort by class name" << endl;
    cout << "m: dump metadata for class <classId>" << endl;
    cout << "o: dump object simple data for class <classId>" << endl;
    cout << "ci: dump collection infos. If collectionId is given, dump chunk infos for that collections" << endl;
    cout << "ti: dump value type infos" << endl;
    return -1;
  }

  string path(argv[1]);
  string name(argv[2]);

  try {
    string fullpath = path + "/" + name;
    size_t freeSpace = (size_t)filesize(fullpath.c_str());

    DatabaseInfo dbinfo(path, name, freeSpace);
    dbinfo.loadClassMeta();

    if(opt == "m") {
      ClassId classId = (ClassId)atoi(argv[4]);
      dumpClassMeta(dbinfo, classId);
    }
    else if(opt == "o") {
      ClassId classId = (ClassId)atoi(argv[4]);
      dumpClassObjects(dbinfo, classId);
    }
    else if(opt == "ci") {
      dbinfo.loadCollectionInfos();
      if(argc > 4) {
        ObjectId collId = (ObjectId)atoi(argv[4]);
        dumpCollectionInfo(dbinfo, collId);
      }
      else  dumpCollectionInfos(dbinfo);
    }
    else if(opt == "ti") {
      dbinfo.loadValueTypeInfos();
      dumpValueTypeInfos(dbinfo);
    }
    else {
      dumpClassesMeta(dbinfo, opt);
    }
  }
  catch(::lmdb::runtime_error e) {
    cout <<"database error " << e.what() << endl;
  }
}

void dumpClassesMeta(DatabaseInfo &dbinfo, string opt)
{
  size_t len = 0;
  for(auto &ci : dbinfo.classInfos) {
    if(len < ci.name.length()) len = ci.name.length();
    dbinfo.loadClassData(ci);
  }

  std::function<bool(lo::persistence::kvdump::ClassInfo, lo::persistence::kvdump::ClassInfo)> sortByName =
      [](lo::persistence::kvdump::ClassInfo ci1, lo::persistence::kvdump::ClassInfo ci2) ->bool {
        return ci1.name > ci2.name;
      };
  std::function<bool(lo::persistence::kvdump::ClassInfo, lo::persistence::kvdump::ClassInfo)> sortByCount =
      [](lo::persistence::kvdump::ClassInfo ci1, lo::persistence::kvdump::ClassInfo ci2) ->bool {
        return ci1.num_objects > ci2.num_objects;
      };

  if(opt == "n") sort(dbinfo.classInfos.begin(), dbinfo.classInfos.end(), sortByName);
  else if(opt == "c") sort(dbinfo.classInfos.begin(), dbinfo.classInfos.end(), sortByCount);

  for(auto &ci : dbinfo.classInfos) {
    cout << setw(len) << std::resetiosflags(std::ios::adjustfield) << setiosflags(std::ios::left) << ci.name <<
        " (" << ci.classId << ")" << "  count: " << setw(7) << ci.num_objects << "  bytes: " << setw(10) <<
        setiosflags(std::ios::left) << ci.sum_objects_size;

    if(!ci.refcounts.empty()) cout << "rcnt: ";
    for(auto &r : ci.refcounts) {
      cout << r.first << "(" << r.second << ")";
    }
    cout << endl;
  }
}

void dumpCollectionInfos(DatabaseInfo &dbinfo)
{
  for(auto ci : dbinfo.collectionInfos) {
    stringstream ss; ss << "(" << ci->collectionId << ")";
    cout << "collection " << setw(8) << ss.str();
    cout << setw(6) << ci->chunkInfos.size() << setw(12) << " chunks";
    cout << setw(8) << ci->count() << " elements" << endl;
  }
}

void dumpValueTypeInfos(DatabaseInfo &dbinfo)
{
  size_t maxSize = 0;
  for(auto &ti : dbinfo.typeInfos)
    if(ti.name.length() > maxSize) maxSize = ti.name.length();

  for(auto &ti : dbinfo.typeInfos) {
    cout << setw(maxSize+5) << std::resetiosflags(std::ios::adjustfield) << setiosflags(std::ios::left) << ti.name;
    cout << setw(7) << "TypeId: " << setw(8) << ti.typeId << setw(7) << "ChunkId: " << setw(8) << ti.chunkId << endl;
  }
}

void dumpCollectionInfo(DatabaseInfo &dbinfo, ObjectId collId)
{
  CollectionInfo *ci = nullptr;
  for(auto i : dbinfo.collectionInfos) {
    if(i->collectionId == collId) {
      ci = i;
      break;
    }
  }
  if(!ci) {
    cout << "collection " << collId << "not found" << endl;
    return;
  }

  cout << std::resetiosflags(std::ios::adjustfield) << setiosflags(std::ios::left);

  stringstream ss; ss << "(" << ci->collectionId << ")";
  cout << "collection " << setw(8) << ss.str();
  cout << " chunks: " << setw(6) << ci->chunkInfos.size();
  cout << " elements: " << setw(8) << ci->count() << endl;

  for(auto &chunk : ci->chunkInfos) {
    stringstream ss; ss << "(" << chunk.chunkId << ")";
    cout << "chunk " << setw(6) << ss.str();
    cout << "  startIndex: " << setw(8) << chunk.startIndex;
    cout << "  elementCount: " << setw(8) << chunk.elementCount;
    cout << "  dataSize: " << setw(8) << chunk.dataSize << endl;
  }
}

void dumpClassMeta(DatabaseInfo &dbinfo, ClassId classId)
{
  lo::persistence::kvdump::ClassInfo *ci = nullptr;
  for(auto &cls : dbinfo.classInfos) {
    if(cls.classId == classId) {
      ci = &cls;
      break;
    }
  }
  if(!ci) {
    cout << "invalid classId" << endl;
    return;
  }
  size_t nameLen = 0;
  for(auto &pi : ci->propertyInfos) {
    if(pi.name.length() >nameLen) nameLen = pi.name.length();
  }

  cout << ci->name << endl;
  for(auto &pi : ci->propertyInfos) {
    cout << setw(nameLen) << std::resetiosflags(std::ios::adjustfield) << setiosflags(std::ios::left) << pi.name <<
        " (" << pi.id << ") " << " typeId: " << setw(4) << pi.typeId << " byteSize: " << setw(4) << pi.byteSize <<
        " isVector:" << (pi.isVector ? "y" : "n");

    switch(pi.storeLayout) {
      case StoreLayout::all_embedded:
        cout << " StoreLayout::all_embedded";
        break;
      case StoreLayout::embedded_key:
        cout << " StoreLayout::embedded_key";
        break;
      case StoreLayout::property:
        cout << " StoreLayout::property";
        break;
      case StoreLayout::none:
        cout << " StoreLayout::none";
        break;
    }
    if(!pi.className.empty()) cout << " class: " << pi.className;
    cout << endl;
  }

  for(auto &cls : dbinfo.classInfos) {
    if(binary_search(cls.subclasses.begin(), cls.subclasses.end(), ci->name)) {
      dumpClassMeta(dbinfo, cls.classId);
    }
  }
}

template <typename T> void dumpData(const char *tname, PropertyInfo &pi, ReadBuf &buf)
{
  if(TypeTraits<T>::id != pi.typeId)
    return;

  T val;
  ValueTraits<T>::getBytes(buf, val);

  cout << setw(15) << tname << " " << val;
}

void addSuperProperties(lo::persistence::kvdump::ClassInfo *ci, DatabaseInfo &dbinfo, vector<PropertyInfo> &properties)
{
  for(auto &cls : dbinfo.classInfos) {
    if (binary_search(cls.subclasses.begin(), cls.subclasses.end(), ci->name)) {
      addSuperProperties(&cls, dbinfo, properties);
      properties.insert(properties.end(), cls.propertyInfos.cbegin(), cls.propertyInfos.cend());
    }
  }
}

void dumpClassObjects(DatabaseInfo &dbinfo, ClassId classId)
{
  lo::persistence::kvdump::ClassInfo *ci = nullptr;
  for(auto &cls : dbinfo.classInfos) {
    if(cls.classId == classId) {
      ci = &cls;
      break;
    }
  }
  if(!ci) {
    cout << "invalid classId" << endl;
    return;
  }
  size_t nameLen = 0;
  for(auto &pi : ci->propertyInfos) {
    if(pi.name.length() >nameLen) nameLen = pi.name.length();
  }

  vector<PropertyInfo> properties;
  addSuperProperties(ci, dbinfo, properties);
  properties.insert(properties.end(), ci->propertyInfos.cbegin(), ci->propertyInfos.cend());

  auto txn = ::lmdb::txn::begin(dbinfo.m_env, nullptr, MDB_RDONLY);

  ::lmdb::val key;
  auto cursor = ::lmdb::cursor::open(txn, dbinfo.m_dbi_data);

  SK_CONSTR(sk, classId, 0, 0);
  key.assign(sk, sizeof(sk));

  if(cursor.get(key, MDB_SET_RANGE) && SK_CLASSID(key.data<byte_t>()) == classId) {
    do {
      ::lmdb::val val;
      cursor.get(key, val, MDB_GET_CURRENT);
      ObjectId oid = SK_OBJID(key.data<byte_t>());
      if(SK_PROPID(key.data<byte_t>()) == 0) {
        ReadBuf buf(val.data<byte_t>(), val.size());
        bool giveUp = false;

        cout << ci->name << " (" << oid << ")" << endl;
        for(auto &pi : properties) {
          cout << "  " << setw(nameLen+5) << std::resetiosflags(std::ios::adjustfield) << setiosflags(std::ios::left) <<
              pi.name << " (" << pi.id << "): ";

          switch(pi.storeLayout) {
            case StoreLayout::embedded_key:
              ClassId cid; ObjectId oid;
              buf.read(cid, oid);
              cout << setw(15) << "object key" << " " << "(" << cid << ", " << oid << ")";
              break;
            case StoreLayout::all_embedded:
              dumpData<short>("short", pi, buf);
              dumpData<unsigned short>("unsigned short", pi, buf);
              dumpData<int>("int", pi, buf);
              dumpData<unsigned int>("unsigned int", pi, buf);
              dumpData<long>("long", pi, buf);
              dumpData<unsigned long>("unsigned long", pi, buf);
              dumpData<long long>("long long", pi, buf);
              dumpData<unsigned long long>("unsigned long long", pi, buf);
              dumpData<bool>("bool", pi, buf);
              dumpData<float>("float", pi, buf);
              dumpData<double>("double", pi, buf);
              dumpData<const char *>("const char *", pi, buf);
              dumpData<std::string>("std::string", pi, buf);
              break;
            default:
              if(pi.byteSize) {
                buf.read(pi.byteSize);
                cout << "bytes[" << pi.byteSize << "]";
              }
              else {
                cout << "unknown size value. Giving up" << endl;
                giveUp = true;
              }
          }
          cout << endl;
          if(giveUp) break;
        }
      }
    } while(cursor.get(key, MDB_NEXT) && SK_CLASSID(key.data<byte_t>()) == classId);
  }
  cursor.close();
  txn.abort();
}

//
// Created by chris on 1/11/16.
//

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <kvstore.h>
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

namespace flexis {
namespace persistence {
namespace lmdb {
int meta_dup_compare(const MDB_val *a, const MDB_val *b);
int key_compare(const MDB_val *a, const MDB_val *b);
}}}

static const unsigned ObjectId_off = flexis::persistence::kv::ClassId_sz;
static const unsigned PropertyId_off = flexis::persistence::kv::ClassId_sz + flexis::persistence::kv::ObjectId_sz;

#define SK_CONSTR(nm, c, o, p) byte_t nm[StorageKey::byteSize]; *(ClassId *)nm = c; *(ObjectId *)(nm+ObjectId_off) = o; \
*(PropertyId *)(nm+PropertyId_off) = p

#define SK_CLASSID(k) *(ClassId *)k
#define SK_OBJID(k) *(ObjectId *)(k+ObjectId_off)
#define SK_PROPID(k) *(PropertyId *)(k+PropertyId_off)

using namespace std;
using namespace flexis::persistence::lmdb;

namespace flexis {
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

class DatabaseInfo
{
  string m_dbpath;

  ::lmdb::env m_env;
  ::lmdb::dbi m_dbi_meta = 0;
  ::lmdb::dbi m_dbi_data = 0;

public:
  vector<ClassInfo> classInfos;

  DatabaseInfo(string location, string name, size_t mapsize) : m_env(::lmdb::env::create())
  {
    m_dbpath = location;
    if(m_dbpath.back() != separator_char) m_dbpath += separator_char;
    m_dbpath += (name.empty() ? "kvdata" : name);

    m_env.set_mapsize(mapsize);

    //classmeta + classdata db
    m_env.set_max_dbs(2);

    m_env.open(m_dbpath.c_str(), MDB_NOSUBDIR | MDB_NOLOCK, 0664);

    MDB_stat envstat;
    mdb_env_stat(m_env, &envstat);

    auto txn = ::lmdb::txn::begin(m_env, nullptr);

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
    auto txn = ::lmdb::txn::begin(m_env, nullptr);

    ::lmdb::val key, val;
    auto cursor = ::lmdb::cursor::open(txn, m_dbi_meta);
    while(cursor.get(key, val, MDB_NEXT_NODUP)) {
      bool first = true;
      ::lmdb::val dupkey;

      classInfos.push_back(ClassInfo(string(key.data(), key.size())));
      ClassInfo &ci = classInfos.back();
      for (bool read = cursor.get(dupkey, val, MDB_FIRST_DUP); read; read = cursor.get(dupkey, val, MDB_NEXT_DUP)) {
        if(first) {
          //first record is [propertyId == 0, classId]
          ci.classId = read_integer<ClassId>(val.data<byte_t>()+2, 2);
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
    auto txn = ::lmdb::txn::begin(m_env, nullptr);

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
        ci.sum_objects_size += val.size();
      }
    }
    cursor.close();
    txn.abort();
  }
};

}
}
}

using namespace flexis::persistence;
using namespace flexis::persistence::kvdump;

std::ifstream::pos_type filesize(const char* filename)
{
  std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
  return in.tellg();
}

int main(int argc, char* argv[])
{
  if(argc < 3)
    cout << "usage: lo_dump <path> <name>" << endl;

  try {
    string path(argv[1]);
    string name(argv[2]);

    string fullpath = path + "/" + name;
    size_t freeSpace = (size_t)filesize(fullpath.c_str());

    DatabaseInfo dbinfo(path, name, freeSpace);

    dbinfo.loadClassMeta();

    for(auto &ci : dbinfo.classInfos) {
      dbinfo.loadClassData(ci);
      cout << ci.name << "(" << ci.num_objects << ", " << ci.sum_objects_size << ")" << endl;
    }
  }
  catch(::lmdb::runtime_error e) {
    cout <<"database error " << e.what() << endl;
  }
}

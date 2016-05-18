//
// Created by cse on 10/10/15.
//

#include <kvstore.h>
#include <lmdb++.h>
#include <lmdb/lmdb_kvstore.h>
#include <iostream>
#include <chrono>
#include <cassert>
#include "testclasses.h"

using namespace flexis::persistence;
using namespace flexis::persistence::kv;
using namespace flexis;
using namespace std;
using namespace flexis::Overlays;

namespace flexislmdb = flexis::persistence::lmdb;

static const long rounds = 1000000;

#define MK_KEY() char kbuf[10]; sprintf(kbuf, "%d", i);
#define MK_KEYL() char kbuf[50]; sprintf(kbuf, "%ld", i);
#define MK_DATA() char vbuf[50]; Data data(i, i+1, i+2); sprintf(vbuf, "%f;%f;%f", data.d1, data.d2, data.d3);

#define BEG() auto begin = std::chrono::high_resolution_clock::now();
#define DUR() std::chrono::high_resolution_clock::duration dur = std::chrono::high_resolution_clock::now() - begin; \
std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(dur); cout << ms.count() << endl;

void testColored2DPointWrite(KeyValueStore *kv)
{
  BEG()
  auto wtxn = kv->beginWrite(true); //we use a bulk transaction here,because we know objects are shallow
  for(int i=0; i< rounds; i++) {
    Colored2DPoint p;
    p.set(2.0f+i, 3.0f+i, 4.0f+i, 5.0f+i, 6.0f+i, 7.5f+i);
    wtxn->putObject(p);
  }
  wtxn->commit();
  DUR()
}

void testValueCollection(KeyValueStore *kv) {
  //test to persistent collection of scalar (primitve) values
  ObjectId collectionId;

  {
    //save test data
    vector<double> vect;
    for(unsigned i=0; i<1000; i++)
      vect.push_back(1.44 * i);

    auto wtxn = kv->beginWrite();
    collectionId = wtxn->putValueCollection(vect);
    wtxn->commit();
  }
  /*{
    //load saved collection
    auto rtxn = kv->beginRead();
    vector<double> loaded = rtxn->getValueCollection<double>(collectionId);
    assert(loaded.size() == 1000);
    rtxn->abort();
  }*/
  {
    //iterate over collection w/ cursor
    auto rtxn = kv->beginRead();

    unsigned count = 0;
    auto cursor = rtxn->openValueCursor<double>(collectionId);
    for (; !cursor->atEnd(); cursor->next()) {
      count++;
      double val = cursor->get();
    }
    assert(count == 1000);

    rtxn->abort();
  }
  {
    //append more test data.
    vector<double> vect;

    for(unsigned i=1; i<1001; i++)
      vect.push_back(5.66 * i);

    auto wtxn = kv->beginWrite();
    wtxn->appendValueCollection(collectionId, vect);
    wtxn->commit();
  }
  /*{
    //load saved collection
    auto rtxn = kv->beginRead();
    vector<double> loaded = rtxn->getValueCollection<double>(collectionId);
    assert(loaded.size() == 2000);
    rtxn->abort();
  }*/
  {
    //use appender to add more test data
    auto wtxn = kv->beginWrite();

    auto appender = wtxn->appendValueCollection<double>(collectionId);
    for(int i=0; i<1000; i++) appender->put(6.55 * i);
    appender->close();

    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();
    vector<double> loaded = rtxn->getValueCollection<double>(collectionId);
    assert(loaded.size() == 3000);
    rtxn->abort();
  }
}

void testColored2DPointRead(KeyValueStore *kv)
{
  BEG()
  auto rtxn = kv->beginRead();
  long count = 0;
  ObjectKey key;
  for(auto cursor = rtxn->openCursor<Colored2DPoint>(); !cursor->atEnd(); cursor->next()) {
    Colored2DPoint *loaded = cursor->get(key);

    if(loaded) {
      count++;
      delete loaded;
    }
  }
  rtxn->abort();
  assert(count == rounds);
  DUR()
}

//raw LMDB tests
//////////////////

//write Colored2DPoint w/ integer key + raw copying
void test_lmdb_write()
{
  auto env = ::lmdb::env::create();
  size_t sz = size_t(1) * size_t(1024) * size_t(1024) * size_t(1024); //1 GiB
  env.set_mapsize(sz);
  env.open(".", MDB_NOLOCK, 0664);

  BEG()
  auto wtxn = ::lmdb::txn::begin(env);
  auto dbi = ::lmdb::dbi::open(wtxn, nullptr, MDB_INTEGERKEY);

  for(size_t i=0; i< rounds; i++) {
    ::lmdb::val kval;
    kval.assign(&i, sizeof(i));

    Colored2DPoint p;
    p.set(2.0f+i, 3.0f+i, 4.0f+i, 5.0f+i, 6.0f+i, 7.5f+i);
    ::lmdb::val dval;
    dval.assign(&p, sizeof(p));

    dbi.put(wtxn, kval, dval, MDB_APPEND);
  }

  wtxn.commit();
  DUR()
}

//read + verify
void test_lmdb_read()
{
  auto env = ::lmdb::env::create();
  size_t sz = size_t(1) * size_t(1024) * size_t(1024) * size_t(1024); //1 GiB
  env.set_mapsize(sz);
  env.open(".", MDB_NOLOCK, 0664);

  BEG()
  auto rtxn = ::lmdb::txn::begin(env, nullptr, MDB_RDONLY);
  auto dbi = ::lmdb::dbi::open(rtxn, nullptr, MDB_INTEGERKEY);

  auto cursor = ::lmdb::cursor::open(rtxn, dbi);
  size_t k = 0;
  ::lmdb::val kval;
  kval.assign(&k, sizeof(k));

  long count = 0;
  if(cursor.get(kval, MDB_SET_RANGE)) {
    do {
      Colored2DPoint *pd = kval.data<Colored2DPoint>();
      if(pd) {
        Colored2DPoint *p = new Colored2DPoint();
        p->set(pd->x, pd->y, pd->r, pd->g, pd->b, pd->a);

        count++;
        delete p;
      }
    } while(cursor.get(kval, MDB_NEXT));
  }
  cursor.close();

  assert(count == rounds);
  rtxn.abort();
  DUR()
}

//test writing chunks of 0.5 MB
void test_lmdb_write2()
{
  auto env = ::lmdb::env::create();
  size_t sz = size_t(10) * size_t(1024) * size_t(1024) * size_t(1024); //1 GiB
  env.set_mapsize(sz);
  env.open(".", MDB_NOLOCK, 0664);

  BEG()

  auto wtxn = ::lmdb::txn::begin(env);
  auto dbi = ::lmdb::dbi::open(wtxn, nullptr, MDB_INTEGERKEY);
  for(size_t i=1; i < 20001; i++) {

    ::lmdb::val kval, dval;
    kval.assign(&i, sizeof(i));

    char buf[1024 * 512];
    memset(buf, 'x', 1024 * 512);
    dval.assign(buf, 1024 * 512);

    dbi.put(wtxn, kval, dval, 0);
  }
  wtxn.commit();

  DUR()
}

//test writing chunks of 0.5 MB, using MDB_RESERVE to avoid memcpying
void test_lmdb_write2a()
{
  auto env = ::lmdb::env::create();
  size_t sz = size_t(10) * size_t(1024) * size_t(1024) * size_t(1024); //1 GiB
  env.set_mapsize(sz);
  env.open(".", MDB_NOLOCK, 0664);

  BEG()

  auto wtxn = ::lmdb::txn::begin(env);
  auto dbi = ::lmdb::dbi::open(wtxn, nullptr, MDB_INTEGERKEY);
  for(size_t i=1; i < 20001; i++) {

    ::lmdb::val kval{&i, sizeof(i)};
    ::lmdb::val dval{nullptr, 1024 * 512};

    dbi.put(wtxn, kval, dval, MDB_RESERVE);

    memset(dval.data(), 'x', 1024 * 512);
  }
  wtxn.commit();

  DUR()
}

//read & verify
void test_lmdb_read2()
{
  auto env = ::lmdb::env::create();
  size_t sz = size_t(10) * size_t(1024) * size_t(1024) * size_t(1024); //1 GiB
  env.set_mapsize(sz);
  env.open(".", MDB_NOLOCK, 0664);

  BEG()
  auto rtxn = ::lmdb::txn::begin(env, nullptr, MDB_RDONLY);
  auto dbi = ::lmdb::dbi::open(rtxn, nullptr, MDB_INTEGERKEY);

  auto cursor = ::lmdb::cursor::open(rtxn, dbi);
  size_t k = 1;
  ::lmdb::val kval, dval;
  kval.assign(&k, sizeof(k));

  long count = 0;
  if(cursor.get(kval, dval, MDB_SET)) {
    do {
      //cursor.get(kval, dval, MDB_GET_CURRENT);
      assert(dval.size() == 1024 * 512 && dval.data()[0] == 'x');
      count++;
    } while(cursor.get(kval, dval, MDB_NEXT));
  }
  cursor.close();

  assert(count == 20000);
  rtxn.abort();
  DUR()
}

int main()
{
#if 1
  KeyValueStore *kv = flexislmdb::KeyValueStore::Factory{0, ".", "bench"};

  kv->putSchema<Colored2DPoint, ColoredPolygon>();

  testColored2DPointWrite(kv);
  testColored2DPointRead(kv);
  testValueCollection(kv);

  delete kv;

  test_lmdb_write();
  test_lmdb_read();
#endif
#if 0

  test_lmdb_write2a();
  test_lmdb_read2();
#endif

  return 0;
}

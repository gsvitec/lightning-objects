//
// Created by cse on 10/10/15.
//

#include <sstream>
#include <kvstore/kvstore.h>
#include <kv/kvlibtraits.h>
#include <lmdb_kvstore.h>
#include "testclasses.h"

using namespace flexis::persistence;
using namespace flexis::persistence::kv;
using namespace flexis;
using namespace std;

void testColored2DPoint(KeyValueStore *kv) 
{
  long id = 0;

  Colored2DPoint p;
  p.set(2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.5f);

  auto wtxn = kv->beginWrite();
  id = wtxn->putObject(p);
  wtxn->commit();

  Colored2DPoint *p2;
  auto rtxn = kv->beginRead();
  p2 = rtxn->getObject<Colored2DPoint>(id);
  rtxn->abort();

  assert(p2 && p2->x == 2.0f && p2->y == 3.0f && p2->r == 4.0f && p2->g == 5.0f && p2->b == 6.0f && p2->a == 7.5f);
  delete p2;
}

void testColoredPolygon(KeyValueStore *kv)
{
  long id = 0;

  auto wtxn = kv->beginWrite();

  ColoredPolygon polygon;
  polygon.visible = true;

  Colored2DPoint p1;
  p1.set(1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f);
  polygon.pts.push_back(p1);

  Colored2DPoint p2;
  p2.set(2.0f, 2.0f, 2.0f, 2.0f, 2.0f, 2.0f);
  polygon.pts.push_back(p2);

  Colored2DPoint p3;
  p3.set(3.0f, 3.0f, 3.0f, 3.0f, 3.0f, 3.0f);
  polygon.pts.push_back(p3);

  id = wtxn->putObject(polygon);

  wtxn->commit();

  ColoredPolygon *loaded;
  auto rtxn = kv->beginRead();
  loaded = rtxn->getObject<ColoredPolygon>(id);
  rtxn->abort();

  assert(loaded && loaded->pts.size() == 3);
  delete loaded;
}

void testColoredPolygonIterator(KeyValueStore *kv)
{
  auto rtxn = kv->beginRead();

  for(auto cursor = rtxn->openCursor<ColoredPolygon>(); !cursor->atEnd(); ++(*cursor)) {
    auto loaded = cursor->get();
    if(loaded) {
      cout << "loaded ColoredPolygon visible: " << loaded->visible << " pts: " << loaded->pts.size() << endl;
    }
  }

  for(auto cursor = rtxn->openCursor<Colored2DPoint>(); !cursor->atEnd(); ++(*cursor)) {
    auto loaded = cursor->get();
    if(loaded) {
      cout << "loaded Colored2DPoint x: " << loaded->x << " y: " << loaded->y << endl;
    }
  }

  rtxn->abort();
}

void testValueVectorProperty(KeyValueStore *kv)
{
  ObjectId oid;
  {
    OtherThingA hans("Hans");
    hans.testnames.push_back("Eva");
    hans.testnames.push_back("Rudi");

    auto wtxn = kv->beginWrite();

    oid = wtxn->putObject(hans);

    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    OtherThingA *hans = rtxn->getObject<OtherThingA>(oid);
    assert(hans && hans->testnames.size() == 2);
    assert(hans->testnames[0] == "Eva");
    assert(hans->testnames[1] == "Rudi");

    rtxn->abort();
    delete hans;
  }
}

void testPolymorphism(KeyValueStore *kv)
{
  flexis::player::SourceInfo si;
  auto ro = make_obj<RectangularOverlay>();
  auto to = make_obj<TimeCodeOverlay>();

  ro->rangeInP->setValue(-1);
  ro->rangeOutP->setValue(-1);
  ro->validFor.push_back(flexis::data::recording::ContextType::Player);
  to->rangeInP->setValue(-1);
  to->rangeOutP->setValue(-1);
  to->validFor.push_back(flexis::data::recording::ContextType::Player);
  si.userOverlays.push_back(to);
  si.userOverlays.push_back(ro);

  long val = -1;
  WriteBuf bf(10);
  ValueTraits<long>::putBytes(bf, val);
  ReadBuf rbuf;
  rbuf.start(bf.data()+2, 9);
  ValueTraits<long>::getBytes(rbuf, val);

  auto wtxn = kv->beginWrite();
  ObjectId id = wtxn->putObject(si);
  wtxn->commit();

  player::SourceInfo *loaded;
  auto rtxn = kv->beginRead();
  loaded = rtxn->getObject<player::SourceInfo>(id);
  rtxn->abort();

  assert(loaded && loaded->userOverlays.size() == 2
         && loaded->userOverlays[0]->rangeInP->getValue() == -1
            && loaded->userOverlays[0]->rangeOutP->getValue() == -1);
  for(auto &ovl : loaded->userOverlays)
    cout << ovl->type() << endl;
  delete loaded;
}


using OtherThingPtr = shared_ptr<OtherThing>;

void testLazyPolymorphicCursor(KeyValueStore *kv)
{
  ObjectId sv_id;
  {
    //insert test data
    SomethingWithALazyVector sv;

    auto ptra = make_obj<OtherThingA>("Hans");
    sv.otherThings.push_back(ptra);

    auto ptrb = make_obj<OtherThingB>("Otto");
    sv.otherThings.push_back(ptrb);

    auto wtxn = kv->beginWrite();

    //save the object proper
    sv_id = wtxn->putObject(sv);

    //since otherThings is lazy, we need to kick it separately
    wtxn->updateMember(sv_id, sv, PROPERTY_ID(SomethingWithALazyVector, otherThings));

    wtxn->commit();
  }
  {
    //test deferred full load
    SomethingWithALazyVector *loaded;
    auto rtxn = kv->beginRead();

    loaded = rtxn->getObject<SomethingWithALazyVector>(sv_id);
    assert(loaded && loaded->otherThings.empty());

    rtxn->loadMember(sv_id, *loaded, PROPERTY_ID(SomethingWithALazyVector, otherThings));

    assert(loaded && loaded->otherThings.size() == 2);
    for (auto &ot : loaded->otherThings)
      cout << ot->sayhello() << " my name is " << ot->name << " my number is " << ot->dvalue << endl;

    rtxn->abort();
    delete loaded;
  }
  {
    //test lazy cursor
    SomethingWithALazyVector *loaded;
    auto rtxn = kv->beginRead();

    loaded = rtxn->getObject<SomethingWithALazyVector>(sv_id);
    assert(loaded && loaded->otherThings.empty());

    unsigned count = 0;
    auto cursor = rtxn->openCursor<SomethingWithALazyVector, OtherThing>(sv_id, loaded,
                                                                         PROPERTY_ID(SomethingWithALazyVector, otherThings));
    for (; !cursor->atEnd(); cursor->next()) {
      count++;
      auto ot = cursor->get();
      cout << ot->sayhello() << " my name is " << ot->name << " my number is " << ot->dvalue << endl;
    }
    assert(count == 2);

    rtxn->abort();
    delete loaded;
  }
  {
    //test data-only access (no object instantiation, no copying, read into mapped memory)
    SomethingWithALazyVector *loaded;
    auto rtxn = kv->beginRead();

    loaded = rtxn->getObject<SomethingWithALazyVector>(sv_id);
    assert(loaded && loaded->otherThings.empty());

    unsigned count = 0;
    auto cursor = rtxn->openCursor<SomethingWithALazyVector, OtherThing>(sv_id, loaded,
                                                                         PROPERTY_ID(SomethingWithALazyVector, otherThings));
    if(!cursor->atEnd()) {
      for (; !cursor->atEnd(); cursor->next()) {
        count++;
        const char *name; const byte_t *buf=nullptr;
        double *dval;

        //we're passing in buf, so that only the first call will go to the store
        cursor->get(PROPERTY_ID(OtherThing, name), (const byte_t **)&name, &buf);
        //buf is set now, so this call will simply return a pointer into buf
        cursor->get(PROPERTY_ID(OtherThing, dvalue), (const byte_t **)&dval, &buf);

        cout << name << " dvalue: " << *dval << endl;
      }
    }
    assert(count == 2);
    rtxn->abort();
    delete loaded;
  }
}

void testObjectCollection(KeyValueStore *kv)
{
  //test polymorphic access to persistent collection
  ObjectId collectionId;

  {
    //save test data
    vector<OtherThingPtr> vect;

    vect.push_back(OtherThingPtr(new OtherThingA("Hans")));
    vect.push_back(OtherThingPtr(new OtherThingB("Otto")));
    vect.push_back(OtherThingPtr(new OtherThingB("Gabi")));
    vect.push_back(OtherThingPtr(new OtherThingB("Josef")));
    vect.push_back(OtherThingPtr(new OtherThingB("Mario")));
    vect.push_back(OtherThingPtr(new OtherThingB("Fred")));
    vect.push_back(OtherThingPtr(new OtherThingB("Gunter")));
    vect.push_back(OtherThingPtr(new OtherThingB("Johannes")));
    vect.push_back(OtherThingPtr(new OtherThingB("Friedrich")));
    vect.push_back(OtherThingPtr(new OtherThingB("Thomas")));
    vect.push_back(OtherThingPtr(new OtherThingB("Rudi")));
    vect.push_back(OtherThingPtr(new OtherThingB("Sabine")));

    auto wtxn = kv->beginWrite();

    collectionId = wtxn->putCollection(vect);

    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();

    vector<OtherThingPtr> loaded = rtxn->getCollection<OtherThing>(collectionId);
    assert(loaded.size() == 12);

    cout << "FULLY LOADED COLLECTION:" << endl;
    for (auto &ot : loaded)
      cout << ot->sayhello() << " my name is " << ot->name << " my number is " << ot->dvalue << endl;

    rtxn->abort();
  }
  {
    //iterate over collection w/ cursor
    auto rtxn = kv->beginRead();

    cout << "COLLECTION CURSOR:" << endl;
    unsigned count = 0;
    auto cursor = rtxn->openCursor<OtherThing>(collectionId);
    for (; !cursor->atEnd(); cursor->next()) {
      count++;
      OtherThing *ot = cursor->get();
      cout << ot->sayhello() << " my name is " << ot->name << " my number is " << ot->dvalue << endl;
      delete ot;
    }
    assert(count == 12);

    rtxn->abort();
  }
  {
    //append more test data
    vector<OtherThingPtr> vect;

    vect.push_back(OtherThingPtr(new OtherThingA("Marcel")));
    vect.push_back(OtherThingPtr(new OtherThingB("Marianne")));
    vect.push_back(OtherThingPtr(new OtherThingB("Nicolas")));

    auto wtxn = kv->beginWrite();

    wtxn->appendCollection(collectionId, vect);

    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();
    vector<OtherThingPtr> loaded = rtxn->getCollection<OtherThing>(collectionId);
    assert(loaded.size() == 15);
    rtxn->abort();
  }
  {
    //use appender to add more test data
    auto wtxn = kv->beginWrite();

    auto appender = wtxn->appendCollection<OtherThing>(collectionId);
    for(int i=0; i<10; i++) {
      stringstream ss;
      ss << "Test_" << i;
      appender->put(OtherThingPtr(new OtherThingB(ss.str())));
    }
    for(int i=10; i<20; i++) {
      stringstream ss;
      ss << "Test_" << i;
      OtherThingB ob(ss.str());
      appender->put(&ob);
    }
    appender->close();

    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();
    vector<OtherThingPtr> loaded = rtxn->getCollection<OtherThing>(collectionId);

    cout << "APPEND TEST:" << endl;
    for(auto ot : loaded)
      cout << ot->sayhello() << " my name is " << ot->name << " my number is " << ot->dvalue << endl;

    assert(loaded.size() == 35);
    rtxn->abort();
  }
}

//test persistent collection of scalar (primitve) values
void testValueCollection(KeyValueStore *kv)
{
  ObjectId collectionId;

  {
    //save test data
    vector<double> vect;

    for(unsigned i=0; i<10; i++)
      vect.push_back(1.44 * i);

    auto wtxn = kv->beginWrite();

    collectionId = wtxn->putValueCollection(vect);

    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();

    vector<double> loaded = rtxn->getValueCollection<double>(collectionId);
    assert(loaded.size() == 10);

    cout << "FULLY LOADED VALUE COLLECTION:" << endl;
    for (auto ot : loaded)
      cout << "value: " << ot << endl;

    rtxn->abort();
  }
  {
    //iterate over collection w/ cursor
    auto rtxn = kv->beginRead();

    cout << "VALUE COLLECTION CURSOR:" << endl;
    unsigned count = 0;
    auto cursor = rtxn->openValueCursor<double>(collectionId);
    for (; !cursor->atEnd(); cursor->next()) {
      count++;
      double val = cursor->get();
      cout << "value: " << val << endl;
    }
    assert(count == 10);

    rtxn->abort();
  }
  {
    //append more test data. Actually, this could be of any type, but then you'd have to know
    //exactly when to read what
    vector<double> vect;

    for(unsigned i=1; i<6; i++)
      vect.push_back(5.66 * i);

    auto wtxn = kv->beginWrite();

    wtxn->appendValueCollection(collectionId, vect, 128);

    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();
    vector<double> loaded = rtxn->getValueCollection<double>(collectionId);
    assert(loaded.size() == 15);
    rtxn->abort();
  }
  {
    //use appender to add more test data
    auto wtxn = kv->beginWrite();

    auto appender = wtxn->appendValueCollection<double>(collectionId, 128);
    for(int i=0; i<20; i++) {
      appender->put(6.55 * i);
    }
    appender->close();

    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();
    vector<double> loaded = rtxn->getValueCollection<double>(collectionId);

    cout << "APPEND TEST:" << endl;
    for(auto ot : loaded)
      cout << "value: " << ot << endl;

    assert(loaded.size() == 35);
    rtxn->abort();
  }
}

//test persistent collection of scalar (primitve) values sub-array API
void testValueCollectionData(KeyValueStore *kv)
{
  ObjectId collectionId;

  {
    //save test data
    double vect[1000];

    for (unsigned i = 0; i < 1000; i++)
      vect[i] = 1.44 * i;

    auto wtxn = kv->beginWrite();

    collectionId = wtxn->putDataCollection(vect, 1000);

    wtxn->commit();
  }
  {
    //load saved collection arrays
    auto rtxn = kv->beginExclusiveRead();

    CollectionData<double>::Ptr cd1 = rtxn->getValueCollectionData<double>(collectionId, 2, 3);
    assert(cd1);
    double *data = cd1->data();

    double d0 = data[0];
    double d2 = data[2];
    assert(d0 == 1.44 * 2 && d2 == 1.44 * 4);

    CollectionData<double>::Ptr cd2 = rtxn->getValueCollectionData<double>(collectionId, 2, 100);
    assert(cd2);
    data = cd2->data();
    d0 = data[0];
    double d99 = data[99];
    assert(d0 == 1.44 * 2 && d99 == 1.44 * 101);

    auto cd3 = rtxn->getValueCollectionData<double>(collectionId, 100, 50);
    data = cd3->data();
    assert(data[0] == 1.44 * 100 && data[49] == 1.44 * 149);

    rtxn->abort();
  }
}
//test persistent collection of scalar (primitve) values sub-array API
void testValueCollectionData2(KeyValueStore *kv)
{
  ObjectId collectionId2;
  {
    //raw array API:
    long long darray[100];
    for(int i=0;i<100; i++) darray[i] = -99999 * i;

    auto wtxn = kv->beginWrite();

    collectionId2 = wtxn->putDataCollection(darray, 100);

    wtxn->commit();
  }
  {
    auto rtxn = kv->beginExclusiveRead();

    auto cd4 = rtxn->getValueCollectionData<long long>(collectionId2, 10, 50);
    long long *data2 = cd4->data();
    assert(data2[0] == -99999 * 10 && data2[49] == -99999 * 59);

    rtxn->abort();
  }
  {
    //raw array API:
    long long darray[100];
    for(int i=0;i<100; i++) darray[i] = 555 * i;

    auto wtxn = kv->beginWrite();

    wtxn->appendDataCollection(collectionId2, darray, 100, 128);

    wtxn->commit();
  }
  {
    auto rtxn = kv->beginExclusiveRead();

    auto cd = rtxn->getValueCollectionData<long long>(collectionId2, 190, 10);
    long long *data = cd->data();
    assert(data[0] == 555 * 90 && data[9] == 555 * 99);

    rtxn->abort();
  }
}

void  testObjectPtrPropertyStorage(KeyValueStore *kv)
{
  auto sd = make_obj<flexis::player::SourceDisplayConfig>(1, 2, false, 4, 5, 6, 7);

  flexis::player::SourceInfo *si = new flexis::player::SourceInfo(sd);

  auto wtxn = kv->beginWrite();
  ObjectId id = wtxn->putObjectP(si);
  wtxn->commit();

  auto rtxn = kv->beginRead();
  flexis::player::SourceInfo *si2 = rtxn->getObject<flexis::player::SourceInfo>(id);
  rtxn->abort();

  assert(si2 && si2->displayConfig && si2->displayConfig->sourceIndex == 1 && si2->displayConfig->attachedIndex == 2);

  delete si2;
  delete si;
}

void testObjectVectorPropertyStorageEmbedded(KeyValueStore *kv)
{
  ObjectId objectId;
  {
    SomethingWithAnEmbbededObjectVector sweov;
    sweov.name = "sweov";

    sweov.objects.push_back(FixedSizeObject(1, 2));
    sweov.objects.push_back(FixedSizeObject(3, 4));
    sweov.objects.push_back(FixedSizeObject(5, 6));
    sweov.objects.push_back(FixedSizeObject(7, 8));

    auto wtxn = kv->beginWrite();
    objectId = wtxn->putObject(sweov);
    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    SomethingWithAnEmbbededObjectVector *loaded = rtxn->getObject<SomethingWithAnEmbbededObjectVector>(objectId);

    assert(loaded && loaded->name == "sweov" && loaded->objects.size() == 4 \
           && loaded->objects[0].number1 == 1 \
           && loaded->objects[0].number2 == 2 \
           && loaded->objects[1].number1 == 3 \
           && loaded->objects[3].number1 == 7 \
           && loaded->objects[3].number2 == 8);
    delete loaded;
  }
}

void testGrowDatabase(KeyValueStore *kv)
{
  ObjectId collectionId;
  {
    auto wtxn = kv->beginWrite();

    unsigned data [1000];
    for(unsigned i=0; i<1000; i++) data[i] = i;
    collectionId = wtxn->putDataCollection(data, 1000);

    for(unsigned i=0; i<1000; i++) {
      for(unsigned j=0; j<1000; j++) data[j] = (i+1) * 1000 + j;
      wtxn->appendDataCollection(collectionId, data, 1000);
    }
    wtxn->commit();
  }
  {
    auto rtxn = kv->beginExclusiveRead();

    auto coll = rtxn->getValueCollectionData<unsigned>(collectionId, 2000, 10);
    unsigned * data = coll->data();

    assert(data[0] == 2000 && data[9] == 2009);

    coll = rtxn->getValueCollectionData<unsigned>(collectionId, 900000, 11);
    data = coll->data();

    assert(data[0] == 900000 && data[10] == 900010);

    coll = rtxn->getValueCollectionData<unsigned>(collectionId, 1000000-10, 10);
    data = coll->data();

    assert(data[0] == 999990 && data[9] == 999999);

    coll = rtxn->getValueCollectionData<unsigned>(collectionId, 1001000-10, 10);
    data = coll->data();

    assert(data[0] == 1000990 && data[9] == 1000999);

    rtxn->abort();
  }
}

int main()
{
  KeyValueStore *kv = lmdb::KeyValueStore::Factory{".", "test"};
  kv->registerType<FixedSizeObject>();
  kv->registerType<SomethingWithAnEmbbededObjectVector>();

#if 1
  kv->registerType<Colored2DPoint>();
  kv->registerType<ColoredPolygon>();

  kv->registerType<player::SourceDisplayConfig>();
  kv->registerType<player::SourceInfo>();

  kv->registerAbstractType<flexis::data::recording::StreamProcessor>();
  kv->registerAbstractType<flexis::IFlexisOverlay>();
  kv->registerType<flexis::RectangularOverlay>();
  kv->registerType<flexis::TimeCodeOverlay>();

  kv->registerAbstractType<OtherThing>();
  kv->registerType<OtherThingA>();
  kv->registerType<OtherThingB>();
  kv->registerType<SomethingWithALazyVector>();

  testColored2DPoint(kv);
  testColoredPolygon(kv);
  testColoredPolygonIterator(kv);
  testPolymorphism(kv);
  testLazyPolymorphicCursor(kv);
  testObjectCollection(kv);
  testValueCollection(kv);
  testObjectPtrPropertyStorage(kv);
  testValueVectorProperty(kv);
  testObjectVectorPropertyStorageEmbedded(kv);
  testValueCollectionData(kv);
  testValueCollectionData2(kv);
#endif
  testGrowDatabase(kv);

  delete kv;
  return 0;
}

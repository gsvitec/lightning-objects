//
// Created by cse on 10/10/15.
//

#include <cassert>
#include <sstream>
#include <kvstore.h>
#include <lmdb/lmdb_kvstore.h>
#include "testclasses.h"

using namespace lo::persistence;
using namespace lo::persistence::kv;
using namespace lo;
using namespace std;
using namespace lo::Overlays;

template <typename T>
bool all_predicate(shared_ptr<T> t=nullptr) {return true;}

template <typename T>
unsigned countInstances(TransactionPtr tr, function<bool(shared_ptr<T>)> predicate=all_predicate<T>)
{
  unsigned count = 0;
  for(auto curs = tr->openCursor<T>(); !curs->atEnd(); curs->next()) {
    if(predicate(curs->get())) {
      count++;
    }
  }
  return count;
}
template <typename T>
vector<shared_ptr<T>> getInstances(TransactionPtr tr, function<bool(shared_ptr<T>)> predicate=all_predicate<T>)
{
  vector<shared_ptr<T>> result;
  for(auto curs = tr->openCursor<T>(); !curs->atEnd(); curs->next()) {
    auto obj = curs->get();
    if(predicate(obj)) {
      result.push_back(obj);
    }
  }
  return result;
}

void testKeyedProperties(KeyValueStore *kv)
{
  SomethingWithAllValueKeyedProperties swakp;
  swakp.name = "James";
  swakp.counter = 22;
  swakp.numbers = {1, 2, 3, 4, 5, 6};
  swakp.children = set<string>({"Bob", "Mary", "Jim"});

  ObjectKey key;
  auto wtxn = kv->beginWrite();
  wtxn->saveObject(swakp, key);
  wtxn->commit();

  SomethingWithAllValueKeyedProperties *p2;
  auto rtxn = kv->beginRead();
  p2 = rtxn->getObject<SomethingWithAllValueKeyedProperties>(key);
  rtxn->end();

  assert(p2->name == "James" && p2->counter == 22 && p2->numbers.size() == 6 && p2->numbers[3] == 4
         && p2->children.size() == 3 && p2->children.count("Jim"));
}

void testColored2DPoint(KeyValueStore *kv)
{
  ObjectKey key;

  Colored2DPoint p;
  p.set(2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.5f);

  auto wtxn = kv->beginWrite();
  wtxn->saveObject(p, key);
  wtxn->commit();

  Colored2DPoint *p2;
  auto rtxn = kv->beginRead();
  p2 = rtxn->getObject<Colored2DPoint>(key);
  rtxn->end();

  assert(p2 && p2->x == 2.0f && p2->y == 3.0f && p2->r == 4.0f && p2->g == 5.0f && p2->b == 6.0f && p2->a == 7.5f);
  delete p2;
}

void testColoredPolygon(KeyValueStore *kv)
{
  ObjectKey key;

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

  wtxn->saveObject(polygon, key);

  wtxn->commit();

  ColoredPolygon *loaded;
  auto rtxn = kv->beginRead();
  loaded = rtxn->getObject<ColoredPolygon>(key);
  rtxn->end();

  assert(loaded && loaded->pts.size() == 3);
  delete loaded;
}

void testColoredPolygonIterator(KeyValueStore *kv)
{
  auto rtxn = kv->beginRead();

  for(auto cursor = rtxn->openCursor<ColoredPolygon>(); !cursor->atEnd(); cursor->next()) {
    auto loaded = cursor->get();
    if(loaded) {
      cout << "loaded ColoredPolygon visible: " << loaded->visible << " pts: " << loaded->pts.size() << endl;
    }
  }

  for(auto cursor = rtxn->openCursor<Colored2DPoint>(); !cursor->atEnd(); cursor->next()) {
    auto loaded = cursor->get();
    if(loaded) {
      cout << "loaded Colored2DPoint x: " << loaded->x << " y: " << loaded->y << endl;
    }
  }

  rtxn->end();
}

void testClassCursor(KeyValueStore *kv)
{
  {
    auto wtxn = kv->beginWrite();

    SomethingVirtual sv1(11, "SomethingVirtual");
    SomethingVirtual sv2(12, "SomethingVirtualAgain");
    SomethingVirtual1 sv11(13, "SomethingVirtual1", "Programmer");
    SomethingVirtual2 sv21(14, "SomethingVirtual2", "Knitting");
    SomethingVirtual3 sv31(15, "SomethingVirtual3", "Knitting", 22);

    wtxn->putObject(sv1);
    wtxn->putObject(sv2);
    wtxn->putObject(sv11);
    wtxn->putObject(sv21);
    wtxn->putObject(sv31);

    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    unsigned count = 0;
    auto cursor = rtxn->openCursor<SomethingVirtual>();
    while(!cursor->atEnd()) {
      count++;
      auto sv = cursor->get();
      sv->sayhello(); cout << endl;
      cursor->next();
    }

    assert(count == 5);
    rtxn->end();
  }
}

void testValueVectorProperty(KeyValueStore *kv)
{
  ObjectKey key;
  {
    OtherThingA hans("Hans");
    hans.testnames.push_back("Eva");
    hans.testnames.push_back("Rudi");
    hans.enumtest = TestEnum::Three;

    auto wtxn = kv->beginWrite();

    wtxn->saveObject(hans, key);

    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    OtherThingA *hans = rtxn->getObject<OtherThingA>(key);
    assert(hans && hans->testnames.size() == 2);
    assert(hans->testnames[0] == "Eva");
    assert(hans->testnames[1] == "Rudi");
    assert(hans->enumtest == TestEnum::Three);

    rtxn->end();
    delete hans;
  }
}

void testObjectMappings(KeyValueStore *kv)
{
  ObjectKey key;
  {
    ObjectPropertyTest test(1, 2, 3, "testname");
    test.fso_vect.push_back(FixedSizeObject(4, 5));
    test.vso_vect.push_back(VariableSizeObject(6, "sechs"));
    test.fso_vect.push_back(FixedSizeObject(7, 8));
    test.vso_vect.push_back(VariableSizeObject(9, "neun"));

    auto wtxn = kv->beginWrite();

    wtxn->saveObject(test, key);

    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    ObjectPropertyTest *test = rtxn->getObject<ObjectPropertyTest>(key);
    assert(test && test->fso_vect.size() == 2 && test->vso_vect.size() == 2);
    assert(test->fso.number1 == 1 && test->fso.number2 == 2);
    assert(test->vso.number == 3 && test->vso.name == "testname");
    assert(test->fso_vect[0].number1 == 4 && test->fso_vect[1].number1 == 7);
    assert(test->vso_vect[0].number == 6 && test->vso_vect[1].number == 9);

    rtxn->end();
    delete test;
  }
}

void testCustomValueTypes(KeyValueStore *kv)
{
  ObjectKey key;
  {
    VariableSizeObject vo;
    vo.number = 1;
    vo.name = "HansOtto";
    vo.vtest.number = 22;
    vo.vtest.name = "Gabi";
    vo.vtest2.number = 1.55;
    vo.vtest2.number2 = 3.55;

    auto wtxn = kv->beginWrite();

    wtxn->saveObject(vo, key);

    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    VariableSizeObject *vo = rtxn->getObject<VariableSizeObject>(key);
    assert(vo->number == 1);
    assert(vo->name == "HansOtto");
    assert(vo->vtest.number == 22);
    assert(vo->vtest.name == "Gabi");
    assert(vo->vtest2.number == (float)1.55);
    assert(vo->vtest2.number2 == (float)3.55);

    rtxn->end();
    delete vo;
  }
}

void testFlexisProperties(KeyValueStore *kv)
{
  lo::player::SourceInfo si;
  auto ro = make_obj<TestRectangularOverlay>();
  auto to = make_obj<TimeCodeOverlay>();

  ro->rangeIn = -1;
  ro->rangeOut = -1;
  to->rangeIn = -1;
  to->rangeOut = -1;
  si.userOverlays.push_back(to);
  si.userOverlays.push_back(ro);

  auto wtxn = kv->beginWrite();
  ObjectKey key;
  wtxn->saveObject(si, key);
  wtxn->commit();

  player::SourceInfo *loaded;
  auto rtxn = kv->beginRead();
  loaded = rtxn->getObject<player::SourceInfo>(key);
  rtxn->end();

  assert(loaded && loaded->userOverlays.size() == 2
         && loaded->userOverlays[0]->rangeIn == -1
            && loaded->userOverlays[0]->rangeOut == -1);
  for(auto &ovl : loaded->userOverlays)
    cout << ovl->type() << endl;
  delete loaded;
}


using OtherThingPtr = shared_ptr<OtherThing>;

void testLazyPolymorphicCursor(KeyValueStore *kv)
{
  ObjectKey key;
  {
    //insert test data
    SomethingWithALazyVector sv;

    auto ptra = make_obj<OtherThingA>("Hans");
    sv.otherThings.push_back(ptra);

    auto ptrb = make_obj<OtherThingB>("Otto");
    sv.otherThings.push_back(ptrb);

    auto wtxn = kv->beginWrite();

    //save the object proper
    wtxn->saveObject(sv, key);

    //since otherThings is lazy, we need to kick it separately
    wtxn->updateMember(key, sv, PROPERTY(SomethingWithALazyVector, otherThings));

    wtxn->commit();
  }
  {
    //test deferred full load
    SomethingWithALazyVector *loaded;
    auto rtxn = kv->beginRead();

    loaded = rtxn->getObject<SomethingWithALazyVector>(key);
    assert(loaded && loaded->otherThings.empty());

    rtxn->loadMember(key.objectId, *loaded, PROPERTY(SomethingWithALazyVector, otherThings));

    assert(loaded && loaded->otherThings.size() == 2);
    for (auto &ot : loaded->otherThings)
      cout << ot->sayhello() << " my name is " << ot->name << " my number is " << ot->dvalue << endl;

    rtxn->end();
    delete loaded;
  }
  {
    //test lazy cursor
    SomethingWithALazyVector *loaded;
    auto rtxn = kv->beginRead();

    loaded = rtxn->getObject<SomethingWithALazyVector>(key);
    assert(loaded && loaded->otherThings.empty());

    unsigned count = 0;
    auto cursor = rtxn->openCursor<SomethingWithALazyVector, OtherThing>(key.objectId, PROPERTY_ID(SomethingWithALazyVector, otherThings));
    for (; !cursor->atEnd(); cursor->next()) {
      count++;
      auto ot = cursor->get();
      cout << ot->sayhello() << " my name is " << ot->name << " my number is " << ot->dvalue << endl;
    }
    assert(count == 2);

    rtxn->end();
    delete loaded;
  }
  {
    //test data-only access (no object instantiation, no copying, read into mapped memory)
    SomethingWithALazyVector *loaded;
    auto rtxn = kv->beginRead();

    loaded = rtxn->getObject<SomethingWithALazyVector>(key);
    assert(loaded && loaded->otherThings.empty());

    unsigned count = 0;
    auto cursor = rtxn->openCursor<SomethingWithALazyVector, OtherThing>(key.objectId, PROPERTY_ID(SomethingWithALazyVector, otherThings));
    if(!cursor->atEnd()) {
      for (; !cursor->atEnd(); cursor->next()) {
        count++;
        const char *name;
        double *dval;

        //we're passing in buf, so that only the first call will go to the store. We are not copying the buffer,
        //which we can only do because we are sure no updates will happen until we are done
        ObjectBuf obuf(false);
        cursor->get(PROPERTY(OtherThing, name), obuf);
        name = (const char *)obuf.read();

        //buf is set now, so this call will simply return a pointer into buf
        cursor->get(PROPERTY(OtherThing, dvalue), obuf);
        dval = (double *)obuf.read();

        cout << name << " dvalue: " << *dval << endl;
      }
    }
    assert(count == 2);
    rtxn->end();
    delete loaded;
  }
}

void benchObjectCollection(KeyValueStore *kv)
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

    rtxn->end();
  }
  {
    //iterate over collection w/ cursor
    auto rtxn = kv->beginRead();

    cout << "COLLECTION CURSOR:" << endl;
    unsigned count = 0;
    auto cursor = rtxn->openCursor<OtherThing>(collectionId);
    while (OtherThing *ot = cursor->get()) {
      count++;
      cout << ot->sayhello() << " my name is " << ot->name << " my number is " << ot->dvalue << endl;
      delete ot;
    }
    assert(count == 12);

    rtxn->end();
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
    rtxn->end();
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
    rtxn->end();
  }
}

//test persistent collection of scalar (primitve) values
void benchValueCollection(KeyValueStore *kv)
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

    rtxn->end();
  }
  {
    //iterate over collection w/ cursor
    auto rtxn = kv->beginRead();

    cout << "VALUE COLLECTION CURSOR:" << endl;
    unsigned count = 0;
    auto cursor = rtxn->openValueCursor<double>(collectionId);
    for (double val; cursor->get(val); ) {
      count++;
      cout << "value: " << val << endl;
    }
    assert(count == 10);

    rtxn->end();
  }
  {
    //append more test data. Actually, this could be of any type, but then you'd have to know
    //exactly when to read what
    vector<double> vect;

    for(unsigned i=1; i<6; i++)
      vect.push_back(5.66 * i);

    auto wtxn = kv->beginWrite();

    wtxn->appendValueCollection(collectionId, vect);

    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();
    vector<double> loaded = rtxn->getValueCollection<double>(collectionId);
    assert(loaded.size() == 15);
    rtxn->end();
  }
  {
    //use appender to add more test data
    auto wtxn = kv->beginWrite();

    auto appender = wtxn->appendValueCollection<double>(collectionId);
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
    rtxn->end();
  }
}

//test persistent collection of scalar (primitve) values sub-array API
void testDataCollection1(KeyValueStore *kv)
{
  ObjectId collectionId, collectionId2;

  {
    //save test data into application-side buffer
    double vect[1000];
    for (unsigned i = 0; i < 1000; i++)
      vect[i] = 1.44 * i;

    auto wtxn = kv->beginWrite();

    collectionId = wtxn->putDataCollection(vect, 1000);

    //alternative: preallocate buffer from DB + access directly
    double *vect2;
    collectionId2 = wtxn->putDataCollection(&vect2, 1000);
    for (unsigned i = 0; i < 1000; i++)
      vect2[i] = 1.44 * i;

    //append another chunk using zero-copy access
    wtxn->appendDataCollection(collectionId2, &vect2, 1000);
    for (unsigned i = 0; i < 1000; i++)
      vect2[i] = 4.44 * i;
    wtxn->commit();
  }
  {
    //load saved collection arrays
    auto rtxn = kv->beginExclusiveRead();

    CollectionData<double>::Ptr cd1 = rtxn->getDataCollection<double>(collectionId, 2, 3);
    assert(cd1);
    double *data = cd1->data();

    double d0 = data[0];
    double d2 = data[2];
    assert(d0 == 1.44 * 2 && d2 == 1.44 * 4);

    CollectionData<double>::Ptr cd2 = rtxn->getDataCollection<double>(collectionId, 2, 100);
    assert(cd2);
    data = cd2->data();
    d0 = data[0];
    double d99 = data[99];
    assert(d0 == 1.44 * 2 && d99 == 1.44 * 101&& !cd2->isOwned());

    auto cd3 = rtxn->getDataCollection<double>(collectionId, 100, 50);
    data = cd3->data();
    assert(data[0] == 1.44 * 100 && data[49] == 1.44 * 149&& !cd3->isOwned());

    CollectionData<double>::Ptr cd4 = rtxn->getDataCollection<double>(collectionId2, 0, 1000);
    data = cd4->data();
    assert(data[1] == 1.44 && data[999] == 1.44 * 999&& !cd4->isOwned());

    CollectionData<double>::Ptr cd5 = rtxn->getDataCollection<double>(collectionId2, 1000, 1000);
    data = cd5->data();
    assert(data[1] == 4.44 && data[999] == 4.44 * 999 && !cd5->isOwned());

    CollectionData<double>::Ptr cd6 = rtxn->getDataCollection<double>(collectionId2, 500, 1000);
    data = cd6->data();
    assert(data[0] == 1.44 * 500 && data[999] == 4.44 * 499 && cd6->isOwned());

    rtxn->end();
  }

  {
    //load saved collection arrays into preallocated storage
    auto rtxn = kv->beginRead();

    double d2_3[3];
    size_t r1 = rtxn->getDataCollection<double>(collectionId, 2, 3, d2_3);
    assert(r1 == 3);

    double d0 = d2_3[0];
    double d2 = d2_3[2];
    assert(d0 == 1.44 * 2 && d2 == 1.44 * 4);

    double d2_100[100];
    size_t r2 = rtxn->getDataCollection<double>(collectionId, 2, 100, d2_100);
    assert(r2 == 100);
    d0 = d2_100[0];
    double d99 = d2_100[99];
    assert(d0 == 1.44 * 2 && d99 == 1.44 * 101);

    double d100_50[50];
    auto r3 = rtxn->getDataCollection<double>(collectionId, 100, 50, d100_50);
    assert(r3 == 50);
    assert(d100_50[0] == 1.44 * 100 && d100_50[49] == 1.44 * 149);

    double d0_1000[1000];
    auto r4 = rtxn->getDataCollection<double>(collectionId2, 0, 1000, d0_1000);
    assert(r4 == 1000);
    assert(d0_1000[1] == 1.44 && d0_1000[999] == 1.44 * 999);

    double d1000_1000[1000];
    auto r5 = rtxn->getDataCollection<double>(collectionId2, 1000, 1000, d1000_1000);
    assert(r5 == 1000);
    assert(d1000_1000[1] == 4.44 && d1000_1000[999] == 4.44 * 999);

    double d500_1000[1000];
    auto r6 = rtxn->getDataCollection<double>(collectionId2, 500, 1000, d500_1000);
    assert(r6 == 1000);
    assert(d500_1000[0] == 1.44 * 500 && d500_1000[999] == 4.44 * 499);

    rtxn->end();
  }
}
//test persistent collection of scalar (primitve) values sub-array API
void testDataCollection2(KeyValueStore *kv)
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

    auto cd4 = rtxn->getDataCollection<long long>(collectionId2, 10, 50);
    long long *data2 = cd4->data();
    assert(data2[0] == -99999 * 10 && data2[49] == -99999 * 59);

    rtxn->end();
  }
  {
    //raw array API:
    long long darray[100];
    for(int i=0;i<100; i++) darray[i] = 555 * i;

    auto wtxn = kv->beginWrite();

    wtxn->appendDataCollection(collectionId2, darray, 100);

    wtxn->commit();
  }
  {
    auto rtxn = kv->beginExclusiveRead();

    auto cd = rtxn->getDataCollection<long long>(collectionId2, 190, 10);
    long long *data = cd->data();
    assert(data[0] == 555 * 90 && data[9] == 555 * 99);

    rtxn->end();
  }
}

void  testObjectPtrPropertyStorage(KeyValueStore *kv)
{
  auto sd = make_obj<lo::player::SourceDisplayConfig>(1, 2, false, 4, 5, 6, 7);

  lo::player::SourceInfo *si = new lo::player::SourceInfo(sd);

  auto wtxn = kv->beginWrite();
  ObjectKey key;
  wtxn->saveObject(*si, key);
  wtxn->commit();

  auto rtxn = kv->beginRead();
  lo::player::SourceInfo *si2 = rtxn->getObject<lo::player::SourceInfo>(key);
  rtxn->end();

  assert(si2 && si2->displayConfig && si2->displayConfig->sourceIndex == 1 && si2->displayConfig->attachedIndex == 2);

  delete si2;
  delete si;
}

void testAttachedCollection(KeyValueStore *kv)
{
  static const unsigned ATTACHED_COLLECTION_ID = 999;

  ObjectId vopId;
  {
    VariableSizeObjectPtr vop = kv::make_obj<VariableSizeObject>();
    vop->name = "Johnny";
    vop->number = 22;

    vector<FixedSizeObjectPtr> fops;
    for(int i=0; i<10; i++)
      fops.push_back(kv::make_obj<FixedSizeObject>(2 * i, 3 * i));

    auto tr = kv->beginWrite();
    vopId = tr->saveObject(vop);
    tr->putCollection(vop, ATTACHED_COLLECTION_ID, fops);
    tr->commit();
  }
  {
    auto tr = kv->beginRead();
    VariableSizeObjectPtr vop = tr->getObject<VariableSizeObject>(vopId);
    assert(vop);

    vector<FixedSizeObjectPtr> fops;
    tr->getCollection(vop, ATTACHED_COLLECTION_ID, fops);

    assert(fops.size() == 10);
    for(int i=0; i<10; i++)
      assert(fops[i]->number1 == 2 * i && fops[i]->number2 == 3 * i);

    tr->end();

    auto wtr = kv->beginWrite();
    wtr->deleteCollection<VariableSizeObject, FixedSizeObject>(vop, ATTACHED_COLLECTION_ID);
    wtr->commit();

    vector<FixedSizeObjectPtr> fops2;
    tr = kv->beginRead();
    tr->getCollection(vop, ATTACHED_COLLECTION_ID, fops2);
    tr->end();

    assert(fops2.empty());
  }
}

void testObjectVectorPropertyStorageEmbedded(KeyValueStore *kv)
{
  ObjectKey key;
  {
    SomethingWithEmbbededObjectVectors sov;
    sov.name = "sweov";

    sov.fsos.push_back(FixedSizeObject(1, 2));
    sov.fsos.push_back(FixedSizeObject(3, 4));
    sov.fsos.push_back(FixedSizeObject(5, 6));
    sov.fsos.push_back(FixedSizeObject(7, 8));

    sov.fsos2.push_back(FixedSizeObject2(1.11, 2.22));
    sov.fsos2.push_back(FixedSizeObject2(3.33, 4.44));
    sov.fsos2.push_back(FixedSizeObject2(5.55, 6.66));
    sov.fsos2.push_back(FixedSizeObject2(7.77, 8.88));

    sov.fsos3.push_back(FixedSizeObject2(1.11, 2.22));
    sov.fsos3.push_back(FixedSizeObject2(3.33, 4.44));
    sov.fsos3.push_back(FixedSizeObject2(5.55, 6.66));
    sov.fsos3.push_back(FixedSizeObject2(7.77, 8.88));

    sov.vsos.push_back(VariableSizeObject(1, "Frankfurt"));
    sov.vsos.push_back(VariableSizeObject(3, "München"));
    sov.vsos.push_back(VariableSizeObject(5, "Regensburg"));

    SomethingWithEmbeddedObjects so(20, 21, 20, "so1.vso");
    sov.sweos.push_back(so);

    SomethingWithEmbeddedObjects so2(30, 31, 30, "so2.vso");
    sov.sweos.push_back(so2);

    auto wtxn = kv->beginWrite();
    wtxn->saveObject(sov, key);
    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    SomethingWithEmbbededObjectVectors *loaded = rtxn->getObject<SomethingWithEmbbededObjectVectors>(key);

    assert(loaded && loaded->name == "sweov" && loaded->fsos.size() == 4 \
           && loaded->fsos[0].number1 == 1 \
           && loaded->fsos[0].number2 == 2 \
           && loaded->fsos[1].number1 == 3 \
           && loaded->fsos[3].number1 == 7 \
           && loaded->fsos[3].number2 == 8);

    assert(loaded && loaded->name == "sweov" && loaded->fsos2.size() == 4 \
           && loaded->fsos2[0].number1 == 1.11 \
           && loaded->fsos2[0].number2 == 2.22 \
           && loaded->fsos2[1].number1 == 3.33 \
           && loaded->fsos2[3].number1 == 7.77 \
           && loaded->fsos2[3].number2 == 8.88);

    assert(loaded && loaded->name == "sweov" && loaded->fsos3.size() == 4 \
           && loaded->fsos3[0].number1 == 1.11 \
           && loaded->fsos3[0].number2 == 2.22 \
           && loaded->fsos3[1].number1 == 3.33 \
           && loaded->fsos3[3].number1 == 7.77 \
           && loaded->fsos3[3].number2 == 8.88);

    assert(loaded->vsos.size() ==  3 && loaded->vsos[0].name == "Frankfurt" && loaded->vsos[2].name == "Regensburg");

    assert(loaded->sweos.size() == 2 && loaded->sweos[0].fso.number1 == 20 && loaded->sweos[1].vso.name == "so2.vso");

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

    auto coll = rtxn->getDataCollection<unsigned>(collectionId, 2000, 10);
    unsigned * data = coll->data();

    assert(data[0] == 2000 && data[9] == 2009);

    coll = rtxn->getDataCollection<unsigned>(collectionId, 900000, 11);
    data = coll->data();

    assert(data[0] == 900000 && data[10] == 900010);

    coll = rtxn->getDataCollection<unsigned>(collectionId, 1000000-10, 10);
    data = coll->data();

    assert(data[0] == 999990 && data[9] == 999999);

    coll = rtxn->getDataCollection<unsigned>(collectionId, 1001000-10, 10);
    data = coll->data();

    assert(data[0] == 1000990 && data[9] == 1000999);

    rtxn->end();
  }
}

void testDelete(KeyValueStore *kv, unsigned expectedOverlays)
{
  ObjectKey siKey, otKey;
  {
    player::SourceInfo si;
    si.sourceIndex = 123456789;

    TestRectangularOverlayPtr ro = kv::make_obj<TestRectangularOverlay>();
    ro->name = "testDelete.FlexisOverlay";
    si.userOverlays.push_back(ro);

    TimeCodeOverlayPtr  to = kv::make_obj<TimeCodeOverlay>();
    to->name = "testDelete.FlexisOverlay";
    si.userOverlays.push_back(to);

    RefCountingTest ot;
    ot.vso = kv::make_obj<VariableSizeObject>(1, "testDelete.VariableSizeObject");
    ot.fso = kv::make_obj<FixedSizeObject>(99, 99);
    ot.fso_vect.push_back(kv::make_obj<FixedSizeObject>(99, 99));

    auto txn = kv->beginWrite();

    txn->saveObject(si, siKey);
    txn->saveObject(ot, otKey);

    txn->commit();
  }
  {
    auto txn = kv->beginRead();

    unsigned si = countInstances<player::SourceInfo>(
        txn, [](shared_ptr<player::SourceInfo> s)->bool {return s->sourceIndex == 123456789;});
    assert(si == 1);
    unsigned ov = countInstances<IFlexisOverlay>(
        txn, [](shared_ptr<IFlexisOverlay> o)->bool {return o->name == "testDelete.FlexisOverlay";});
    assert(ov == 2);

    unsigned ot = countInstances<RefCountingTest>(
        txn, [](shared_ptr<RefCountingTest> o)->bool {return o->vso->name == "testDelete.VariableSizeObject";});
    assert(ot == 1);
    unsigned vo = countInstances<VariableSizeObject>(
        txn, [](shared_ptr<VariableSizeObject> v)->bool {return v->name == "testDelete.VariableSizeObject";});
    assert(vo == 1);
    unsigned fo = countInstances<FixedSizeObject>(
        txn, [](shared_ptr<FixedSizeObject> v)->bool {return v->number1 == 99;});
    assert(fo == 2);

    txn->end();
  }
  {
    auto txn = kv->beginWrite();

    txn->deleteObject<player::SourceInfo>(siKey);
    txn->deleteObject<RefCountingTest>(otKey);

    txn->commit();
  }
  {
    auto txn = kv->beginWrite();

    unsigned si = countInstances<player::SourceInfo>(
        txn, [](shared_ptr<player::SourceInfo> s)->bool {return s->sourceIndex == 123456789;});
    assert(si == 0);
    vector<IFlexisOverlayPtr> ov = getInstances<IFlexisOverlay>(
        txn, [](shared_ptr<IFlexisOverlay> o)->bool {return o->name == "testDelete.FlexisOverlay";});
    assert(ov.size() == expectedOverlays);
    for(auto &o : ov) txn->deleteObject(o);

    unsigned ot = countInstances<RefCountingTest>(
        txn, [](shared_ptr<RefCountingTest> o)->bool {return o->vso->name == "testDelete.VariableSizeObject";});
    assert(ot == 0);
    unsigned vo = countInstances<VariableSizeObject>(
        txn, [](shared_ptr<VariableSizeObject> v)->bool {return v->name == "testDelete.VariableSizeObject";});
    assert(vo == 0);
    unsigned fo = countInstances<FixedSizeObject>(
        txn, [](shared_ptr<FixedSizeObject> v)->bool {return v->number1 == 99;});
    assert(fo == 0);

    txn->commit();
  }
}

void testUpdate(KeyValueStore *kv, unsigned expectedOverlays=1)
{
  ObjectKey siKey, rtKey;
  {
    player::SourceInfo si;
    si.sourceIndex = 2233445;
    si.displayConfig = kv::make_obj<player::SourceDisplayConfig>(1);
    TestRectangularOverlayPtr ro = kv::make_obj<TestRectangularOverlay>();
    ro->name = "testUpdate.FlexisOverlay";
    si.userOverlays.push_back(ro);

    TimeCodeOverlayPtr  to = kv::make_obj<TimeCodeOverlay>();
    to->name = "testUpdate.FlexisOverlay";
    si.userOverlays.push_back(to);

    RefCountingTest ot;
    ot.fso_vect.push_back(kv::make_obj<FixedSizeObject>(11, 11));
    ot.fso_vect.push_back(kv::make_obj<FixedSizeObject>(22, 22));

    auto txn = kv->beginWrite();

    txn->saveObject(si, siKey);
    txn->saveObject(ot, rtKey);

    txn->commit();
  }
  {
    auto txn = kv->beginWrite();

    auto si = txn->getObject<player::SourceInfo>(siKey);
    assert(si->userOverlays.size() == 2);
    assert(si->displayConfig->sourceIndex == 1);
    auto ot = txn->getObject<RefCountingTest>(rtKey);
    assert(ot->fso_vect.size() == 2);

    si->userOverlays.erase(si->userOverlays.begin());
    ot->fso_vect.pop_back();
    si->displayConfig = nullptr;

    txn->saveObject(*si, siKey);
    txn->saveObject(*ot, rtKey);

    txn->commit();
    delete ot;
    delete si;
  }
  {
    auto txn = kv->beginWrite();

    auto si = txn->getObject<player::SourceInfo>(siKey);
    assert(si->userOverlays.size() == 1);
    assert(si->displayConfig == nullptr);
    auto ot = txn->getObject<RefCountingTest>(rtKey);
    assert(ot->fso_vect.size() == 1);

    vector<IFlexisOverlayPtr> cov = getInstances<IFlexisOverlay>(
        txn, [](shared_ptr<IFlexisOverlay> o)->bool {return o->name == "testUpdate.FlexisOverlay";});
    assert(cov.size() == expectedOverlays);
    for(auto &o : cov) txn->deleteObject(o);

    unsigned vo = countInstances<player::SourceDisplayConfig>(
        txn, [](shared_ptr<player::SourceDisplayConfig> v)->bool {return v->sourceIndex == 1;});
    assert(vo == 0);
    unsigned fo = countInstances<FixedSizeObject>(
        txn, [](shared_ptr<FixedSizeObject> v)->bool {return v->number1 == 11 || v->number1 == 22;});
    assert(fo == 1);

    txn->commit();
    delete ot;
    delete si;
  }
  auto txn = kv->beginWrite();
  txn->deleteObject<player::SourceInfo>(siKey);
  txn->deleteObject<RefCountingTest>(rtKey);
  txn->commit();
}

void testRefCounting(KeyValueStore *kv, unsigned expectedCount=0)
{
  ObjectKey siKey;
  {
    player::SourceInfo si;
    si.sourceIndex = 8887766;
    si.displayConfig = kv::make_obj<player::SourceDisplayConfig>(1);

    TestRectangularOverlayPtr ro = kv::make_obj<TestRectangularOverlay>();
    ro->name = "testRefCounting.TestRectangularOverlay";

    TimeCodeOverlayPtr  to = kv::make_obj<TimeCodeOverlay>();
    to->name = "testRefCounting.TimeCodeOverlay";

    si.userOverlays.push_back(to);
    si.userOverlays.push_back(ro);

    auto txn = kv->beginWrite();

    txn->saveObject(ro);        //owned by application + userOverlays
    txn->saveObject(to, false); //owned by userOverlays only

    txn->saveObject(si, siKey);

    txn->commit();
  }
  {
    auto txn = kv->beginWrite();

    auto si = txn->getObject<player::SourceInfo>(siKey);

    assert(si->userOverlays.size() == 2);
    assert(si->displayConfig->sourceIndex == 1);

    txn->deleteObject<player::SourceInfo>(siKey);

    txn->commit();
    delete si;
  }
  {
    auto txn = kv->beginWrite();

    vector<TestRectangularOverlayPtr> rov = getInstances<TestRectangularOverlay>(
        txn, [](shared_ptr<TestRectangularOverlay> o)->bool {return o->name == "testRefCounting.TestRectangularOverlay";});
    assert(rov.size() == expectedCount);
    for(auto &r : rov) txn->deleteObject(r);

    vector<TimeCodeOverlayPtr> tov = getInstances<TimeCodeOverlay>(
        txn, [](shared_ptr<TimeCodeOverlay> o)->bool {return o->name == "testRefCounting.TimeCodeOverlay";});
    assert(tov.size() == expectedCount);
    for(auto &t : tov) txn->deleteObject(t);

    txn->commit();
  }
}

void testObjectIterProperty(KeyValueStore *kv)
{
  ObjectKey key;

  {
    auto wtxn = kv->beginWrite();

    SomethingWithAnObjectIter soi;
    wtxn->initMember(soi, PROPERTY(SomethingWithAnObjectIter, history)); //ensure interator member is initialized

    for(int i=0; i<200; i++)
      soi.history->addHistoryValue(FixedSizeObjectPtr(new FixedSizeObject(i, i+1)));

    wtxn->writeCollections();  //must write collections first, as the last chunk may still be dangling
    wtxn->saveObject(soi, key); //must save explicitly only because collection was created lazily
    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    SomethingWithAnObjectIter *soi = rtxn->getObject<SomethingWithAnObjectIter>(key);

    assert(soi->history->size() == 200);

    //do a little sparse positioning, forward..
    for(int i=0; i<soi->history->size(); i+=2) {
      auto fso = soi->history->getHistoryValue(i);
      assert(fso->number1 == i && fso->number2 == i+1);
    }
    //.. and backward
    for(int i=soi->history->size()-1; i>=0; i-=2) {
      auto fso = soi->history->getHistoryValue(i);
      assert(fso->number1 == i && fso->number2 == i+1);
    }

    //peek randomly..
    auto fso = soi->history->getHistoryValue(133);
    assert(fso->number1 == 133 && fso->number2 == 134);
    fso = soi->history->getHistoryValue(12);
    assert(fso->number1 == 12 && fso->number2 == 13);

    delete soi;
  }
}

void testValueIterProperty(KeyValueStore *kv)
{
  ObjectKey key;

  {
    auto wtxn = kv->beginWrite();

    SomethingWithAValueIter sv;
    wtxn->initMember(sv, PROPERTY(SomethingWithAValueIter, values)); //ensure interator member is initialized

    for(int i=0; i<200; i++) {
      stringstream ss;
      ss << "value_" << i;
      sv.values->add(ss.str());
    }

    wtxn->writeCollections();  //must write collections first, as the last chunk may still be dangling
    wtxn->saveObject(sv, key); //must save explicitly only because collection was created lazily
    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    SomethingWithAValueIter *sv = rtxn->getObject<SomethingWithAValueIter>(key);

    assert(sv->values->count() == 200);

    //do a little sparse positioning, forward..
    for(int i=0; i<sv->values->count(); i+=2) {
      auto val = sv->values->get(i);
      stringstream ss; ss << "value_" << i;
      assert(val == ss.str());
    }
    //.. and backward
    for(int i=sv->values->count()-1; i>=0; i-=2) {
      auto val = sv->values->get(i);
      stringstream ss; ss << "value_" << i;
      assert(val == ss.str());
    }

    //peek randomly..
    auto val = sv->values->get(133);
    assert(val == "value_133");

    val = sv->values->get(12);
    assert(val == "value_12");

    delete sv;
  }
}

void testDataIterProperty(KeyValueStore *kv)
{
  ObjectKey key;

  {
    auto wtxn = kv->beginWrite();

    SomethingWithAValueIter sv;
    wtxn->initMember(sv, PROPERTY(SomethingWithAValueIter, datas)); //ensure interator member is initialized

    long long lbuf[100];
    for(int i=0; i<200; i++) {
      for(int j=0; j<100; j++) lbuf[j] = i;
      sv.datas->add(lbuf, 100);
    }

    wtxn->writeCollections();  //must write collections first, as the last chunk may still be dangling
    wtxn->saveObject(sv, key); //must save explicitly only because collection was created lazily
    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    SomethingWithAValueIter *sv = rtxn->getObject<SomethingWithAValueIter>(key);

    assert(sv->datas->size() == 20000);

    //do a little sparse positioning, forward..
    for(int i=0; i<sv->values->count(); i+=2) {
      long long *lbuf = nullptr;
      bool success = sv->datas->get(i * 100, lbuf, 100);
      assert(success && lbuf[0] == i && lbuf[99] == i);
    }
    //.. and backward
    for(int i=sv->values->count()-1; i>=0; i-=2) {
      long long *lbuf = nullptr;
      bool success = sv->datas->get(i * 100, lbuf, 100);
      assert(success && lbuf[0] == i && lbuf[99] == i);
    }

    //peek randomly..
    long long *lbuf = nullptr;
    bool success = sv->datas->get(133 * 100, lbuf, 100);
    assert(success && lbuf[0] == 133 && lbuf[99] == 133);

    //we must nullify the pointer so the API will set it to DB memory
    lbuf = nullptr;
    success = sv->datas->get(12 * 100, lbuf, 100);
    assert(success && lbuf[0] == 12 && lbuf[99] == 12);

    delete sv;
  }
}

ObjectKey setupTestCompatibleDatabase(KeyValueStore *kv)
{
  ObjectKey key;
  {
    auto wtxn = kv->beginWrite();

    Wonderful w = Wonderful();
    w.abstractsEmbedded.push_back(kv::make_obj<SomethingConcrete1>("Hans", "da oide Hans"));
    w.abstractsEmbedded.push_back(kv::make_obj<SomethingConcrete2>("Otto", 33));

    w.virtualsEmbedded.push_back(kv::make_obj<SomethingVirtual>(1, "Gabi"));
    w.virtualsEmbedded.push_back(kv::make_obj<SomethingVirtual1>(2, "Girlande", "Köchin"));
    w.virtualsEmbedded.push_back(kv::make_obj<SomethingVirtual2>(3, "Maria", "Stricken"));

    w.virtualsPointers.push_back(kv::make_obj<SomethingVirtual>(1, "Rosine"));
    w.virtualsPointers.push_back(kv::make_obj<SomethingVirtual1>(2, "Methode", "Köchin"));
    w.virtualsPointers.push_back(kv::make_obj<SomethingVirtual2>(3, "Fontäne", "Stricken"));

    w.embeddedVirtual1 = kv::make_obj<SomethingVirtual1>(4, "Franzine", "Pfarrerin");
    w.embeddedVirtual2 = kv::make_obj<SomethingVirtual2>(5, "Fontäne", "Stricken");

    w.toplevelVirtual1 = kv::make_obj<SomethingVirtual1>(4, "Karl", "der Große");
    w.toplevelVirtual2 = kv::make_obj<SomethingVirtual2>(5, "Siegfried", "Drachentöten");

    wtxn->saveObject(w, key);

    //we need to kick virtualsLazy separately
    w.virtualsLazy.push_back(kv::make_obj<SomethingVirtual>(1, "Albrecht"));
    w.virtualsLazy.push_back(kv::make_obj<SomethingVirtual1>(2, "Hannes", "Anwalt"));
    w.virtualsLazy.push_back(kv::make_obj<SomethingVirtual2>(3, "Friedrich", "der Große"));
    wtxn->updateMember(key, w, PROPERTY(Wonderful, virtualsLazy));

    wtxn->commit();
  }
  {
    auto rtxn = kv->beginRead();

    auto loaded = rtxn->getObject<Wonderful>(key.objectId);

    assert(loaded->abstractsEmbedded.size() == 2 && loaded->virtualsEmbedded.size() == 3 && loaded->virtualsPointers.size() == 3);

    rtxn->end();
  }
  return key;
}

#define IS_TYPE(__val, __cls) typeid(__val) == typeid(__cls)

void testCompatibleDatabase(ObjectKey key)
{
  KeyValueStore *kv = lmdb::KeyValueStore::Factory{0, ".", "test"};

  //need to cleanup static data for test only
  ClassTraits<SomethingAbstract>::traits_data(kv->id).classId = 0;
  ClassTraits<SomethingAbstract>::traits_info->subs.clear();
  ClassTraits<SomethingAbstract>::traits_initialized = false;
  ClassTraits<SomethingConcrete1>::traits_data(kv->id).classId = 0;
  ClassTraits<SomethingConcrete1>::traits_initialized = false;
  ClassTraits<SomethingConcrete2>::traits_data(kv->id).classId = 0;
  ClassTraits<SomethingConcrete2>::traits_initialized = false;
  ClassTraits<SomethingVirtual>::traits_data(kv->id).classId = 0;
  ClassTraits<SomethingVirtual>::traits_info->subs.clear();
  ClassTraits<SomethingVirtual>::traits_initialized = false;
  ClassTraits<SomethingVirtual1>::traits_data(kv->id).classId = 0;
  ClassTraits<SomethingVirtual1>::traits_initialized = false;
  ClassTraits<SomethingVirtual2>::traits_data(kv->id).classId = 0;
  ClassTraits<SomethingVirtual2>::traits_info->subs.clear();
  ClassTraits<SomethingVirtual2>::traits_initialized = false;
  ClassTraits<SomethingVirtual3>::traits_data(kv->id).classId = 0;
  ClassTraits<SomethingVirtual3>::traits_initialized = false;

  kv->putSchema<SomethingAbstract, SomethingVirtual, SomethingVirtual2, Wonderful>();
  kv->registerSubstitute<SomethingVirtual,UnknownVirtual>(); //substitute for missing SomethingVirtual1

  {
    //see how embedded and non-embedded relationships behave
    auto rtxn = kv->beginRead();

    auto loaded = rtxn->getObject<Wonderful>(key.objectId);

    assert(loaded);
    assert(loaded->abstractsEmbedded.empty() && loaded->virtualsEmbedded.size() == 3 && loaded->virtualsPointers.size() == 3);
    assert(loaded->virtualsEmbedded[0]->name == "Gabi" && loaded->virtualsEmbedded[1]->name == "Girlande" && loaded->virtualsEmbedded[1]->unknown && loaded->virtualsEmbedded[2]->name == "Maria");
    assert(loaded->virtualsPointers[0]->name == "Rosine" && loaded->virtualsPointers[1]->name == "Methode" && loaded->virtualsPointers[1]->unknown && loaded->virtualsPointers[2]->name == "Fontäne");

    assert(loaded->embeddedVirtual1->name == "Franzine" && IS_TYPE(*loaded->embeddedVirtual1, UnknownVirtual));
    assert(loaded->embeddedVirtual2->name == "Fontäne");

    assert(loaded->toplevelVirtual1->name == "Karl" && IS_TYPE(*loaded->toplevelVirtual1, UnknownVirtual));
    assert(loaded->toplevelVirtual2->name == "Siegfried");

    rtxn->end();
  }
  {
    //see how it goes with class cursors
    auto rtxn = kv->beginRead();

    unsigned count = 0;
    auto cursor = rtxn->openCursor<SomethingVirtual>();
    while(!cursor->atEnd()) {
      count++;
      auto sv = cursor->get();
      sv->sayhello(); cout << endl;
      cursor->next();
    }

    //substitutes don't work with class cursors, we get the classes that are available.
    //that's 2 from w.virtualsPointers, 1 from w.toplevelVirtual2, 2 from w.virtualsLazy, and 3 top-level, created in
    //testClassCursor. Objects from embedded collections don't count because they don't have a top-level key
    assert(count == 8);
    rtxn->end();
  }
  {
    //test lazy collection cursor
    auto rtxn = kv->beginRead();

    auto loaded = rtxn->getObject<Wonderful>(key.objectId);

    unsigned count = 0, unknownCount=0;
    auto cursor = rtxn->openCursor<Wonderful, SomethingVirtual>(loaded, PROPERTY_ID(Wonderful, virtualsLazy));
    while(!cursor->atEnd()) {
      count++;
      auto sv = cursor->get();
      if(sv->unknown) unknownCount++;
      sv->sayhello(); cout << endl;
      cursor->next();
    }

    assert(count == 3 && unknownCount == 1);
    rtxn->end();
  }

  delete kv;
}

void test_classupdate();

using namespace lightningobjects::valuetest;

int main()
{
  //test_classupdate();

  KeyValueStore *kv = lmdb::KeyValueStore::Factory{0, ".", "test"};
#if 1

  //be a little nasty with type registrations
  kv->putTypes<ValueTest>();
  kv->putTypes<ValueTest, ValueTest2>();
  kv->putTypes<ValueTest2, ValueTest>();
  kv->putTypes<ValueTest3>();
  //this will create a second chunk
  kv->putTypes<ValueTest4, ValueTest5, ValueTest6, ValueTest7, ValueTest8, ValueTest9, ValueTest10, ValueTest11, ValueTest12>();
  //now validate once more
  kv->putTypes<ValueTest, ValueTest2, ValueTest3, ValueTest8, ValueTest4, ValueTest5, ValueTest6, ValueTest7, ValueTest12>();

  cout << "ValueTest::id: " << TypeTraits<ValueTest>::id << endl;
  cout << "ValueTest2::id: " << TypeTraits<ValueTest2>::id << endl;
  cout << "ValueTest3::id: " << TypeTraits<ValueTest3>::id << endl;
  cout << "ValueTest4::id: " << TypeTraits<ValueTest4>::id << endl;
  cout << "ValueTest5::id: " << TypeTraits<ValueTest5>::id << endl;
  cout << "ValueTest6::id: " << TypeTraits<ValueTest6>::id << endl;
  cout << "ValueTest7::id: " << TypeTraits<ValueTest7>::id << endl;
  cout << "ValueTest8::id: " << TypeTraits<ValueTest8>::id << endl;

  kv->putSchema<ObjectPropertyTest,
      RefCountingTest,
      FixedSizeObject,
      FixedSizeObject2,
      VariableSizeObject,
      SomethingWithEmbeddedObjects,
      SomethingWithEmbbededObjectVectors,
      SomethingWithAnObjectIter,
      SomethingWithAValueIter,
      SomethingWithAllValueKeyedProperties,
      SomethingAbstract,
      SomethingConcrete1,
      SomethingConcrete2,
      SomethingVirtual,
      SomethingVirtual1,
      SomethingVirtual2,
      SomethingVirtual3,
      Wonderful>();

  kv->putSchema<Colored2DPoint,
      ColoredPolygon,
      player::SourceDisplayConfig,
      player::SourceInfo,
      lo::Overlays::IFlexisOverlay,
      lo::Overlays::TestRectangularOverlay,
      lo::Overlays::TimeCodeOverlay,
      OtherThing,
      OtherThingA,
      OtherThingB,
      SomethingWithALazyVector>();

  kv->setRefCounting<VariableSizeObject>();
  kv->setRefCounting<FixedSizeObject>();
  kv->setRefCounting<player::SourceDisplayConfig>();

  testUpdate(kv, 2);
  testDelete(kv, 2);
  testRefCounting(kv, 1);

  kv->setRefCounting<IFlexisOverlay>();
  testUpdate(kv, 1);
  testDelete(kv, 0);
  testRefCounting(kv, 0);

  testAttachedCollection(kv);

  testKeyedProperties(kv);
  testColored2DPoint(kv);
  testColoredPolygon(kv);
  testColoredPolygonIterator(kv);
  testFlexisProperties(kv);
  testLazyPolymorphicCursor(kv);
  benchObjectCollection(kv);
  benchValueCollection(kv);
  testObjectPtrPropertyStorage(kv);
  testValueVectorProperty(kv);
  testDataCollection1(kv);
  testDataCollection2(kv);
  testGrowDatabase(kv);
  testObjectVectorPropertyStorageEmbedded(kv);
  testObjectIterProperty(kv);
  testValueIterProperty(kv);
  testDataIterProperty(kv);
  testClassCursor(kv);
  testObjectMappings(kv);
  testCustomValueTypes(kv);

  ObjectKey key = setupTestCompatibleDatabase(kv);
  delete kv;

  testCompatibleDatabase(key);
#endif

  return 0;
}

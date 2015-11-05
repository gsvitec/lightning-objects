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
    ColoredPolygon *loaded = cursor->get();
    if(loaded) {
      cout << "loaded ColoredPolygon visible: " << loaded->visible << " pts: " << loaded->pts.size() << endl;
      delete loaded;
    }
  }

  for(auto cursor = rtxn->openCursor<Colored2DPoint>(); !cursor->atEnd(); ++(*cursor)) {
    Colored2DPoint *loaded = cursor->get();
    if(loaded) {
      cout << "loaded Colored2DPoint x: " << loaded->x << " y: " << loaded->y << endl;
      delete loaded;
    }
  }

  rtxn->abort();
}

void testPolymorphism(KeyValueStore *kv)
{
  flexis::player::SourceInfo si;
  IFlexisOverlayPtr ro(new RectangularOverlay());
  IFlexisOverlayPtr to(new TimeCodeOverlay());

  si.userOverlays.push_back(to);
  si.userOverlays.push_back(ro);

  auto wtxn = kv->beginWrite();
  ObjectId id = wtxn->putObject(si);
  wtxn->commit();

  player::SourceInfo *loaded;
  auto rtxn = kv->beginRead();
  loaded = rtxn->getObject<player::SourceInfo>(id);
  rtxn->abort();

  assert(loaded && loaded->userOverlays.size() == 2);
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

    OtherThingPtr ptra(new OtherThingA("Hans"));
    sv.otherThings.push_back(ptra);

    OtherThingPtr ptrb(new OtherThingB("Otto"));
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
      OtherThing *ot = cursor->get();
      cout << ot->sayhello() << " my name is " << ot->name << " my number is " << ot->dvalue << endl;
      delete ot;
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
        const char *name, *buf=nullptr;
        double *dval;

        //we're passing in buf, so that only the first call will go to the store
        cursor->get(PROPERTY_ID(OtherThing, name), &name, &buf);
        //buf is set now, so this call will simply return a pointer into buf
        cursor->get(PROPERTY_ID(OtherThing, dvalue), (const char **)&dval, &buf);

        cout << name << " dvalue: " << *dval << endl;
      }
    }
    assert(count == 2);
    rtxn->abort();
    delete loaded;
  }
}

void testPersistentCollection(KeyValueStore *kv)
{
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

    auto wtxn = kv->beginWrite();

    collectionId = wtxn->putCollection(vect, 4);

    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();

    vector<OtherThingPtr> loaded = rtxn->getCollection<OtherThing>(collectionId);
    assert(loaded.size() == 10);

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
    assert(count == 10);

    rtxn->abort();
  }
  {
    //append more test data
    vector<OtherThingPtr> vect;

    vect.push_back(OtherThingPtr(new OtherThingA("Marcel")));
    vect.push_back(OtherThingPtr(new OtherThingB("Marianne")));
    vect.push_back(OtherThingPtr(new OtherThingB("Nicolas")));

    auto wtxn = kv->beginWrite();

    wtxn->appendCollection(collectionId, vect, 4);

    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();
    vector<OtherThingPtr> loaded = rtxn->getCollection<OtherThing>(collectionId);
    assert(loaded.size() == 13);
    rtxn->abort();
  }
  {
    //use write cursor to append more test data
    auto wtxn = kv->beginWrite();

    auto writer = wtxn->openWriter<OtherThing>(collectionId, 4);
    for(int i=0; i<20; i++) {
      stringstream ss;
      ss << "Test_" << i;
      writer->append(OtherThingPtr(new OtherThingB(ss.str().c_str())));
    }
    writer->close();
    wtxn->commit();
  }
  {
    //load saved collection
    auto rtxn = kv->beginRead();
    vector<OtherThingPtr> loaded = rtxn->getCollection<OtherThing>(collectionId);

    cout << "APPEND TEST:" << endl;
    for(auto ot : loaded)
      cout << ot->sayhello() << " my name is " << ot->name << " my number is " << ot->dvalue << endl;

    assert(loaded.size() == 33);
    rtxn->abort();
  }
}

int main()
{
  KeyValueStore *kv = lmdb::KeyValueStore::Factory{".", "test"};

  kv->registerType<Colored2DPoint>();
  kv->registerType<ColoredPolygon>();

  kv->registerType<player::SourceInfo>();
  kv->registerType<flexis::RectangularOverlay>();
  kv->registerType<flexis::TimeCodeOverlay>();

  kv->registerType<OtherThingA>();
  kv->registerType<OtherThingB>();
  kv->registerType<SomethingWithALazyVector>();

  testColored2DPoint(kv);
  testColoredPolygon(kv);
  testColoredPolygonIterator(kv);
  testPolymorphism(kv);
  testLazyPolymorphicCursor(kv);
  testPersistentCollection(kv);

  /*auto rtxn = kv->beginRead();

  for(auto cursor = rtxn->openCursor<SomethingWithALazyVector>(); !cursor->atEnd(); ++(*cursor)) {
    ObjectId id;
    SomethingWithALazyVector *loaded = loaded = cursor->get(&id);
    rtxn->loadMember(id, *loaded, PROPERTY_ID(SomethingWithALazyVector, otherThings));

    cout << id << ": " << loaded->name << endl;
    for(auto &o : loaded->otherThings)
      cout << o->sayhello() << " my name is " << o->name << " my number is " << o->dvalue << endl;
  }

  rtxn->abort();*/

  delete kv;
  return 0;
}

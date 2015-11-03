//
// Created by cse on 10/10/15.
//

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
}

void testColoredPolygonIterator(KeyValueStore *kv)
{
  auto rtxn = kv->beginRead();

  for(auto cursor = rtxn->openCursor<ColoredPolygon>(); !cursor->atEnd(); ++(*cursor)) {
    ColoredPolygon *loaded = loaded = cursor->get();
    if(loaded) {
      cout << "loaded ColoredPolygon visible: " << loaded->visible << " pts: " << loaded->pts.size() << endl;
    }
  }

  for(auto cursor = rtxn->openCursor<Colored2DPoint>(); !cursor->atEnd(); ++(*cursor)) {
    Colored2DPoint *loaded = loaded = cursor->get();
    if(loaded) {
      cout << "loaded Colored2DPoint x: " << loaded->x << " y: " << loaded->y << endl;
    }
  }

  rtxn->abort();
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
  cout << "sv_id " << sv_id << endl;
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
    }
    assert(count == 2);

    rtxn->abort();
  }
  {
    //test data-only access (no object instantiation, read into mapped memory)
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

        cursor->get(PROPERTY_ID(OtherThing, name), &name, &buf);
        cursor->get(PROPERTY_ID(OtherThing, dvalue), (const char **)&dval, &buf);
        cout << name << " dvalue: " << *dval << endl;
      }
    }
    assert(count == 2);
    rtxn->abort();
  }
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
}

std::shared_ptr<Colored2DPoint> test(std::shared_ptr<Colored2DPoint> p) {
  std::shared_ptr<Colored2DPoint> p2 = p;
  return p2;
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

  return 0;
}

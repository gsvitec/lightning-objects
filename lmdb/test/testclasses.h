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

#ifndef FLEXIS_TESTCLASSES_H
#define FLEXIS_TESTCLASSES_H

#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <kvstore.h>

#ifdef TESTCLASSES_IMPL
#include <traits_impl.h>
#else
#include <traits_decl.h>
#endif

using namespace std;

namespace flexis {

namespace Overlays {

template <class T> class ObjectHistory {

public:
  virtual size_t size() = 0;
  virtual shared_ptr<T> getHistoryValue(uint64_t bufferPos) = 0;
  virtual void addHistoryValue(shared_ptr<T> val) = 0;
};
template <class T> using ObjectHistoryPtr = shared_ptr<ObjectHistory<T>>;

class Colored2DPoint {

public:

  float x;
  float y;

  float r;
  float g;
  float b;
  float a;

  Colored2DPoint() {}
  Colored2DPoint(float x_, float y_, float r_, float g_, float b_, float a_) : x(x_), y(y_), r(r_), g(g_), b(b_), a(a_)
  {}

  void set(float x_, float y_, float r_, float g_, float b_, float a_) {
    x = x_;
    y = y_;
    r = r_;
    g = g_;
    b = b_;
    a = a_;
  }
};

struct ColoredPolygon {
  vector<Colored2DPoint> pts;
  bool visible;
};

class IFlexisOverlay
{

public:
  long id;
  string name;

  bool userVisible;
  double opacity;
  long rangeIn;
  long rangeOut;
  bool selectable;

  virtual string type() const = 0;
};
using IFlexisOverlayPtr = shared_ptr<IFlexisOverlay>;

class TestRectangularOverlay : public IFlexisOverlay {

public:
  double ovlX;
  double ovlY;
  double ovlW;
  double ovlH;

  string type() const override {return "TestRectangularOverlay";}
};

using TestRectangularOverlayPtr = shared_ptr<TestRectangularOverlay>;


class TimeCodeOverlay : public IFlexisOverlay {

public:
  double ovlX;
  double ovlY;
  int fontSize;

  string text;

  string type() const override {return "TimeCodeOverlay";}
};
using TimeCodeOverlayPtr = shared_ptr<TimeCodeOverlay>;

} //Overlays

namespace player {

/**
 * describes a display configuration for a flexis source.
 */
struct SourceDisplayConfig
{
  using Ptr = std::shared_ptr<SourceDisplayConfig>;

  unsigned sourceIndex;
  unsigned attachedIndex;
  bool attached;
  unsigned window_x, window_y, window_width, window_height;

  SourceDisplayConfig()
      : sourceIndex(0), attachedIndex(0), attached(false), window_x(0), window_y(0), window_width(0), window_height(0)
  {}

  /**
   * create a display configuration from the given values.
   */
  SourceDisplayConfig(
      unsigned sourceIndex, unsigned attachedIndex, bool attached,
      unsigned window_x, unsigned window_y, unsigned window_width, unsigned window_height)
      : sourceIndex(sourceIndex), attachedIndex(attachedIndex), attached(attached),
        window_x(window_x), window_y(window_y), window_width(window_width), window_height(window_height)
  { }

  /**
   * create a default display configuration with attachedIndex set to the same value as sourceIndex
   */
  SourceDisplayConfig(unsigned sourceIndex)
      : sourceIndex(sourceIndex), attachedIndex(sourceIndex), attached(true), window_x(0), window_y(0), window_width(800),
        window_height(600)
  { }
};

/**
 * holds data about one flexis source
 */
struct SourceInfo {
  using Ptr = std::shared_ptr<SourceInfo>;

  unsigned sourceIndex;
  SourceDisplayConfig::Ptr displayConfig;
  std::vector<flexis::Overlays::IFlexisOverlayPtr> userOverlays;

  SourceInfo(unsigned sourceIndex = 0) : sourceIndex(sourceIndex) {}
  SourceInfo(SourceDisplayConfig::Ptr displayConfig)
      : sourceIndex(displayConfig->sourceIndex), displayConfig(displayConfig) {}
};

} //player

} //flexis

struct OtherThing {
  std::string name;
  double dvalue = 7.99765;

  OtherThing() {}
  OtherThing(std::string name) : name(name) {}
  virtual const char *sayhello() = 0;
  virtual ~OtherThing() {}
};
struct OtherThingA : public OtherThing {
  long lvalue = 99999999911111;
  std::vector<std::string> testnames;

  OtherThingA() : OtherThing() {}
  OtherThingA(std::string name) : OtherThing(name) {}
  const char *sayhello() override {return "i'm an OtherThingA";}
};
struct OtherThingB : public OtherThing {
  unsigned long long llvalue = 7777777272727272727;

  OtherThingB() : OtherThing() {}
  OtherThingB(std::string name) : OtherThing(name) {}
  const char *sayhello() override {return "i'm an OtherThingB";}
};

struct SomethingWithALazyVector
{
  std::string name;
  std::vector<std::shared_ptr<OtherThing>> otherThings;  
};

struct FixedSizeObject {
  unsigned objectId = 0; //for ObjectPropertyTest

  unsigned number1, number2;
  FixedSizeObject() : number1(0), number2(0) {}
  FixedSizeObject(unsigned number1, unsigned number2) : number1(number1), number2(number2) {}
};
using FixedSizeObjectPtr = std::shared_ptr<FixedSizeObject>;

struct FixedSizeObject2 {
  unsigned objectId = 0; //for ObjectPropertyTest

  double number1, number2;
  FixedSizeObject2() : number1(0), number2(0) {}
  FixedSizeObject2(double number1, double number2) : number1(number1), number2(number2) {}
};
using FixedSizeObject2Ptr = std::shared_ptr<FixedSizeObject2>;

struct VariableSizeObject {
  unsigned objectId = 0; //for ObjectPropertyTest

  unsigned number;
  std::string name;

  VariableSizeObject() : number(0), name("") {}
  VariableSizeObject(unsigned number, const char *nm) : number(number), name(nm) {}
};
using VariableSizeObjectPtr = std::shared_ptr<VariableSizeObject>;

struct ObjectPropertyTest
{
  unsigned id = 0;
  FixedSizeObject fso;
  VariableSizeObject vso;

  vector<FixedSizeObject> fso_vect;
  vector<VariableSizeObject> vso_vect;

  ObjectPropertyTest() {}
  ObjectPropertyTest(unsigned fso_num1, unsigned fso_num2, unsigned vso_num, const char *vso_name)
      : fso(fso_num1, fso_num2), vso(vso_num, vso_name) {}
};

struct RefCountingTest
{
  FixedSizeObjectPtr fso;
  VariableSizeObjectPtr vso;

  vector<FixedSizeObjectPtr> fso_vect;
  vector<VariableSizeObjectPtr> vso_vect;

  RefCountingTest() {}
};

struct SomethingWithEmbeddedObjects {
  FixedSizeObject fso;
  VariableSizeObject vso;

  SomethingWithEmbeddedObjects() {}
  SomethingWithEmbeddedObjects(unsigned nfso1, unsigned nfso2, unsigned nvso, const char *vso_name)
      : fso(nfso1, nfso2), vso(nvso, vso_name) {}
};
struct SomethingWithEmbbededObjectVectors
{
  std::string name;
  std::vector<SomethingWithEmbeddedObjects> sweos;
  std::vector<FixedSizeObject> fsos;
  std::vector<FixedSizeObject2> fsos2;
  std::vector<FixedSizeObject2> fsos3;
  std::vector<VariableSizeObject> vsos;
};

struct SomethingWithAllValueKeyedProperties
{
  std::string name;
  int counter;
  vector<int> numbers;
  set<string> children;
};

struct SomethingWithAnObjectIter
{
  std::string name;
  flexis::Overlays::ObjectHistoryPtr<FixedSizeObject> history;
};

template <typename V>
struct ValueIter {
  virtual void setCollectionId(unsigned id) = 0;
  virtual size_t count() = 0;
  virtual void add(V value) = 0;
  virtual V get(size_t position) = 0;
};
template <typename V>
struct DataIter {
  virtual size_t size() = 0;
  virtual void add(V *value, size_t size) = 0;
  virtual bool get(size_t position, V * &value, size_t size) = 0;
  virtual void release(V *) = 0;
};
struct SomethingWithAValueIter
{
  std::string name;
  std::shared_ptr<ValueIter<string>> values;
  std::shared_ptr<DataIter<long long>> datas;
};

struct SomethingAbstract {
  std::string name;
  SomethingAbstract(string n) : name(n) {}
  SomethingAbstract() {}
  virtual ~SomethingAbstract() {}
  virtual void sayhello() = 0;
};
struct SomethingConcrete1 : public SomethingAbstract {
  std::string description;
  SomethingConcrete1(string n, string d) : SomethingAbstract(n), description(d) {}
  SomethingConcrete1() {}

  void sayhello() override {cout << "hello, my name is " << name << " and " << description << endl;};
};
struct SomethingConcrete2 : public SomethingAbstract {
  unsigned age;
  SomethingConcrete2(string n, unsigned a) : SomethingAbstract(n), age(a) {}
  SomethingConcrete2() {}

  void sayhello() override {cout << "hello, my name is " << name << " and I am " << age << endl;};
};
struct SomethingVirtual {
  unsigned id;
  bool unknown;
  std::string name;
  SomethingVirtual(unsigned id, string n, bool unknown=false) : id(id), name(n), unknown(unknown) {}
  SomethingVirtual() : unknown(false) {}
  virtual ~SomethingVirtual() {}

  virtual void sayhello() {cout << id << ": hello, I'm " << name;}
};
struct SomethingVirtual1 : public SomethingVirtual {
  std::string profession;
  SomethingVirtual1(unsigned id, string n, string p) : SomethingVirtual(id, n), profession(p) {}
  SomethingVirtual1() {}

  void sayhello() override { SomethingVirtual::sayhello(); cout << " my profession is " << profession;}
};
struct SomethingVirtual2 : public SomethingVirtual {
  std::string hobby;
  SomethingVirtual2(unsigned id, string n, string h) : SomethingVirtual(id, n), hobby(h) {}
  SomethingVirtual2() {}

  void sayhello() override { SomethingVirtual::sayhello(); cout << " my hobby is " << hobby;}
};
struct SomethingVirtual3 : public SomethingVirtual2 {
  unsigned age;
  SomethingVirtual3(unsigned id, string n, string h, unsigned age) : SomethingVirtual2(id, n, h), age(age) {}
  SomethingVirtual3() {}

  void sayhello() override { SomethingVirtual2::sayhello(); cout << " and my age: " << age;}
};
struct UnknownVirtual : public SomethingVirtual {
  UnknownVirtual() : SomethingVirtual(0, "unknown", true) {}
  void sayhello() override { SomethingVirtual::sayhello(); cout << " I am only a substitute";}
};
struct Wonderful {
  shared_ptr<SomethingVirtual> embeddedVirtual1;
  shared_ptr<SomethingVirtual> toplevelVirtual1;
  shared_ptr<SomethingVirtual> embeddedVirtual2;
  shared_ptr<SomethingVirtual> toplevelVirtual2;

  vector<shared_ptr<SomethingAbstract>> abstractsEmbedded;
  vector<shared_ptr<SomethingVirtual>> virtualsEmbedded;
  vector<shared_ptr<SomethingVirtual>> virtualsPointers;
  vector<shared_ptr<SomethingVirtual>> virtualsLazy;
};

namespace flexis {
namespace persistence {
namespace kv {

template<typename T>
struct KVObjectHistoryImpl : public flexis::Overlays::ObjectHistory<T>, public IterPropertyBackend
{
  typename WriteTransaction::ObjectCollectionAppender<T>::Ptr appender;
  typename ObjectCollectionCursor<T>::Ptr loader;

  size_t size() override {
    return loader->count();
  }
  void init(WriteTransaction *tr) override
  {
    appender = tr->appendCollection<T>(m_collectionId);
  }

  void load(Transaction *tr) override
  {
    loader = tr->openCursor<T>(m_collectionId);
  }

  void addHistoryValue(shared_ptr<T> val) override {
    appender->put(val);
  }

  shared_ptr<T> getHistoryValue(uint64_t bufferPos) override {
    loader->seek(bufferPos);
    return shared_ptr<T>(loader->get());
  }
};

template <typename V>
struct ValueIterImpl : public ValueIter<V>, public IterPropertyBackend
{
  typename WriteTransaction::ValueCollectionAppender<V>::Ptr appender;
  typename ValueCollectionCursor<V>::Ptr loader;

  void setCollectionId(unsigned id) override {
    IterPropertyBackend::setCollectionId(id);
  }

  void init(WriteTransaction *tr) override {
    appender = tr->appendValueCollection<V>(m_collectionId);
  }

  void load(Transaction *tr) override {
    loader = tr->openValueCursor<V>(m_collectionId);
  }

  size_t count() override {
    return loader->count();
  }

  void add(V value) override {
    appender->put(value);
  }

  V get(size_t position) override {
    loader->seek(position);
    V s;
    loader->get(s);
    return s;
  }
};

template <typename V>
struct DataIterImpl : public DataIter<V>, public IterPropertyBackend {

  typename WriteTransaction::DataCollectionAppender<V>::Ptr appender;
  CollectionInfo *collectionInfo;
  Transaction *readTransaction;

  V *lastData = nullptr;
  bool owned;

  void init(WriteTransaction *tr) override {
    appender = tr->appendDataCollection<V>(m_collectionId);
  }

  void load(Transaction *tr) override {
    readTransaction = tr;
    collectionInfo = tr->getCollectionInfo(m_collectionId);
  }

  size_t size() override {
    return collectionInfo->count();
  }

  void add(V *value, size_t size) override {
    appender->put(value, size);
  }

  bool get(size_t position, V *&value, size_t size) override {
    size_t result = readTransaction->getDataCollection(m_collectionId, position, size, value, &owned);
    lastData = value;
    return result == size;
  }
  void release(V *data) override {
    if(data && data == lastData && owned) free(data);
  }
};

START_MAPPING(flexis::Overlays::Colored2DPoint, x, y, r, g, b, a)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, x)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, y)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, r)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, g)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, b)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, ValuePropertyEmbeddedAssign, float, a)
END_MAPPING(flexis::Overlays::Colored2DPoint)

START_MAPPING(flexis::Overlays::ColoredPolygon, pts, visible)
  MAPPED_PROP(flexis::Overlays::ColoredPolygon, ObjectVectorPropertyEmbeddedAssign, flexis::Overlays::Colored2DPoint, pts)
  MAPPED_PROP(flexis::Overlays::ColoredPolygon, ValuePropertyEmbeddedAssign, bool, visible)
END_MAPPING(flexis::Overlays::ColoredPolygon)

START_MAPPING_A(flexis::Overlays::IFlexisOverlay, name, userVisible, opacity, rangeIn, rangeOut)
  MAPPED_PROP(flexis::Overlays::IFlexisOverlay, ValuePropertyEmbeddedAssign, std::string, name)
  MAPPED_PROP(flexis::Overlays::IFlexisOverlay, ValuePropertyEmbeddedAssign, bool, userVisible)
  MAPPED_PROP(flexis::Overlays::IFlexisOverlay, ValuePropertyEmbeddedAssign, double, opacity)
  MAPPED_PROP(flexis::Overlays::IFlexisOverlay, ValuePropertyEmbeddedAssign, long, rangeIn)
  MAPPED_PROP(flexis::Overlays::IFlexisOverlay, ValuePropertyEmbeddedAssign, long, rangeOut)
END_MAPPING(flexis::Overlays::IFlexisOverlay)

START_MAPPING_SUB(flexis::Overlays::TestRectangularOverlay, flexis::Overlays::IFlexisOverlay, ovlX, ovlY, ovlW, ovlH)
  MAPPED_PROP(flexis::Overlays::TestRectangularOverlay, ValuePropertyEmbeddedAssign, double, ovlX)
  MAPPED_PROP(flexis::Overlays::TestRectangularOverlay, ValuePropertyEmbeddedAssign, double, ovlY)
  MAPPED_PROP(flexis::Overlays::TestRectangularOverlay, ValuePropertyEmbeddedAssign, double, ovlW)
  MAPPED_PROP(flexis::Overlays::TestRectangularOverlay, ValuePropertyEmbeddedAssign, double, ovlH)
END_MAPPING_SUB(flexis::Overlays::TestRectangularOverlay, flexis::Overlays::IFlexisOverlay)

START_MAPPING_SUB(flexis::Overlays::TimeCodeOverlay, flexis::Overlays::IFlexisOverlay, ovlX, ovlY, fontSize)
  MAPPED_PROP(flexis::Overlays::TimeCodeOverlay, ValuePropertyEmbeddedAssign, double, ovlX)
  MAPPED_PROP(flexis::Overlays::TimeCodeOverlay, ValuePropertyEmbeddedAssign, double, ovlY)
  MAPPED_PROP(flexis::Overlays::TimeCodeOverlay, ValuePropertyEmbeddedAssign, int, fontSize)
END_MAPPING_SUB(flexis::Overlays::TimeCodeOverlay, flexis::Overlays::IFlexisOverlay)

START_MAPPING_A(OtherThing, name, dvalue)
  MAPPED_PROP(OtherThing, ValuePropertyEmbeddedAssign, std::string, name)
  MAPPED_PROP(OtherThing, ValuePropertyEmbeddedAssign, double, dvalue)
END_MAPPING(OtherThing)

START_MAPPING_SUB(OtherThingA, OtherThing, lvalue, testnames)
  MAPPED_PROP(OtherThingA, ValuePropertyEmbeddedAssign, long, lvalue)
  MAPPED_PROP(OtherThingA, ValuePropertyKeyedAssign, std::vector<std::string>, testnames)
END_MAPPING_SUB(OtherThingA, OtherThing)


START_MAPPING_SUB(OtherThingB, OtherThing, llvalue)
  MAPPED_PROP(OtherThingB, ValuePropertyEmbeddedAssign, unsigned long long, llvalue)
END_MAPPING_SUB(OtherThingB, OtherThing)

START_MAPPING(SomethingWithALazyVector, name, otherThings)
  MAPPED_PROP(SomethingWithALazyVector, ValuePropertyEmbeddedAssign, std::string, name)
  MAPPED_PROP3(SomethingWithALazyVector, ObjectPtrVectorPropertyAssign, OtherThing, otherThings, true)
END_MAPPING(SomethingWithALazyVector)

START_MAPPING(SomethingWithAllValueKeyedProperties, name, counter, numbers, children)
  MAPPED_PROP(SomethingWithAllValueKeyedProperties, ValuePropertyKeyedAssign, std::string, name)
  MAPPED_PROP(SomethingWithAllValueKeyedProperties, ValuePropertyKeyedAssign, int, counter)
  MAPPED_PROP(SomethingWithAllValueKeyedProperties, ValuePropertyKeyedAssign, vector<int>, numbers)
  MAPPED_PROP(SomethingWithAllValueKeyedProperties, ValuePropertyKeyedAssign, set<string>, children)
END_MAPPING(SomethingWithAllValueKeyedProperties)

START_MAPPING(FixedSizeObject, objectId, number1, number2)
  OBJECT_ID(FixedSizeObject, objectId)
  MAPPED_PROP(FixedSizeObject, ValuePropertyEmbeddedAssign, unsigned, number1)
  MAPPED_PROP(FixedSizeObject, ValuePropertyEmbeddedAssign, unsigned, number2)
END_MAPPING(FixedSizeObject)

START_MAPPING(FixedSizeObject2, objectId, number1, number2)
  OBJECT_ID(FixedSizeObject2, objectId)
  MAPPED_PROP(FixedSizeObject2, ValuePropertyEmbeddedAssign, double, number1)
  MAPPED_PROP(FixedSizeObject2, ValuePropertyEmbeddedAssign, double, number2)
END_MAPPING(FixedSizeObject2)

START_MAPPING(VariableSizeObject, objectId, number, name)
  OBJECT_ID(VariableSizeObject, objectId)
  MAPPED_PROP(VariableSizeObject, ValuePropertyEmbeddedAssign, unsigned, number)
  MAPPED_PROP(VariableSizeObject, ValuePropertyEmbeddedAssign, std::string, name)
END_MAPPING(VariableSizeObject)

START_MAPPING(SomethingWithEmbeddedObjects, fso, vso)
  MAPPED_PROP(SomethingWithEmbeddedObjects, ObjectPropertyEmbeddedAssign, FixedSizeObject, fso)
  MAPPED_PROP(SomethingWithEmbeddedObjects, ObjectPropertyEmbeddedAssign, VariableSizeObject, vso)
END_MAPPING(SomethingWithEmbeddedObjects)

START_MAPPING(SomethingWithEmbbededObjectVectors, name, sweos, fsos, fsos2, fsos3, vsos)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, ValuePropertyEmbeddedAssign, std::string, name)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, ObjectVectorPropertyEmbeddedAssign, SomethingWithEmbeddedObjects, sweos)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, ObjectVectorPropertyEmbeddedAssign, FixedSizeObject, fsos)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, ObjectVectorPropertyEmbeddedAssign, FixedSizeObject2, fsos2)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, ObjectVectorPropertyAssign, FixedSizeObject2, fsos3)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, ObjectVectorPropertyEmbeddedAssign, VariableSizeObject, vsos)
END_MAPPING(SomethingWithEmbbededObjectVectors)

START_MAPPING(flexis::player::SourceDisplayConfig, sourceIndex, attachedIndex, attached, window_x, window_y, window_width, window_height)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, ValuePropertyEmbeddedAssign, unsigned, sourceIndex)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, ValuePropertyEmbeddedAssign, unsigned, attachedIndex)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, ValuePropertyEmbeddedAssign, bool, attached)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, ValuePropertyEmbeddedAssign, unsigned, window_x)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, ValuePropertyEmbeddedAssign, unsigned, window_y)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, ValuePropertyEmbeddedAssign, unsigned, window_width)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, ValuePropertyEmbeddedAssign, unsigned, window_height)
END_MAPPING(flexis::player::SourceDisplayConfig)

START_MAPPING(flexis::player::SourceInfo, sourceIndex, displayConfig, userOverlays)
  MAPPED_PROP(flexis::player::SourceInfo, ValuePropertyEmbeddedAssign, unsigned, sourceIndex)
  MAPPED_PROP(flexis::player::SourceInfo, ObjectPtrPropertyAssign, flexis::player::SourceDisplayConfig, displayConfig)
  MAPPED_PROP(flexis::player::SourceInfo, ObjectPtrVectorPropertyAssign, flexis::Overlays::IFlexisOverlay, userOverlays)
END_MAPPING(flexis::player::SourceInfo)

START_MAPPING(SomethingWithAnObjectIter, history)
  MAPPED_PROP_ITER(SomethingWithAnObjectIter, CollectionIterPropertyAssign, FixedSizeObject, KVObjectHistoryImpl, flexis::Overlays::ObjectHistory, history)
END_MAPPING(SomethingWithAnObjectIter)

START_MAPPING(SomethingWithAValueIter, values, datas)
  MAPPED_PROP_ITER(SomethingWithAValueIter, ValueCollectionIterPropertyAssign, std::string, ValueIterImpl, ValueIter, values)
  MAPPED_PROP_ITER(SomethingWithAValueIter, ValueCollectionIterPropertyAssign, long long, DataIterImpl, DataIter, datas)
END_MAPPING(SomethingWithAValueIter)

START_MAPPING_A(SomethingAbstract, name)
  MAPPED_PROP(SomethingAbstract, ValuePropertyEmbeddedAssign, std::string, name)
END_MAPPING(SomethingAbstract)

START_MAPPING_SUB(SomethingConcrete1, SomethingAbstract, description)
  MAPPED_PROP(SomethingConcrete1, ValuePropertyEmbeddedAssign, std::string, description)
END_MAPPING_SUB(SomethingConcrete1, SomethingAbstract)

START_MAPPING_SUB(SomethingConcrete2, SomethingAbstract, age)
  MAPPED_PROP(SomethingConcrete2, ValuePropertyEmbeddedAssign, unsigned, age)
END_MAPPING_SUB(SomethingConcrete2, SomethingAbstract)

START_MAPPING(SomethingVirtual, id, name)
  MAPPED_PROP(SomethingVirtual, ValuePropertyEmbeddedAssign, unsigned, id)
  MAPPED_PROP(SomethingVirtual, ValuePropertyEmbeddedAssign, std::string, name)
END_MAPPING(SomethingVirtual)

START_MAPPING_SUB(SomethingVirtual1, SomethingVirtual, profession)
  MAPPED_PROP(SomethingVirtual1, ValuePropertyEmbeddedAssign, std::string, profession)
END_MAPPING_SUB(SomethingVirtual1, SomethingVirtual)

START_MAPPING_SUB(SomethingVirtual2, SomethingVirtual, hobby)
  MAPPED_PROP(SomethingVirtual2, ValuePropertyEmbeddedAssign, std::string, hobby)
END_MAPPING_SUB(SomethingVirtual2, SomethingVirtual)

START_MAPPING_SUB(SomethingVirtual3, SomethingVirtual2, age)
  MAPPED_PROP(SomethingVirtual3, ValuePropertyEmbeddedAssign, unsigned, age)
END_MAPPING_SUB(SomethingVirtual3, SomethingVirtual2)

START_MAPPING(Wonderful,
              embeddedVirtual1,
              toplevelVirtual1,
              embeddedVirtual2,
              toplevelVirtual2,
              abstractsEmbedded,
              virtualsEmbedded,
              virtualsPointers,
              virtualsLazy)
  MAPPED_PROP(Wonderful, ObjectPtrPropertyEmbeddedAssign, SomethingVirtual, embeddedVirtual1)
  MAPPED_PROP(Wonderful, ObjectPtrPropertyAssign, SomethingVirtual, toplevelVirtual1)
  MAPPED_PROP(Wonderful, ObjectPtrPropertyEmbeddedAssign, SomethingVirtual, embeddedVirtual2)
  MAPPED_PROP(Wonderful, ObjectPtrPropertyAssign, SomethingVirtual, toplevelVirtual2)
  MAPPED_PROP(Wonderful, ObjectPtrVectorPropertyEmbeddedAssign, SomethingAbstract, abstractsEmbedded)
  MAPPED_PROP(Wonderful, ObjectPtrVectorPropertyEmbeddedAssign, SomethingVirtual, virtualsEmbedded)
  MAPPED_PROP(Wonderful, ObjectPtrVectorPropertyAssign, SomethingVirtual, virtualsPointers)
  MAPPED_PROP3(Wonderful, ObjectPtrVectorPropertyAssign, SomethingVirtual, virtualsLazy, true)
END_MAPPING(Wonderful)

START_MAPPING(ObjectPropertyTest, id, fso, vso, fso_vect, vso_vect)
  OBJECT_ID(ObjectPropertyTest, id)
  MAPPED_PROP(ObjectPropertyTest, ObjectPropertyAssign, FixedSizeObject, fso)
  MAPPED_PROP(ObjectPropertyTest, ObjectPropertyAssign, VariableSizeObject, vso)
  MAPPED_PROP(ObjectPropertyTest, ObjectVectorPropertyAssign, FixedSizeObject, fso_vect)
  MAPPED_PROP(ObjectPropertyTest, ObjectVectorPropertyAssign, VariableSizeObject, vso_vect)
END_MAPPING(ObjectPropertyTest)

START_MAPPING(RefCountingTest, fso, vso, fso_vect, vso_vect)
  MAPPED_PROP(RefCountingTest, ObjectPtrPropertyAssign, FixedSizeObject, fso)
  MAPPED_PROP(RefCountingTest, ObjectPtrPropertyAssign, VariableSizeObject, vso)
  MAPPED_PROP(RefCountingTest, ObjectPtrVectorPropertyAssign, FixedSizeObject, fso_vect)
  MAPPED_PROP(RefCountingTest, ObjectPtrVectorPropertyAssign, VariableSizeObject, vso_vect)
END_MAPPING(RefCountingTest)

}
}
}
#endif //FLEXIS_TESTCLASSES_H

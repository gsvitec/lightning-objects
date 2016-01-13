//
// Created by chris on 11/2/15.
//

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
  virtual T& getHistoryValue(uint64_t bufferPos) = 0;


  virtual ObjectHistory<T>* clone() = 0;
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

class RectangularOverlay : public IFlexisOverlay {

public:
  double ovlX;
  double ovlY;
  double ovlW;
  double ovlH;

  string type() const override {return "RectangularOverlay";}
};

using RectangularOverlayPtr = shared_ptr<RectangularOverlay>;


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
  std::vector<VariableSizeObject> vsos;
};

struct SomethingWithAnObjectIter
{
  std::string name;
  flexis::Overlays::ObjectHistoryPtr<FixedSizeObject> history;
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
struct KVObjectHistory2 : public flexis::Overlays::ObjectHistory<T>, public KVPropertyBackend
{
  T t;
  T& getHistoryValue(uint64_t bufferPos) override {
    return t;
  }
  flexis::Overlays::ObjectHistory<T>* clone() override {
    return nullptr;
  }
};
START_MAPPING(flexis::Overlays::Colored2DPoint, x, y, r, g, b, a)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, BasePropertyAssign, float, x)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, BasePropertyAssign, float, y)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, BasePropertyAssign, float, r)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, BasePropertyAssign, float, g)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, BasePropertyAssign, float, b)
  MAPPED_PROP(flexis::Overlays::Colored2DPoint, BasePropertyAssign, float, a)
END_MAPPING(flexis::Overlays::Colored2DPoint)

START_MAPPING(flexis::Overlays::ColoredPolygon, pts, visible)
  MAPPED_PROP(flexis::Overlays::ColoredPolygon, ObjectVectorPropertyEmbeddedAssign, flexis::Overlays::Colored2DPoint, pts)
  MAPPED_PROP(flexis::Overlays::ColoredPolygon, BasePropertyAssign, bool, visible)
END_MAPPING(flexis::Overlays::ColoredPolygon)

START_MAPPING_A(flexis::Overlays::IFlexisOverlay, name, userVisible, opacity, rangeIn, rangeOut)
  MAPPED_PROP(flexis::Overlays::IFlexisOverlay, BasePropertyAssign, std::string, name)
  MAPPED_PROP(flexis::Overlays::IFlexisOverlay, BasePropertyAssign, bool, userVisible)
  MAPPED_PROP(flexis::Overlays::IFlexisOverlay, BasePropertyAssign, double, opacity)
  MAPPED_PROP(flexis::Overlays::IFlexisOverlay, BasePropertyAssign, long, rangeIn)
  MAPPED_PROP(flexis::Overlays::IFlexisOverlay, BasePropertyAssign, long, rangeOut)
END_MAPPING(flexis::Overlays::IFlexisOverlay)

START_MAPPING_SUB(flexis::Overlays::RectangularOverlay, flexis::Overlays::IFlexisOverlay, ovlX, ovlY, ovlW, ovlH)
  MAPPED_PROP(flexis::Overlays::RectangularOverlay, BasePropertyAssign, double, ovlX)
  MAPPED_PROP(flexis::Overlays::RectangularOverlay, BasePropertyAssign, double, ovlY)
  MAPPED_PROP(flexis::Overlays::RectangularOverlay, BasePropertyAssign, double, ovlW)
  MAPPED_PROP(flexis::Overlays::RectangularOverlay, BasePropertyAssign, double, ovlH)
END_MAPPING_SUB(flexis::Overlays::RectangularOverlay, flexis::Overlays::IFlexisOverlay)

START_MAPPING_SUB(flexis::Overlays::TimeCodeOverlay, flexis::Overlays::IFlexisOverlay, ovlX, ovlY, fontSize)
  MAPPED_PROP(flexis::Overlays::TimeCodeOverlay, BasePropertyAssign, double, ovlX)
  MAPPED_PROP(flexis::Overlays::TimeCodeOverlay, BasePropertyAssign, double, ovlY)
  MAPPED_PROP(flexis::Overlays::TimeCodeOverlay, BasePropertyAssign, int, fontSize)
END_MAPPING_SUB(flexis::Overlays::TimeCodeOverlay, flexis::Overlays::IFlexisOverlay)

START_MAPPING_A(OtherThing, name, dvalue)
  MAPPED_PROP(OtherThing, BasePropertyAssign, std::string, name)
  MAPPED_PROP(OtherThing, BasePropertyAssign, double, dvalue)
END_MAPPING(OtherThing)

START_MAPPING_SUB(OtherThingA, OtherThing, lvalue, testnames)
  MAPPED_PROP(OtherThingA, BasePropertyAssign, long, lvalue)
  MAPPED_PROP(OtherThingA, BasePropertyAssign, std::vector<std::string>, testnames)
END_MAPPING_SUB(OtherThingA, OtherThing)


START_MAPPING_SUB(OtherThingB, OtherThing, llvalue)
  MAPPED_PROP(OtherThingB, BasePropertyAssign, unsigned long long, llvalue)
END_MAPPING_SUB(OtherThingB, OtherThing)

START_MAPPING(SomethingWithALazyVector, name, otherThings)
  MAPPED_PROP(SomethingWithALazyVector, BasePropertyAssign, std::string, name)
  MAPPED_PROP3(SomethingWithALazyVector, ObjectPtrVectorPropertyAssign, OtherThing, otherThings, true)
END_MAPPING(SomethingWithALazyVector)

START_MAPPING(FixedSizeObject, objectId, number1, number2)
  OBJECT_ID(FixedSizeObject, objectId)
  MAPPED_PROP(FixedSizeObject, BasePropertyAssign, unsigned, number1)
  MAPPED_PROP(FixedSizeObject, BasePropertyAssign, unsigned, number2)
END_MAPPING(FixedSizeObject)

START_MAPPING(FixedSizeObject2, objectId, number1, number2)
  OBJECT_ID(FixedSizeObject2, objectId)
  MAPPED_PROP(FixedSizeObject2, BasePropertyAssign, double, number1)
  MAPPED_PROP(FixedSizeObject2, BasePropertyAssign, double, number2)
END_MAPPING(FixedSizeObject2)

START_MAPPING(VariableSizeObject, objectId, number, name)
  OBJECT_ID(VariableSizeObject, objectId)
  MAPPED_PROP(VariableSizeObject, BasePropertyAssign, unsigned, number)
  MAPPED_PROP(VariableSizeObject, BasePropertyAssign, std::string, name)
END_MAPPING(VariableSizeObject)

START_MAPPING(SomethingWithEmbeddedObjects, fso, vso)
  MAPPED_PROP(SomethingWithEmbeddedObjects, ObjectPropertyEmbeddedAssign, FixedSizeObject, fso)
  MAPPED_PROP(SomethingWithEmbeddedObjects, ObjectPropertyEmbeddedAssign, VariableSizeObject, vso)
END_MAPPING(SomethingWithEmbeddedObjects)

START_MAPPING(SomethingWithEmbbededObjectVectors, name, sweos, fsos, fsos2, vsos)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, BasePropertyAssign, std::string, name)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, ObjectVectorPropertyEmbeddedAssign, SomethingWithEmbeddedObjects, sweos)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, ObjectVectorPropertyEmbeddedAssign, FixedSizeObject, fsos)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, ObjectVectorPropertyEmbeddedAssign, FixedSizeObject2, fsos2)
  MAPPED_PROP(SomethingWithEmbbededObjectVectors, ObjectVectorPropertyEmbeddedAssign, VariableSizeObject, vsos)
END_MAPPING(SomethingWithEmbbededObjectVectors)

START_MAPPING(flexis::player::SourceDisplayConfig, sourceIndex, attachedIndex, attached, window_x, window_y, window_width, window_height)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, sourceIndex)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, attachedIndex)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, bool, attached)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, window_x)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, window_y)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, window_width)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, window_height)
END_MAPPING(flexis::player::SourceDisplayConfig)

START_MAPPING(flexis::player::SourceInfo, sourceIndex, displayConfig, userOverlays)
  MAPPED_PROP(flexis::player::SourceInfo, BasePropertyAssign, unsigned, sourceIndex)
  MAPPED_PROP(flexis::player::SourceInfo, ObjectPtrPropertyAssign, flexis::player::SourceDisplayConfig, displayConfig)
  MAPPED_PROP(flexis::player::SourceInfo, ObjectPtrVectorPropertyAssign, flexis::Overlays::IFlexisOverlay, userOverlays)
END_MAPPING(flexis::player::SourceInfo)

START_MAPPING(SomethingWithAnObjectIter, history)
  MAPPED_PROP_ITER(SomethingWithAnObjectIter, CollectionIterPropertyAssign, FixedSizeObject, KVObjectHistory2, flexis::Overlays::ObjectHistory, history)
END_MAPPING(SomethingWithAnObjectIter)

START_MAPPING_A(SomethingAbstract, name)
  MAPPED_PROP(SomethingAbstract, BasePropertyAssign, std::string, name)
END_MAPPING(SomethingAbstract)

START_MAPPING_SUB(SomethingConcrete1, SomethingAbstract, description)
  MAPPED_PROP(SomethingConcrete1, BasePropertyAssign, std::string, description)
END_MAPPING_SUB(SomethingConcrete1, SomethingAbstract)

START_MAPPING_SUB(SomethingConcrete2, SomethingAbstract, age)
  MAPPED_PROP(SomethingConcrete2, BasePropertyAssign, unsigned, age)
END_MAPPING_SUB(SomethingConcrete2, SomethingAbstract)

START_MAPPING(SomethingVirtual, id, name)
  MAPPED_PROP(SomethingVirtual, BasePropertyAssign, unsigned, id)
  MAPPED_PROP(SomethingVirtual, BasePropertyAssign, std::string, name)
END_MAPPING(SomethingVirtual)

START_MAPPING_SUB(SomethingVirtual1, SomethingVirtual, profession)
  MAPPED_PROP(SomethingVirtual1, BasePropertyAssign, std::string, profession)
END_MAPPING_SUB(SomethingVirtual1, SomethingVirtual)

START_MAPPING_SUB(SomethingVirtual2, SomethingVirtual, hobby)
  MAPPED_PROP(SomethingVirtual2, BasePropertyAssign, std::string, hobby)
END_MAPPING_SUB(SomethingVirtual2, SomethingVirtual)

START_MAPPING_SUB(SomethingVirtual3, SomethingVirtual2, age)
  MAPPED_PROP(SomethingVirtual3, BasePropertyAssign, unsigned, age)
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

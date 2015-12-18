//
// Created by chris on 11/2/15.
//

#ifndef FLEXIS_TESTCLASSES_H
#define FLEXIS_TESTCLASSES_H

#include <string>
#include <vector>
#include <memory>
#include <kvstore/kvstore.h>
#include <kv/kvlibtraits.h>

#ifdef TESTCLASSES_IMPL
#include <kvstore/traits_impl.h>
#else
#include <kvstore/traits_decl.h>
#endif

namespace flexis {
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
  std::vector<IFlexisOverlayPtr> userOverlays;

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
  unsigned number1, number2;
  FixedSizeObject() {}
  FixedSizeObject(unsigned number1, unsigned number2) : number1(number1), number2(number2) {}
};
using FixedSizeObjectPtr = std::shared_ptr<FixedSizeObject>;

struct VariableSizeObject {
  unsigned number;
  std::string name;

  VariableSizeObject() {}
  VariableSizeObject(unsigned number, const char *nm) : number(number), name(nm) {}
};

struct SomethingWithAnEmbbededObjectVector
{
  std::string name;
  std::vector<FixedSizeObject> objects;
  std::vector<VariableSizeObject> objects2;
};

struct SomethingWithAnObjectIter
{
  std::string name;
  flexis::ObjectHistoryPtr<FixedSizeObject> history;
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
struct KVObjectHistory2 : public ObjectHistory<T>, public IterPropertyBackend
{
  T t;
  T& getHistoryValue(uint64_t bufferPos) override {
    return t;
  }
  ObjectHistory<T>* clone() override {
    return nullptr;
  }
};

START_MAPPINGHDR_A(OtherThing)
  enum class PropertyIds {name=1, dvalue};
END_MAPPINGHDR(OtherThing)
  MAPPED_PROP(OtherThing, BasePropertyAssign, std::string, name)
  MAPPED_PROP(OtherThing, BasePropertyAssign, double, dvalue)
END_MAPPING(OtherThing)

START_MAPPINGHDR_SUB(OtherThingA, OtherThing, a)
  enum class PropertyIds {lvalue=1, testnames};
END_MAPPINGHDR_SUB(OtherThingA, OtherThing, a)
  MAPPED_PROP(OtherThingA, BasePropertyAssign, long, lvalue)
  MAPPED_PROP(OtherThingA, BasePropertyAssign, std::vector<std::string>, testnames)
END_MAPPING_SUB(OtherThingA, OtherThing, a)


START_MAPPINGHDR_SUB(OtherThingB, OtherThing, b)
  enum class PropertyIds {llvalue=1};
END_MAPPINGHDR_SUB(OtherThingB, OtherThing, b)
  MAPPED_PROP(OtherThingB, BasePropertyAssign, unsigned long long, llvalue)
END_MAPPING_SUB(OtherThingB, OtherThing, b)

//SomethingWithALazyVector
START_MAPPINGHDR(SomethingWithALazyVector)
  enum class PropertyIds {name=1, otherThings};
END_MAPPINGHDR(SomethingWithALazyVector)
  MAPPED_PROP(SomethingWithALazyVector, BasePropertyAssign, std::string, name)
  MAPPED_PROP3(SomethingWithALazyVector, ObjectPtrVectorPropertyAssign, OtherThing, otherThings, true)
END_MAPPING(SomethingWithALazyVector)

START_MAPPINGHDR(FixedSizeObject)
  enum class PropertyIds {number1=1, number2};
END_MAPPINGHDR(FixedSizeObject)
  MAPPED_PROP(FixedSizeObject, BasePropertyAssign, unsigned, number1)
  MAPPED_PROP(FixedSizeObject, BasePropertyAssign, unsigned, number2)
END_MAPPING(FixedSizeObject)

START_MAPPINGHDR(VariableSizeObject)
  enum class PropertyIds {number=1, name};
END_MAPPINGHDR(VariableSizeObject)
  MAPPED_PROP(VariableSizeObject, BasePropertyAssign, unsigned, number)
  MAPPED_PROP(VariableSizeObject, BasePropertyAssign, std::string, name)
END_MAPPING(VariableSizeObject)

START_MAPPINGHDR(SomethingWithAnEmbbededObjectVector)
  enum class PropertyIds {name=1, objects};
END_MAPPINGHDR(SomethingWithAnEmbbededObjectVector)
  MAPPED_PROP(SomethingWithAnEmbbededObjectVector, BasePropertyAssign, std::string, name)
  MAPPED_PROP(SomethingWithAnEmbbededObjectVector, ObjectVectorPropertyEmbeddedAssign, FixedSizeObject, objects)
  MAPPED_PROP(SomethingWithAnEmbbededObjectVector, ObjectVectorPropertyEmbeddedAssign, VariableSizeObject, objects2)
END_MAPPING(SomethingWithAnEmbbededObjectVector)

START_MAPPINGHDR(flexis::player::SourceDisplayConfig)
  enum class PropertyIds {sourceIndex=1, attachedIndex, attached, window_x, window_y, window_width, window_height};
END_MAPPINGHDR(flexis::player::SourceDisplayConfig)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, sourceIndex)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, attachedIndex)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, bool, attached)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, window_x)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, window_y)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, window_width)
  MAPPED_PROP(flexis::player::SourceDisplayConfig, BasePropertyAssign, unsigned, window_height)
END_MAPPING(flexis::player::SourceDisplayConfig)

START_MAPPINGHDR(flexis::player::SourceInfo)
  enum class PropertyIds {sourceIndex=1, displayConfig, userOverlays};
END_MAPPINGHDR(flexis::player::SourceInfo)
  MAPPED_PROP(flexis::player::SourceInfo, BasePropertyAssign, unsigned, sourceIndex)
  MAPPED_PROP(flexis::player::SourceInfo, ObjectPtrPropertyAssign, flexis::player::SourceDisplayConfig, displayConfig)
  MAPPED_PROP(flexis::player::SourceInfo, ObjectPtrVectorPropertyAssign, flexis::IFlexisOverlay, userOverlays)
END_MAPPING(flexis::player::SourceInfo)

START_MAPPINGHDR(SomethingWithAnObjectIter)
  enum class PropertyIds {history=1};
END_MAPPINGHDR(SomethingWithAnObjectIter)
  MAPPED_PROP_ITER(SomethingWithAnObjectIter, CollectionIterPropertyAssign, FixedSizeObject, KVObjectHistory2, ObjectHistory, history)
END_MAPPING(SomethingWithAnObjectIter)

START_MAPPINGHDR_A(SomethingAbstract)
  enum class PropertyIds {name=1};
END_MAPPINGHDR(SomethingAbstract)
  MAPPED_PROP(SomethingAbstract, BasePropertyAssign, std::string, name)
END_MAPPING(SomethingAbstract)

START_MAPPINGHDR_SUB(SomethingConcrete1, SomethingAbstract, c)
  enum class PropertyIds {description=1};
END_MAPPINGHDR_SUB(SomethingConcrete1, SomethingAbstract, c)
  MAPPED_PROP(SomethingConcrete1, BasePropertyAssign, std::string, description)
END_MAPPING_SUB(SomethingConcrete1, SomethingAbstract, c)

START_MAPPINGHDR_SUB(SomethingConcrete2, SomethingAbstract, d)
  enum class PropertyIds {age=1};
END_MAPPINGHDR_SUB(SomethingConcrete2, SomethingAbstract, d)
  MAPPED_PROP(SomethingConcrete2, BasePropertyAssign, unsigned, age)
END_MAPPING_SUB(SomethingConcrete2, SomethingAbstract, d)

START_MAPPINGHDR(SomethingVirtual)
  enum class PropertyIds {id=1, name};
END_MAPPINGHDR(SomethingVirtual)
  MAPPED_PROP(SomethingVirtual, BasePropertyAssign, unsigned, id)
  MAPPED_PROP(SomethingVirtual, BasePropertyAssign, std::string, name)
END_MAPPING(SomethingVirtual)

START_MAPPINGHDR_SUB(SomethingVirtual1, SomethingVirtual, e)
  enum class PropertyIds {profession=1};
END_MAPPINGHDR_SUB(SomethingVirtual1, SomethingVirtual, e)
  MAPPED_PROP(SomethingVirtual1, BasePropertyAssign, std::string, profession)
END_MAPPING_SUB(SomethingVirtual1, SomethingVirtual, e)

START_MAPPINGHDR_SUB(SomethingVirtual2, SomethingVirtual, f)
  enum class PropertyIds {hobby=1};
END_MAPPINGHDR_SUB(SomethingVirtual2, SomethingVirtual, f)
  MAPPED_PROP(SomethingVirtual2, BasePropertyAssign, std::string, hobby)
END_MAPPING_SUB(SomethingVirtual2, SomethingVirtual, f)

START_MAPPINGHDR_SUB(SomethingVirtual3, SomethingVirtual2, g)
  enum class PropertyIds {age=1};
END_MAPPINGHDR_SUB(SomethingVirtual3, SomethingVirtual2, g)
  MAPPED_PROP(SomethingVirtual3, BasePropertyAssign, unsigned, age)
END_MAPPING_SUB(SomethingVirtual3, SomethingVirtual2, g)

START_MAPPINGHDR(Wonderful)
  enum class PropertyIds {embeddedVirtual1=1, toplevelVirtual1, embeddedVirtual2, toplevelVirtual2, abstractsEmbedded, virtualsEmbedded, virtualsPointers, virtualsLazy};
END_MAPPINGHDR(Wonderful)
  MAPPED_PROP(Wonderful, ObjectPtrPropertyEmbeddedAssign, SomethingVirtual, embeddedVirtual1)
  MAPPED_PROP(Wonderful, ObjectPtrPropertyAssign, SomethingVirtual, toplevelVirtual1)
  MAPPED_PROP(Wonderful, ObjectPtrPropertyEmbeddedAssign, SomethingVirtual, embeddedVirtual2)
  MAPPED_PROP(Wonderful, ObjectPtrPropertyAssign, SomethingVirtual, toplevelVirtual2)
  MAPPED_PROP(Wonderful, ObjectPtrVectorPropertyEmbeddedAssign, SomethingAbstract, abstractsEmbedded)
  MAPPED_PROP(Wonderful, ObjectPtrVectorPropertyEmbeddedAssign, SomethingVirtual, virtualsEmbedded)
  MAPPED_PROP(Wonderful, ObjectPtrVectorPropertyAssign, SomethingVirtual, virtualsPointers)
  MAPPED_PROP3(Wonderful, ObjectPtrVectorPropertyAssign, SomethingVirtual, virtualsLazy, true)
END_MAPPING(Wonderful)

}
}
}
#endif //FLEXIS_TESTCLASSES_H

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
struct SomethingWithAnEmbbededObjectVector
{
  std::string name;
  std::vector<FixedSizeObject> objects;
};

struct SomethingWithAnObjectIter
{
  std::string name;
  flexis::ObjectHistoryPtr<FixedSizeObject> history;
};

namespace flexis {
namespace persistence {
namespace kv {

template<typename T>
struct KVObjectHistory2 : public ObjectHistory<T>, public IterPropertyBackend<T>
{
  T& getHistoryValue(uint64_t bufferPos) override {

  }
  ObjectHistory<T>* clone() override {

  }
};

START_MAPPINGHDR(OtherThing)
  enum PropertyIds {name=1, dvalue};
END_MAPPINGHDR(OtherThing)
  MAPPED_PROP(OtherThing, BasePropertyAssign, std::string, name)
  MAPPED_PROP(OtherThing, BasePropertyAssign, double, dvalue)
END_MAPPING(OtherThing)

START_MAPPINGHDR_SUB(OtherThingA, OtherThing, OtherThingA)
  enum PropertyIds {lvalue=1, testnames};
END_MAPPINGHDR_SUB(OtherThingA, OtherThing, OtherThingA)
  MAPPED_PROP(OtherThingA, BasePropertyAssign, long, lvalue)
  MAPPED_PROP(OtherThingA, BasePropertyAssign, std::vector<std::string>, testnames)
END_MAPPING_SUB(OtherThingA, OtherThing, OtherThingA)


START_MAPPINGHDR_SUB(OtherThingB, OtherThing, OtherThingB)
  enum PropertyIds {llvalue=1};
END_MAPPINGHDR_SUB(OtherThingB, OtherThing, OtherThingB)
  MAPPED_PROP(OtherThingB, BasePropertyAssign, unsigned long long, llvalue)
END_MAPPING_SUB(OtherThingB, OtherThing, OtherThingB)

//SomethingWithALazyVector
START_MAPPINGHDR(SomethingWithALazyVector)
  enum PropertyIds {name=1, otherThings};
END_MAPPINGHDR(SomethingWithALazyVector)
  MAPPED_PROP(SomethingWithALazyVector, BasePropertyAssign, std::string, name)
  MAPPED_PROP3(SomethingWithALazyVector, ObjectPtrVectorPropertyAssign, OtherThing, otherThings, true)
END_MAPPING(SomethingWithALazyVector)

START_MAPPINGHDR(FixedSizeObject)
  enum PropertyIds {number1=1, number2};
END_MAPPINGHDR(FixedSizeObject)
  MAPPED_PROP(FixedSizeObject, BasePropertyAssign, unsigned, number1)
  MAPPED_PROP(FixedSizeObject, BasePropertyAssign, unsigned, number2)
END_MAPPING(FixedSizeObject)

START_MAPPINGHDR(SomethingWithAnEmbbededObjectVector)
  enum PropertyIds {name=1, objects};
END_MAPPINGHDR(SomethingWithAnEmbbededObjectVector)
  MAPPED_PROP(SomethingWithAnEmbbededObjectVector, BasePropertyAssign, std::string, name)
  MAPPED_PROP3(SomethingWithAnEmbbededObjectVector, ObjectVectorPropertyEmbeddedAssign, FixedSizeObject, objects, 8)
END_MAPPING(SomethingWithAnEmbbededObjectVector)

START_MAPPINGHDR(flexis::player::SourceDisplayConfig)
  enum PropertyIds {sourceIndex=1, attachedIndex, attached, window_x, window_y, window_width, window_height};
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
  enum PropertyIds {sourceIndex=1, displayConfig, userOverlays};
END_MAPPINGHDR(flexis::player::SourceInfo)
  MAPPED_PROP(flexis::player::SourceInfo, BasePropertyAssign, unsigned, sourceIndex)
  MAPPED_PROP(flexis::player::SourceInfo, ObjectPtrPropertyAssign, flexis::player::SourceDisplayConfig, displayConfig)
  MAPPED_PROP(flexis::player::SourceInfo, ObjectPtrVectorPropertyAssign, flexis::IFlexisOverlay, userOverlays)
END_MAPPING(flexis::player::SourceInfo)

START_MAPPINGHDR(SomethingWithAnObjectIter)
  enum PropertyIds {history=1};
END_MAPPINGHDR(SomethingWithAnObjectIter)
  MAPPED_PROP_ITER(SomethingWithAnObjectIter, ObjectIterPropertyAssign, FixedSizeObject, KVObjectHistory2, ObjectHistory, history)
END_MAPPING(SomethingWithAnObjectIter)

}
}
}
#endif //FLEXIS_TESTCLASSES_H

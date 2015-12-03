//
// Created by chris on 11/2/15.
//

#ifndef FLEXIS_TESTCLASSES_H
#define FLEXIS_TESTCLASSES_H

#include <string>
#include <vector>
#include <memory>
#include <kvstore/kvstore.h>
#include <kvstore/traits_impl.h>
#include <basicoverlays.h>

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
struct SomethingWithAnEmbbededObjectVector
{
  std::string name;
  std::vector<FixedSizeObject> objects;
};

namespace flexis {
namespace persistence {
namespace kv {

using OtherThingTraitsBase = ClassTraitsBase<OtherThing, OtherThing, OtherThingA, OtherThingB>;
START_MAPPINGHDR_INH(OtherThing, OtherThingTraitsBase)
  enum PropertyIds {name=1, dvalue};
END_MAPPINGHDR_INH(OtherThing, OtherThingTraitsBase)
  MAPPED_PROP(OtherThing, BasePropertyAssign, std::string, name)
  MAPPED_PROP(OtherThing, BasePropertyAssign, double, dvalue)
END_MAPPING_INH(OtherThing, OtherThingTraitsBase)

using OtherThingATraitsBase = ClassTraitsBase<OtherThingA, OtherThing>;
START_MAPPINGHDR_INH(OtherThingA, OtherThingATraitsBase)
  enum PropertyIds {lvalue=1, testnames};
END_MAPPINGHDR_INH(OtherThingA, OtherThingATraitsBase)
  MAPPED_PROP(OtherThingA, BasePropertyAssign, long, lvalue)
  MAPPED_PROP(OtherThingA, BasePropertyAssign, std::vector<std::string>, testnames)
END_MAPPING_INH2(OtherThingA, OtherThingATraitsBase, OtherThing)


using OtherThingBTraitsBase = ClassTraitsBase<OtherThingB, OtherThing>;
START_MAPPINGHDR_INH(OtherThingB, OtherThingBTraitsBase)
  enum PropertyIds {llvalue=1};
END_MAPPINGHDR_INH(OtherThingB, OtherThingBTraitsBase)
  MAPPED_PROP(OtherThingB, BasePropertyAssign, unsigned long long, llvalue)
END_MAPPING_INH2(OtherThingB, OtherThingBTraitsBase, OtherThing)

//SomethingWithALazyVector
template <>
struct ClassTraits<SomethingWithALazyVector> : public ClassTraitsBase<SomethingWithALazyVector>{
  enum PropertyIds {name=1, otherThings};
};
template<> ClassInfo ClassTraitsBase<SomethingWithALazyVector>::info ("SomethingWithALazyVector", typeid(SomethingWithALazyVector));
template<> PropertyAccessBase * ClassTraitsBase<SomethingWithALazyVector>::decl_props[] = {
    new BasePropertyAssign<SomethingWithALazyVector, std::string, &SomethingWithALazyVector::name>("name"),
    new ObjectPtrVectorPropertyAssign<SomethingWithALazyVector, OtherThing, &SomethingWithALazyVector::otherThings>("otherThings", true)
};
template<> Properties * ClassTraitsBase<SomethingWithALazyVector>::properties(Properties::mk<SomethingWithALazyVector>());

template <>
struct ClassTraits<FixedSizeObject> : public ClassTraitsBase<FixedSizeObject>{
  enum PropertyIds {name=1, dvalue};
};
template<> ClassInfo ClassTraitsBase<FixedSizeObject>::info ("FixedSizeObject", typeid(FixedSizeObject));
template<> PropertyAccessBase * ClassTraitsBase<FixedSizeObject>::decl_props[] = {
    new BasePropertyAssign<FixedSizeObject, unsigned, &FixedSizeObject::number1>("number1"),
    new BasePropertyAssign<FixedSizeObject, unsigned, &FixedSizeObject::number2>("number2")
};
template<> Properties * ClassTraitsBase<FixedSizeObject>::properties(Properties::mk<FixedSizeObject>());

template <>
struct ClassTraits<SomethingWithAnEmbbededObjectVector> : public ClassTraitsBase<SomethingWithAnEmbbededObjectVector> {
  enum PropertyIds {name=1, objects};
};
template<> ClassInfo ClassTraitsBase<SomethingWithAnEmbbededObjectVector>::info ("SomethingWithAnEmbbededObjectVector", typeid(SomethingWithAnEmbbededObjectVector));
template<> PropertyAccessBase * ClassTraitsBase<SomethingWithAnEmbbededObjectVector>::decl_props[] = {
    new BasePropertyAssign<SomethingWithAnEmbbededObjectVector, std::string, &SomethingWithAnEmbbededObjectVector::name>("name"),
    new ObjectVectorPropertyEmbeddedAssign<SomethingWithAnEmbbededObjectVector, FixedSizeObject, &SomethingWithAnEmbbededObjectVector::objects>("objects", 8)
};
template<> Properties * ClassTraitsBase<SomethingWithAnEmbbededObjectVector>::properties(Properties::mk<SomethingWithAnEmbbededObjectVector>());

template <>
struct ClassTraits<flexis::player::SourceDisplayConfig> : public ClassTraitsBase<flexis::player::SourceDisplayConfig>{
  enum PropertyIds {sourceIndex=1, attachedIndex, attached, window_x, window_y, window_width, window_height};
};
template<> ClassInfo ClassTraitsBase<flexis::player::SourceDisplayConfig>::info ("flexis::player::SourceDisplayConfig", typeid(flexis::player::SourceDisplayConfig));
template<> PropertyAccessBase * ClassTraitsBase<flexis::player::SourceDisplayConfig>::decl_props[] = {
    new BasePropertyAssign<flexis::player::SourceDisplayConfig, unsigned, &flexis::player::SourceDisplayConfig::sourceIndex>("sourceIndex"),
    new BasePropertyAssign<flexis::player::SourceDisplayConfig, unsigned, &flexis::player::SourceDisplayConfig::attachedIndex>("attachedIndex"),
    new BasePropertyAssign<flexis::player::SourceDisplayConfig, bool, &flexis::player::SourceDisplayConfig::attached>("attached"),
    new BasePropertyAssign<flexis::player::SourceDisplayConfig, unsigned, &flexis::player::SourceDisplayConfig::window_x>("window_x"),
    new BasePropertyAssign<flexis::player::SourceDisplayConfig, unsigned, &flexis::player::SourceDisplayConfig::window_y>("window_y"),
    new BasePropertyAssign<flexis::player::SourceDisplayConfig, unsigned, &flexis::player::SourceDisplayConfig::window_width>("window_width"),
    new BasePropertyAssign<flexis::player::SourceDisplayConfig, unsigned, &flexis::player::SourceDisplayConfig::window_height>("window_height")
};
template<> Properties * ClassTraitsBase<flexis::player::SourceDisplayConfig>::properties(Properties::mk<flexis::player::SourceDisplayConfig>());

template <>
struct ClassTraits<flexis::player::SourceInfo> : public ClassTraitsBase<flexis::player::SourceInfo>{
  enum PropertyIds {sourceIndex=1, displayConfig, userOverlays};
};
template<> ClassInfo ClassTraitsBase<flexis::player::SourceInfo>::info ("flexis::player::SourceInfo", typeid(flexis::player::SourceInfo));
template<> PropertyAccessBase * ClassTraitsBase<flexis::player::SourceInfo>::decl_props[] = {
    new BasePropertyAssign<flexis::player::SourceInfo, unsigned, &flexis::player::SourceInfo::sourceIndex>("sourceIndex"),
    new ObjectPtrPropertyAssign<flexis::player::SourceInfo, flexis::player::SourceDisplayConfig, &flexis::player::SourceInfo::displayConfig>("displayConfig"),
    new ObjectPtrVectorPropertyAssign<flexis::player::SourceInfo, flexis::IFlexisOverlay, &flexis::player::SourceInfo::userOverlays>("userOverlays")
};
template<> Properties * ClassTraitsBase<flexis::player::SourceInfo>::properties(Properties::mk<flexis::player::SourceInfo>());

}
}
}
#endif //FLEXIS_TESTCLASSES_H

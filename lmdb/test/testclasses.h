//
// Created by chris on 11/2/15.
//

#ifndef FLEXIS_TESTCLASSES_H
#define FLEXIS_TESTCLASSES_H

#include <string>
#include <vector>
#include <memory>
#include <kvstore/kvstore.h>

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

namespace flexis {
namespace persistence {
namespace kv {

//OtherThing
template <>
struct ClassTraits<OtherThing> : public ClassTraitsBase<OtherThing>{
  enum PropertyIds {name=1, dvalue};
};
template<> ClassInfo ClassTraitsBase<OtherThing>::info ("OtherThing", typeid(OtherThing));
template<> PropertyAccessBase * ClassTraitsBase<OtherThing>::decl_props[] = {
    new BasePropertyAssign<OtherThing, std::string, &OtherThing::name>("name"),
    new BasePropertyAssign<OtherThing, double, &OtherThing::dvalue>("dvalue")
};
template<> Properties * ClassTraitsBase<OtherThing>::properties(Properties::mk<OtherThing>());

//OtherThingA
template <>
struct ClassTraits<OtherThingA> : public ClassTraitsBase<OtherThingA>{
  enum PropertyIds {lvalue=1};
};
template<> ClassInfo ClassTraitsBase<OtherThingA>::info ("OtherThingA", typeid(OtherThingA));
template<> PropertyAccessBase * ClassTraitsBase<OtherThingA>::decl_props[] = {
    new BasePropertyAssign<OtherThingA, long, &OtherThingA::lvalue>("lvalue"),
    new BasePropertyAssign<OtherThingA, std::vector<std::string>, &OtherThingA::testnames>("testnames")
};
template<> Properties * ClassTraitsBase<OtherThingA>::properties(Properties::mk<OtherThingA, OtherThing>());

//OtherThingB
template <>
struct ClassTraits<OtherThingB> : public ClassTraitsBase<OtherThingB>{
  enum PropertyIds {llvalue=1};
};
template<> ClassInfo ClassTraitsBase<OtherThingB>::info ("OtherThingB", typeid(OtherThingB));
template<> PropertyAccessBase * ClassTraitsBase<OtherThingB>::decl_props[] = {
    new BasePropertyAssign<OtherThingB, unsigned long long, &OtherThingB::llvalue>("llvalue")
};
template<> Properties * ClassTraitsBase<OtherThingB>::properties(Properties::mk<OtherThingB, OtherThing>());

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

}
}
}
#endif //FLEXIS_TESTCLASSES_H

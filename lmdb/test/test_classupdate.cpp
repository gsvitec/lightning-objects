//
// Created by chris on 4/26/16.
//

#include <cassert>
#include <sstream>
#include <kvstore.h>

#include <traits_impl.h>

using namespace std;
using namespace flexis::persistence;
using namespace flexis::persistence::kv;

struct TestClass1 {
  string name;
  int number;
};
struct TestClass2 {
  string name;
  string description;
  int number;
  double value;

  shared_ptr<TestClass1> test1_ptr;
  vector<shared_ptr<TestClass1>> test1_emb;
  vector<shared_ptr<TestClass1>> test1_key;
};

namespace flexis {
namespace persistence {
namespace kv {

START_MAPPING(TestClass1, name, number)
MAPPED_PROP(TestClass1, BasePropertyAssign, std::string, name)
MAPPED_PROP(TestClass1, BasePropertyAssign, int, number)
END_MAPPING(TestClass1)

START_MAPPING(TestClass2, name, description, number, value, test1_ptr, test1_emb, test1_key)
MAPPED_PROP(TestClass2, BasePropertyAssign, std::string, name)
MAPPED_PROP(TestClass2, BasePropertyAssign, std::string, description)
MAPPED_PROP(TestClass2, BasePropertyAssign, int, number)
MAPPED_PROP(TestClass2, BasePropertyAssign, double, value)
MAPPED_PROP(TestClass2, ObjectPtrPropertyAssign, TestClass1, test1_ptr)
MAPPED_PROP(TestClass2, ObjectPtrVectorPropertyEmbeddedAssign, TestClass1, test1_emb)
MAPPED_PROP(TestClass2, ObjectPtrVectorPropertyAssign, TestClass1, test1_key)
END_MAPPING(TestClass2)

}
}
}

namespace test {

class KeyValueStore : public KeyValueStoreBase
{
  template <typename T>
  PropertyMetaInfoPtr make_propertyinfo(kv::PropertyId id, const char *name, bool isVector)
  {
    KeyValueStoreBase::PropertyMetaInfoPtr mi(new KeyValueStoreBase::PropertyMetaInfo());

    mi->id = id;
    mi->name = name;
    mi->typeId = TypeTraits<T>::id;
    mi->isVector = isVector;
    mi->byteSize = TypeTraits<T>::byteSize;
    mi->storeLayout = StoreLayout::all_embedded;
    mi->className = "";

    return mi;
  }

  template <typename T>
  PropertyMetaInfoPtr make_propertyinfo(kv::PropertyId id, const char *name, bool isVector, StoreLayout layout)
  {
    KeyValueStoreBase::PropertyMetaInfoPtr mi(new KeyValueStoreBase::PropertyMetaInfo());

    mi->id = id;
    mi->name = name;
    mi->typeId = ClassTraits<T>::traits_data(0).classId;
    mi->isVector = isVector;
    mi->byteSize = ObjectKey_sz;
    mi->storeLayout = layout;
    mi->className = ClassTraits<T>::traits_info->name;

    return mi;
  }

public:
  unsigned test_no = 0;

  KeyValueStore() : KeyValueStoreBase(0) {}

  bool testUpdateClassSchema(
      AbstractClassInfo *classInfo, const PropertyAccessBase ** properties[], unsigned numProperties,
      std::vector<schema_compatibility::Property> &errors)
  {
    return updateClassSchema(classInfo, properties, numProperties, errors);
  }

protected:
  virtual void loadSaveClassMeta(kv::StoreId storeId, kv::AbstractClassInfo *classInfo, const PropertyAccessBase ***currentProps,
                                 unsigned numProps, std::vector<PropertyMetaInfoPtr> &propertyInfos) override
  {
    switch(test_no) {
      case 0:
        //equality
        propertyInfos.push_back(make_propertyinfo<string>(2, "name", false));
        propertyInfos.push_back(make_propertyinfo<string>(3, "description", false));
        propertyInfos.push_back(make_propertyinfo<int>(4, "number", false));
        propertyInfos.push_back(make_propertyinfo<double>(5, "value", false));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(6, "test1_ptr", false, StoreLayout::embedded_key));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(7, "test1_emb", true, StoreLayout::all_embedded));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(8, "test1_key", true, StoreLayout::property));
        break;
      case 1:
        //move property number (results in removal and insertion)
        propertyInfos.push_back(make_propertyinfo<string>(2, "name", false));
        propertyInfos.push_back(make_propertyinfo<int>(3, "number", false));
        propertyInfos.push_back(make_propertyinfo<string>(4, "description", false));
        propertyInfos.push_back(make_propertyinfo<double>(5, "value", false));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(6, "test1_ptr", false, StoreLayout::embedded_key));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(7, "test1_emb", true, StoreLayout::all_embedded));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(8, "test1_key", true, StoreLayout::property));
        break;
      case 2:
        //append embedded property test1_emb
        propertyInfos.push_back(make_propertyinfo<string>(2, "name", false));
        propertyInfos.push_back(make_propertyinfo<string>(3, "description", false));
        propertyInfos.push_back(make_propertyinfo<int>(4, "number", false));
        propertyInfos.push_back(make_propertyinfo<double>(5, "value", false));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(6, "test1_ptr", false, StoreLayout::embedded_key));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(7, "test1_key", true, StoreLayout::property));
        break;
      case 3:
        //add key-only property test1_key
        propertyInfos.push_back(make_propertyinfo<string>(2, "name", false));
        propertyInfos.push_back(make_propertyinfo<string>(3, "description", false));
        propertyInfos.push_back(make_propertyinfo<int>(4, "number", false));
        propertyInfos.push_back(make_propertyinfo<double>(5, "value", false));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(6, "test1_ptr", false, StoreLayout::embedded_key));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(7, "test1_emb", true, StoreLayout::all_embedded));
        break;
      case 4:
        //extreme example
        propertyInfos.push_back(make_propertyinfo<string>(2, "description", false));
        propertyInfos.push_back(make_propertyinfo<double>(3, "value", false));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(4, "test1_emb", true, StoreLayout::all_embedded));
        propertyInfos.push_back(make_propertyinfo<TestClass1>(5, "test1_key", true, StoreLayout::property));
        break;
    }
  }
};

}

#define CALLUPDATE(num) tkv.test_no = num; \
vector<schema_compatibility::Property> errors; \
bool result = tkv.testUpdateClassSchema(&classInfo, ClassTraits<TestClass2>::decl_props, \
ClassTraits<TestClass2>::num_decl_props, errors);

void test_classupdate()
{
  test::KeyValueStore tkv;

  ClassInfo<TestClass2> classInfo("TestClass2", typeid(TestClass2));
  {
    CALLUPDATE(0)
    assert(result && errors.empty() && (classInfo.compatibility == SchemaCompatibility::write));
  }
  {
    CALLUPDATE(1)
    assert(result && errors.size() == 2 && (classInfo.compatibility == SchemaCompatibility::none));
  }
  {
    CALLUPDATE(2)
    assert(result && errors.size() == 1 && (classInfo.compatibility == SchemaCompatibility::write));
  }
  {
    CALLUPDATE(3)
    assert(result && errors.size() == 1 && (classInfo.compatibility == SchemaCompatibility::write));
  }
  {
    CALLUPDATE(4)
    assert(result && errors.size() == 3 && (classInfo.compatibility == SchemaCompatibility::none));
  }
}

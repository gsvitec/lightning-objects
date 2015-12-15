//
// Created by cse on 10/9/15.
//

#ifndef FLEXIS_FLEXIS_KVTRAITS_H
#define FLEXIS_FLEXIS_KVTRAITS_H

#include <string>
#include <vector>
#include <set>
#include <cstring>
#include <typeinfo>
#include <stdexcept>
#include <stdint.h>
#include <type_traits>
#include "kvbuf.h"
#include "FlexisPersistence_Export.h"

namespace flexis {
namespace persistence {
namespace kv {

#define ARRAY_SZ(x) unsigned(sizeof(x) / sizeof(decltype(*x)))

struct PropertyType
{
  //predefined base type id, irrelevant if className is set
  const ClassId id;

  //it's a vector
  const bool isVector;

  //number of bytes, 0 if variable size (e.g. string). For a vector, this is the byteSize of the elements
  const unsigned byteSize;

  //name of the mapped type if this is an object type
  const char *className;

  PropertyType(unsigned id, unsigned byteSize, bool isVector=false)
      : id(id), isVector(isVector), byteSize(byteSize), className(nullptr) {}
  PropertyType(const char *clsName, bool isVector=false) :
      id(0), isVector(isVector), byteSize(StorageKey::byteSize), className(clsName) {}

  bool operator == (const PropertyType &other) const {
    return id == other.id
           && isVector == other.isVector
           && className == other.className;
  }
};

template <typename T> struct TypeTraits;
#define TYPETRAITS template <> struct TypeTraits
#define TYPETRAITSV template <> struct TypeTraits<std::vector
#define TYPETRAITSS template <> struct TypeTraits<std::set

#define TYPEDEF(_id, _sz) static const ClassId id=_id; static const unsigned byteSize=_sz; static const bool isVect=false;
#define TYPEDEFV(_id, _sz) static const ClassId id=_id; static const unsigned byteSize=_sz; static const bool isVect=true;

TYPETRAITS<short>             {TYPEDEF(1, 2);};
TYPETRAITS<unsigned short>    {TYPEDEF(2, 2);};
TYPETRAITS<int>               {TYPEDEF(3, 4);};
TYPETRAITS<unsigned int>      {TYPEDEF(4, 4);};
TYPETRAITS<long>              {TYPEDEF(5, 8);};
TYPETRAITS<unsigned long>     {TYPEDEF(6, 8);};
TYPETRAITS<long long>         {TYPEDEF(7, 8);};
TYPETRAITS<unsigned long long>{TYPEDEF(8, 8);};
TYPETRAITS<bool>              {TYPEDEF(9, 1);};
TYPETRAITS<float>             {TYPEDEF(10, 4);};
TYPETRAITS<double>            {TYPEDEF(11, 8);};
TYPETRAITS<const char *>      {TYPEDEF(12, 0);};
TYPETRAITS<std::string>       {TYPEDEF(13, 0);};

TYPETRAITSV<short>>             {TYPEDEFV(1, 2);};
TYPETRAITSV<unsigned short>>    {TYPEDEFV(2, 2);};
TYPETRAITSV<int>>               {TYPEDEFV(3, 4);};
TYPETRAITSV<unsigned int>>      {TYPEDEFV(4, 4);};
TYPETRAITSV<long>>              {TYPEDEFV(5, 8);};
TYPETRAITSV<unsigned long>>     {TYPEDEFV(6, 8);};
TYPETRAITSV<long long>>         {TYPEDEFV(7, 8);};
TYPETRAITSV<unsigned long long>>{TYPEDEFV(8, 8);};
TYPETRAITSV<bool>>              {TYPEDEFV(9, 1);};
TYPETRAITSV<float>>             {TYPEDEFV(10, 4);};
TYPETRAITSV<double>>            {TYPEDEFV(11, 8);};
TYPETRAITSV<const char *>>      {TYPEDEFV(12, 0);};
TYPETRAITSV<std::string>>       {TYPEDEFV(13, 0);};

TYPETRAITSS<short>>             {TYPEDEFV(1, 2);};
TYPETRAITSS<unsigned short>>    {TYPEDEFV(2, 2);};
TYPETRAITSS<int>>               {TYPEDEFV(3, 4);};
TYPETRAITSS<unsigned int>>      {TYPEDEFV(4, 4);};
TYPETRAITSS<long>>              {TYPEDEFV(5, 8);};
TYPETRAITSS<unsigned long>>     {TYPEDEFV(6, 8);};
TYPETRAITSS<long long>>         {TYPEDEFV(7, 8);};
TYPETRAITSS<unsigned long long>>{TYPEDEFV(8, 8);};
TYPETRAITSS<bool>>              {TYPEDEFV(9, 1);};
TYPETRAITSS<float>>             {TYPEDEFV(10, 4);};
TYPETRAITSS<double>>            {TYPEDEFV(11, 8);};
TYPETRAITSS<const char *>>      {TYPEDEFV(12, 0);};
TYPETRAITSS<std::string>>       {TYPEDEFV(13, 0);};

//these assertions must hold because certain elmements are written/read natively
static_assert(sizeof(ClassId) == TypeTraits<ClassId>::byteSize, "ClassId: byteSize must match native size");
static_assert(sizeof(ObjectId) == TypeTraits<ObjectId>::byteSize, "ObjectId: byteSize must match native size");
static_assert(sizeof(PropertyId) == TypeTraits<PropertyId>::byteSize, "PropertyId: byteSize must match native size");
static_assert(sizeof(size_t) == TypeTraits<size_t>::byteSize, "size_t: byteSize must match native size");

class ReadTransaction;
class WriteTransaction;
class PropertyAccessBase;

enum class StoreMode {force_none, force_all, force_buffer, force_property};

enum class StoreLayout {all_embedded, embedded_key, property};

/**
 * abstract superclass for classes that handle serializing mapped values to the datastore
 */
struct StoreAccessBase
{
  const StoreLayout layout;

  StoreAccessBase(StoreLayout layout=StoreLayout::all_embedded) : layout(layout) {}

  virtual size_t size(const byte_t *buf) const = 0;
  virtual size_t size(void *obj, const PropertyAccessBase *pa) {return 0;}

  virtual void save(WriteTransaction *tr,
                    ClassId classId, ObjectId objectId,
                    void *obj, const PropertyAccessBase *pa,
                    StoreMode mode=StoreMode::force_none) = 0;

  virtual void load(ReadTransaction *tr,
                    ReadBuf &buf,
                    ClassId classId, ObjectId objectId,
                    void *obj, const PropertyAccessBase *pa,
                    StoreMode mode=StoreMode::force_none) = 0;

  virtual void * initMember(void *obj, const PropertyAccessBase *pa) {
    return nullptr;
  }
};

/**
 * abstract superclass for all Store Access classes that represent mapped-object valued properties where
 * the referred-to object is saved individually and key value saved in the enclosing object's buffer
 */
struct StoreAccessEmbeddedKey : public StoreAccessBase
{
  StoreAccessEmbeddedKey() : StoreAccessBase(StoreLayout::embedded_key) {}

  size_t size(const byte_t *buf) const override {return StorageKey::byteSize;}
  size_t size(void *obj, const PropertyAccessBase *pa) override {return StorageKey::byteSize;}
};

/**
 * abstract superclass for all Store Access classes that represent mapped-object valued properties that are saved
 * under a property key, with nothing saved in the enclosing object's buffer
 */
struct StoreAccessPropertyKey: public StoreAccessBase
{
  StoreAccessPropertyKey() : StoreAccessBase(StoreLayout::property) {}

  size_t size(const byte_t *buf) const override {return 0;}
  size_t size(void *obj, const PropertyAccessBase *pa) override {return 0;}
};

template<typename T, typename V> struct PropertyStorage : public StoreAccessBase {};

/**
 * base class for value handler templates. Main task is determining storage size and serializing/deserializing
 */
struct ValueTraitsBase {
  const bool fixed;
  ValueTraitsBase(bool fixed) : fixed(fixed) {}
  virtual size_t data_size(const byte_t *) = 0;
};

template <bool Fixed>
struct ValueTraitsFixed : public ValueTraitsBase
{
  ValueTraitsFixed() : ValueTraitsBase(Fixed) {}
};

/**
 * base class for single-byte value handlers
 */
template <typename T>
struct ValueTraitsByte : public ValueTraitsFixed<true>
{
  static size_t size(const T &val) {
    return 1;
  }
  static void getBytes(ReadBuf &buf, T &val) {
    const byte_t *data = buf.read(1);
    val = (T)*data;
  }
  static void putBytes(WriteBuf &buf, T val) {
    byte_t *data = buf.allocate(1);
    *data = (byte_t)val;
  }
};

/**
 * base template value handler for fixed size values
 */
template <typename T>
struct ValueTraits : public ValueTraitsFixed<true>
{
  size_t data_size(const byte_t *) override {
    return TypeTraits<T>::byteSize;
  }
  static size_t size(const T &val) {
    return TypeTraits<T>::byteSize;
  }
  static void getBytes(ReadBuf &buf, T &val) {
    size_t byteSize = TypeTraits<T>::byteSize;
    const byte_t *data = buf.read(byteSize);
    val = read_integer<T>(data, byteSize);
  }
  static void putBytes(WriteBuf &buf, T val) {
    size_t byteSize = TypeTraits<T>::byteSize;
    byte_t *data = buf.allocate(byteSize);
    write_integer(data, val, byteSize);
  }
};

/**
 * value handler specialization for boolean values
 */
template <>
struct ValueTraits<bool> : public ValueTraitsFixed<true>
{
  static size_t size(const bool &val) {
    return TypeTraits<bool>::byteSize;
  }
  static void getBytes(ReadBuf &buf, bool &val) {
    const byte_t *data = buf.read(1);
    val = *data != 0;
  }
  static void putBytes(WriteBuf &buf, bool val) {
    byte_t *data = buf.allocate(1);
    *data = byte_t(val ? 1 : 0);
  }
};

/**
 * value handler specialization for string values
 */
template <>
struct ValueTraits<std::string> : public ValueTraitsFixed<false>
{
  size_t data_size(const byte_t *data) override {
    return strlen((const char *)data) + 1;
  }
  static size_t size(const std::string &val) {
    return val.length() + 1;
  }
  static void getBytes(ReadBuf &buf, std::string &val) {
    val = (const char *)buf.read(0);
    buf.read(val.length() +1); //move the pointer
  }
  static void putBytes(WriteBuf &buf, std::string val) {
    buf.append(val.data(), val.length()+1);
  }
};

/**
 * value handler specialization for C string values
 */
template <>
struct ValueTraits<const char *> : public ValueTraitsFixed<false>
{
  size_t data_size(const byte_t *data) override {
    return strlen((const char *)data) + 1;
  }
  static size_t size(const char * const &val) {
    return strlen(val) + 1;
  }
  static void getBytes(ReadBuf &buf, const char *&val) {
    val = buf.readCString();
  }
  static void putBytes(WriteBuf &buf, const char *&val) {
    buf.appendCString(val);
  }
};

/**
 * value handler base class for float values
 */
template <typename T>
struct ValueTraitsFloat : public ValueTraitsFixed<true>
{
  size_t data_size(const byte_t *data) override {
    return TypeTraits<T>::byteSize;
  }
  static size_t size(const T &val) {
    return TypeTraits<T>::byteSize;
  }
  static void getBytes(ReadBuf &buf, T &val) {
    size_t byteSize = TypeTraits<T>::byteSize;
    const byte_t *data = buf.read(byteSize);
    val = *reinterpret_cast<const T *>(data);
  }
  static void putBytes(WriteBuf &buf, T val) {
    size_t byteSize = TypeTraits<T>::byteSize;
    byte_t *data = buf.allocate(byteSize);
    *reinterpret_cast<T *>(data) = val;
  }
};

#define PROPERTY_TYPE(P) PropertyType(TypeTraits<P>::id, TypeTraits<P>::byteSize, TypeTraits<P>::isVect)

/**
 * value handler specialization for float values
 */
template <>
struct ValueTraits<float> : public ValueTraitsFloat<float> {};
/**
 * value handler specialization for double values
 */
template <>
struct ValueTraits<double> : public ValueTraitsFloat<double> {};

/**
 * non-templated base class for property accessors
 */
struct PropertyAccessBase
{
  const char * const name;
  bool enabled = true;
  ClassId classId;
  unsigned id = 0;
  StoreAccessBase *storage;
  const PropertyType type;
  PropertyAccessBase(const char * name, StoreAccessBase *storage, const PropertyType &type)
      : name(name), storage(storage), type(type) {}
  virtual bool same(void *obj, ObjectId oid) {return false;}
  virtual ~PropertyAccessBase() {delete storage;}

  void *initMember(void *obj) {
    return storage->initMember(obj, this);
  }
};

/**
 * templated abstract superclass for property accessors
 */
template <typename O, typename P>
struct PropertyAccess : public PropertyAccessBase {
  PropertyAccess(const char * name, StoreAccessBase *storage, const PropertyType &type)
      : PropertyAccessBase(name, storage, type) {}
  virtual void set(O &o, P val) const = 0;
  virtual P get(O &o) const = 0;
};

/**
 * property accessor that performs direct assignment
 */
template <typename O, typename P, P O::*p> struct PropertyAssign : public PropertyAccess<O, P> {
  PropertyAssign(const char * name, StoreAccessBase *storage, const PropertyType &type)
      : PropertyAccess<O, P>(name, storage, type) {}
  void set(O &o, P val) const override { o.*p = val;}
  P get(O &o) const override { return o.*p;}
};

/**
 * assignment property accessor for predeclared base types
 */
template <typename O, typename P, P O::*p>
struct BasePropertyAssign : public PropertyAssign<O, P, p> {
  BasePropertyAssign(const char * name)
      : PropertyAssign<O, P, p>(name, new PropertyStorage<O, P>(), PROPERTY_TYPE(P)) {}
};

template <typename T> struct ClassTraits;

/**
 * dummy class
 */
struct EmptyClass
{
};

/**
 * iterates over class property mappings. In an inheritance context, the iteration will start with the topmost
 * class and  run down the hierarchy so that all properties are covered. Single-inheritance only
 */
class Properties
{
  const unsigned keyStorageId;
  const unsigned numProps;
  PropertyAccessBase ** const properties;
  Properties * const superIter;
  const unsigned startPos;

  Properties(PropertyAccessBase * properties[], unsigned numProps, Properties *superIter, unsigned keyStorageId)
      : properties(properties),
        numProps(numProps),
        superIter(superIter),
        startPos(superIter ? superIter->full_size() : 0),
        keyStorageId(keyStorageId)
  {
    fixedSize = 0;
    if(superIter) {
      fixedSize = superIter->fixedSize;
      if(!fixedSize) return;
    }
    for(unsigned i=0; i<numProps; i++) {
      if(properties[i]->enabled) {
        size_t bs = properties[i]->type.byteSize;
        if(!bs) {
          fixedSize = 0;
          return;
        }
        fixedSize += bs;
      }
    }
  }

  Properties(const Properties& mit) = delete;
public:
  size_t fixedSize;

  template <typename T, typename S=EmptyClass>
  static Properties *mk()
  {
    Properties *p = new Properties(
        ClassTraits<T>::decl_props,
        ARRAY_SZ(ClassTraits<T>::decl_props),
        ClassTraits<S>::properties,
        ClassTraits<T>::keyPropertyId);

    for(unsigned i=0; i<p->full_size(); i++)
      p->get(i)->id = i+1;

    return p;
  }

  template <typename O>
  PropertyAccess<O, ObjectId> *objectIdAccess()
  {
    if(keyStorageId)
      return (PropertyAccess<O, ObjectId> *)properties[keyStorageId-1];
    else
      return superIter ? superIter->objectIdAccess<O>() : nullptr;
  }

  inline unsigned full_size() {
    return superIter ? superIter->full_size() + numProps : numProps;
  }

  PropertyAccessBase * get(unsigned index) {
    return index >= startPos ? properties[index-startPos] : superIter->get(index);
  }
};

/**
 * non-templated superclass for ClassInfo
 */
struct AbstractClassInfo {
  static const ClassId MIN_USER_CLSID = 10; //ids below are reserved

  AbstractClassInfo(const AbstractClassInfo &other) = delete;

  const char *name;
  const std::type_info &typeinfo;
  ClassId classId = 0;
  ObjectId maxObjectId = 0;

  std::vector<AbstractClassInfo *> subs;

  AbstractClassInfo(const char *name, const std::type_info &typeinfo, ClassId classId)
      : name(name), typeinfo(typeinfo), classId(classId) {}

  void addSub(AbstractClassInfo *rsub) {
    subs.push_back(rsub);
  }

  bool isPoly() {
    return !subs.empty();
  }

  bool isInstance(ClassId _classId) {
    if(classId == _classId) return true;
    for(auto s : subs) {
      if(s->isInstance(_classId)) return true;
    }
    return false;
  }

  AbstractClassInfo *resolve(ClassId otherClassId)
  {
    if(otherClassId == classId) {
      return this;
    }
    for(auto res : subs) {
      AbstractClassInfo *r = res->resolve(otherClassId);
      if(r) return r;
    }
    return nullptr;
  }

  AbstractClassInfo *resolve(const std::type_info &ti)
  {
      const char *n1 = ti.name();
      const char *n2 = typeinfo.name();
    if(ti == typeinfo) {
      return this;
    }
    for(auto res : subs) {
      AbstractClassInfo *r = res->resolve(ti);
      if(r) return r;
    }
    return nullptr;
  }

  /**
   * search the inheritance tree rooted in this object for a class info with the given classId
   * @return the classinfo
   * @throw persistence_error if not found
   */
  AbstractClassInfo *doresolve(ClassId otherClassId)
  {
    AbstractClassInfo *resolved = resolve(otherClassId);
    if(!resolved) {
      throw persistence_error("unknow classId. Class missing from registry");
    }
    return resolved;
  }

  /**
   * search the inheritance tree rooted in this object for a class info with the given typeid
   * @return the classinfo
   * @throw persistence_error if not found
   */
  AbstractClassInfo *doresolve(const std::type_info &ti)
  {
    AbstractClassInfo *resolved = resolve(ti);
    if(!resolved) {
      throw persistence_error("unknow typeid. Class missing from registry");
    }
    return resolved;
  }
};

namespace sub {

/**
 * a group of structs that resolve the variadic template list used by the ClassInfo#subclass function
 * the list is expanded and the ClassTraits for each type are notified about the subclass
 */

//this one does the real work by adding S as subtype to T
template<typename T, typename S>
struct resolve_impl
{
  bool publish(AbstractClassInfo *res) {
    ClassTraits<S>::info->addSub(res);
    return true;
  }
};

//primary template
template<typename T, typename... Sargs>
struct resolve;

//helper that removes one type arg from the list
template<typename T, typename S, typename... Sargs>
struct resolve_helper
{
  bool publish(AbstractClassInfo *res) {
    if(resolve_impl<T, S>().publish(res)) return true;
    return resolve<T, Sargs...>().publish(res);
  }
};

//template specialization for non-empty list
template<typename T, typename... Sargs>
struct resolve
{
  bool publish(AbstractClassInfo *res) {
    return resolve_helper<T, Sargs...>().publish(res);
  }
};

//template specialization for empty list
template<typename T>
struct resolve<T>
{
  bool publish(AbstractClassInfo *res) {return false;}
};

template <typename T>
struct Substitute {
  virtual T *getPtr() = 0;
};

template <typename T, typename S>
struct SubstituteImpl : public Substitute<T> {
  T *getPtr() override {return new S();}
};

} //sub

/**
 * peer object for the ClassTraitsBase below which contains class metadata and references subclasses
 */
template <typename T, typename ... Sup>
struct ClassInfo : public AbstractClassInfo
{
  T *(* const getSubstitute)();
  size_t (* const size)(T *obj);
  void * (* const initMember)(T *obj, PropertyAccessBase *pa);
  T * (* const makeObject)(ClassId classId);
  Properties * (* const getProperties)(ClassId classId);
  bool (* const add)(T *obj, PropertyAccessBase *pa, size_t &size, unsigned flags);
  bool (* const get_id)(const std::shared_ptr<T> &obj, ObjectId &oid, unsigned flags);
  bool (* const save)(WriteTransaction *wtr,
                      ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode, unsigned flags);
  bool (* const load)(ReadTransaction *tr, ReadBuf &buf,
                      ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode, unsigned flags);

  sub::Substitute<T> *substitute;

  ClassInfo(const char *name, const std::type_info &typeinfo, ClassId classId=MIN_USER_CLSID)
      : AbstractClassInfo(name, typeinfo, classId),
        getSubstitute(&ClassTraits<T>::getSubstitute),
        size(&ClassTraits<T>::size),
        initMember(&ClassTraits<T>::initMember),
        makeObject(&ClassTraits<T>::makeObject),
        getProperties(&ClassTraits<T>::getProperties),
        add(&ClassTraits<T>::add),
        get_id(&ClassTraits<T>::get_id),
        save(&ClassTraits<T>::save),
        load(&ClassTraits<T>::load) {}

  ~ClassInfo() {if(substitute) delete substitute;}

  template <typename S>
  void setSubstitute() {
    substitute = new sub::SubstituteImpl<T, S>();
  }

  template <typename ... Sup2>
  static ClassInfo<T, Sup...> *subclass(const char *name, const std::type_info &typeinfo, ClassId classId=MIN_USER_CLSID)
  {
    //create a classinfo
    return new ClassInfo<T, Sup2...>(name, typeinfo, classId);
  }

  void publish() {
    //make it known to superclasses
    sub::resolve<T, Sup...>().publish(this);
  }
};

#define FIND_CLS(__Tpl, __cid) static_cast<ClassInfo<__Tpl> *>(ClassTraits<__Tpl>::info->resolve(__cid))
#define RESOLVE_SUB(__cid) static_cast<ClassInfo<T> *>(ClassTraits<T>::info->doresolve(__cid))
#define RESOLVE_SUB_TI(__ti) static_cast<ClassInfo<T> *>(ClassTraits<T>::info->doresolve(__ti))

/**
 * base class for class/inheritance resolution infrastructure. Every mapped class is represented by a templated
 * instance of this class. All calls to access/update mapped object properties should go through here and will be
 * dispatched to the correct location
 */
template <typename T, typename SUP=EmptyClass>
class ClassTraitsBase
{
  template <typename, typename ...> friend struct ClassInfo;

  static const unsigned FLAG_UP = 0x1;
  static const unsigned FLAG_DN = 0x2;
  static const unsigned FLAG_HR = 0x4;
  static const unsigned FLAGS_ALL = FLAG_UP | FLAG_DN | FLAG_HR;

#define DN flags & FLAG_DN
#define UP flags & FLAG_UP

  /**
   * determine the buffer size for the given object. Non-polymorpic
   */
  static size_t size(T *obj)
  {
    if(properties->fixedSize) return properties->fixedSize;

    size_t size = 0;
    for(unsigned i=0, sz=properties->full_size(); i<sz; i++) {
      auto pa = properties->get(i);

      if(!pa->enabled) continue;

      add(obj, pa, size);
    }
    return size;
  }

public:
  static const unsigned keyPropertyId = 0;

  static const char *name;
  static ClassInfo<T, SUP> *info;
  static Properties * properties;
  static PropertyAccessBase * decl_props[];
  static const unsigned num_decl_props;

  /**
   * @return the objectid accessor for this class
   */
  static PropertyAccess<T, ObjectId> *objectIdAccess() {
    return properties->objectIdAccess<T>();
  }

  static size_t bufferSize(T *obj, ClassId *clsId=nullptr)
  {
    const std::type_info &ti = typeid(*obj);
    if(ti == info->typeinfo) {
      if(clsId) *clsId = info->classId;
      return size(obj);
    }
    else {
      ClassInfo<T> *sub = RESOLVE_SUB_TI(ti);
      if(clsId) *clsId = sub->classId;
      return sub->size(obj);
    }
  }

  static T *getSubstitute()
  {
    if(info->substitute) return info->substitute->getPtr();

    for(auto &sub : info->subs) {
      ClassInfo<T> * si = static_cast<ClassInfo<T> *>(sub);
      T *subst = si->getSubstitute();
      if(subst != nullptr) return subst;
    }
    return nullptr;
  }

  static void * initMember(T *obj, PropertyAccessBase *pa)
  {
    if(pa->classId == info->classId)
      return pa->initMember(obj);
    else if(pa->classId)
      return RESOLVE_SUB(pa->classId)->initMember(obj, pa);
    return nullptr;
  }

  static Properties * getProperties(ClassId classId)
  {
    if(classId == info->classId)
      return properties;
    else if(classId)
      return RESOLVE_SUB(classId)->getProperties(classId);
    return nullptr;
  }

  static bool add(T *obj, PropertyAccessBase *pa, size_t &size, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId == info->classId) {
      size += pa->storage->size(obj, pa);
      return true;
    }
    else if(pa->classId) {
      if(UP && ClassTraits<SUP>::info->classId != info->classId && ClassTraits<SUP>::add(obj, pa, size, FLAG_UP))
        return true;
      return DN && RESOLVE_SUB(pa->classId)->add(obj, pa, size, FLAG_DN);
    }
    return false;
  }

  static bool get_id(const std::shared_ptr<T> &obj, ObjectId &oid, unsigned flags=FLAGS_ALL)
  {
    if(get_objectid(obj, oid)) {
      return true;
    }
    else {
      if(UP && ClassTraits<SUP>::get_id(obj, oid, FLAG_UP)) return true;
      if(DN) {
        for(auto &sub : info->subs) {
          ClassInfo<T> * si = static_cast<ClassInfo<T> *>(sub);
          if(si->get_id(obj, oid, FLAG_DN)) return true;
        }
      }
    }
    return false;
  }

  static bool save(WriteTransaction *wtr,
                   ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId == info->classId) {
      pa->storage->save(wtr, classId, objectId, obj, pa, mode);
      return true;
    }
    else if(pa->classId) {
      if(UP && ClassTraits<SUP>::info->classId != info->classId && ClassTraits<SUP>::save(wtr, classId, objectId, obj, pa, mode, FLAG_UP))
        return true;
      return DN && RESOLVE_SUB(pa->classId)->save(wtr, classId, objectId, obj, pa, mode, FLAG_DN);
    }
    return false;
  }

  static bool load(ReadTransaction *tr,
                   ReadBuf &buf, ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa,
                   StoreMode mode=StoreMode::force_none, unsigned flags=FLAGS_ALL)
  {
    if(pa->classId == info->classId) {
      pa->storage->load(tr, buf, classId, objectId, obj, pa, mode);
      return true;
    }
    else if(pa->classId) {
      if(UP && ClassTraits<SUP>::info->classId != info->classId && ClassTraits<SUP>::load(tr, buf, classId, objectId, obj, pa, mode, FLAG_UP))
        return true;
      return DN && RESOLVE_SUB(pa->classId)->load(tr, buf, classId, objectId, obj, pa, mode, FLAG_DN);
    }
    return false;
  }

  template <typename TV>
  static void put(T &d, const PropertyAccessBase *pa, TV &value, unsigned flags=FLAGS_ALL) {
    if(pa->classId != info->classId)
      throw persistence_error("internal error: type mismatch");

    const PropertyAccess <T, TV> *acc = (const PropertyAccess <T, TV> *) pa;
    value = acc->get(d);
  }

  /**
   * update the given property using value. Must only be called after type resolution, such
   * that pa->classId == info->classId
   */
  template <typename TV>
  static void get(T &d, const PropertyAccessBase *pa, TV &value, unsigned flags=FLAGS_ALL) {
    if(pa->classId != info->classId)
      throw persistence_error("internal error: type mismatch");

    const PropertyAccess<T, TV> *acc = (const PropertyAccess<T, TV> *)pa;
    acc->set(d, value);
  }
};

/**
 * ClassTraits extension for concrete classes
 */
template <typename T> struct ClassTraitsAbstract
{
  static const bool isAbstract = true;

  static T *makeObject(ClassId classId)
  {
    if(classId == ClassTraits<T>::info->classId) {
      throw persistence_error("abstract class cannot be instantiated");
    }
    else if(classId)
      return RESOLVE_SUB(classId)->makeObject(classId);
    return nullptr;
  }
};

/**
 * ClassTraits extension for abstract classes
 */
template <typename T> struct ClassTraitsConcrete
{
  static const bool isAbstract = false;

  static T *makeObject(ClassId classId)
  {
    if(classId == ClassTraits<T>::info->classId) {
      return new T();
    }
    else if(classId)
      return RESOLVE_SUB(classId)->makeObject(classId);
    return nullptr;
  }
};

/**
 * represents a non-class, e.g. where a mapped superclass must be defined but does not exist
 */
template <>
struct ClassTraits<EmptyClass>
{
  static const bool isAbstract = true;
  static ClassInfo<EmptyClass> *info;
  static Properties * properties;
  static const unsigned num_decl_props = 0;
  static PropertyAccessBase * decl_props[0];

  static EmptyClass *makeObject(ClassId classId) {return nullptr;}
  static Properties * getProperties(ClassId classId) {return nullptr;}

  template <typename T>
  static T *getSubstitute() {
    return nullptr;
  }
  template <typename T>
  static size_t bufferSize(T *obj, ClassId *cid=nullptr) {
    return 0;
  }
  template <typename T>
  static size_t size(T *obj) {
    return 0;
  }
  template <typename T>
  static void * initMember(T *obj, PropertyAccessBase *pa) {
    return nullptr;
  }
  template <typename T>
  static bool add(T *obj, PropertyAccessBase *pa, size_t &size, unsigned flags=0) {
    return false;
  }
  template <typename T>
  static bool get_id(const std::shared_ptr<T> &obj, ObjectId &oid, unsigned flags=0) {
    return false;
  }
  template <typename T>
  static bool save(WriteTransaction *wtr,
                   ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode, unsigned flags=0) {
    return false;
  }
  template <typename T>
  static bool load(ReadTransaction *tr, ReadBuf &buf,
                   ClassId classId, ObjectId objectId, T *obj, PropertyAccessBase *pa, StoreMode mode, unsigned flags=0) {
    return false;
  }
  template <typename T, typename TV>
  static void put(T &d, const PropertyAccessBase *pa, TV &value, unsigned flags) {
  }
  template <typename T, typename TV>
  static void get(T &d, const PropertyAccessBase *pa, TV &value, unsigned flags) {
  }
};

template<typename T>
static PropertyType object_vector_t() {
  using Traits = ClassTraits<T>;
  return PropertyType(Traits::name, true);
}

template<typename T>
static PropertyType object_t() {
  using Traits = ClassTraits<T>;
  return PropertyType(Traits::name);
}

} //kv
} //persistence
} //flexis

using NO_SUPERCLASS = flexis::persistence::kv::EmptyClass;

#endif //FLEXIS_FLEXIS_KVTRAITS_H

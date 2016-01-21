//
// Created by chris on 1/5/16.
//

#ifndef FLEXIS_KVPTR_H
#define FLEXIS_KVPTR_H

namespace flexis {
namespace persistence {
namespace kv {

using ClassId = uint16_t;
using ObjectId = uint32_t;
using PropertyId = uint16_t;

/**
 * Persistent object identifier
 */
struct ObjectKey
{
  static const ObjectKey NIL;

  ClassId classId;
  ObjectId objectId;
  uint16_t refcount; //not serialized

  ObjectKey() : classId(0), objectId(0), refcount(0) {}
  ObjectKey(ClassId classId, ObjectId objectId) : classId(classId), objectId(objectId), refcount(0) {}
  bool isNew() {return objectId == 0;}
  bool isValid() {return classId > 0;}
  bool operator <(const ObjectKey &other) const {
    return classId < other.classId || (classId == other.classId && objectId < other.objectId);
  }
};

/**
 * custom deleter used to store the ObjectKey inside a std::shared_ptr
 */
template <typename T> struct object_handler : public ObjectKey
{
  object_handler(ObjectKey key) : ObjectKey(key.classId, key.objectId) {}
  object_handler(ClassId classId, ObjectId objectId) : ObjectKey(classId, objectId) {}
  object_handler() : ObjectKey(0, 0) {}

  void operator () (T *t) {
    delete t;
  }
};

/**
 * create an object wrapped by a shared_ptr ready to be handed to the KV API. All shared_ptr objects passed
 * to the KV API, unless obtained from KV itself, should be created through this method or through make_ptr
 */
template <typename T, typename... Args>
static auto make_obj(Args&&... args) -> decltype(std::make_shared<T>(std::forward<Args>(args)...))
{
  return std::shared_ptr<T>(new T(std::forward<Args>(args)...), object_handler<T>());
}

/**
 * create a shared_ptr ready to be handed to the KV API. All shared_ptr objects passed to the KV API, unless
 * obtained from KV itself, should be created through this method, or through make_obj
 */
template <typename T>
static std::shared_ptr<T> make_ptr(T *t)
{
  return std::shared_ptr<T>(t, object_handler<T>());
}

} //kv
} //persistence
} //flexis

#endif //FLEXIS_KVPTR_H

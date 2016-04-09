//
// Created by chris on 10/7/15.
//

#ifndef FLEXIS_LMDBSTORE_H
#define FLEXIS_LMDBSTORE_H

#include "../kvstore.h"

namespace flexis {
namespace persistence {
namespace lmdb {

class KeyValueStore : public flexis::persistence::KeyValueStore
{
public:
  struct Options {
    const unsigned initialMapSizeMB = 1;
    const unsigned minTransactionSpaceKB = 512;
    const unsigned increaseMapSizeKB = 512;
    const bool lockFile = false;
    const bool writeMap = true;

    Options(unsigned mapSizeMB = 1024, bool lockFile = false, bool writeMap = false)
        : initialMapSizeMB(mapSizeMB), lockFile(lockFile), writeMap(writeMap) {}
  };

  struct Factory
  {
    const std::string location, name;
    const Options options;

    Factory(std::string location, std::string name, Options options = Options())
        : location(location), name(name), options(options) {}
    operator flexis::persistence::KeyValueStore *() const;
  };
};

/**
 * a storage key. This structure must not be changed (lest db files become unreadable)
 */
struct StorageKey
{
  static const unsigned byteSize = kv::ClassId_sz + kv::ObjectId_sz + kv::PropertyId_sz;

  kv::ClassId classId;
  kv::ObjectId objectId;
  kv::PropertyId propertyId; //will be 0 if this is an object key

  StorageKey() : classId(0), objectId(0), propertyId(0) {}
  StorageKey(kv::ClassId classId, kv::ObjectId objectId, kv::PropertyId propertyId)
      : classId(classId), objectId(objectId), propertyId(propertyId) {}
};

} //lmdb
} //persistence
} //flexis


#endif //FLEXIS_LMDBSTORE_H

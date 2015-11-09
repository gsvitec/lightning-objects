//
// Created by chris on 10/7/15.
//

#ifndef FLEXIS_LMDBSTORE_H
#define FLEXIS_LMDBSTORE_H

#include "../kvstore.h"

namespace flexis {
namespace persistence {
namespace lmdb {

class FlexisPersistence_EXPORT KeyValueStore : public flexis::persistence::KeyValueStore
{
public:
  struct Options {
    const size_t mapSizeMB = 1024;
    const bool lockFile = false;
    const bool writeMap = false;

    Options(size_t mapSizeMB = 1024, bool lockFile = false, bool writeMap = false)
        : mapSizeMB(mapSizeMB), lockFile(lockFile), writeMap(writeMap) {}
  };

  struct FlexisPersistence_EXPORT Factory
  {
    const std::string location, name;
    const Options options;

    Factory(std::string location, std::string name, Options options = Options())
        : location(location), name(name), options(options) {}
    operator flexis::persistence::KeyValueStore *() const;
  };
};

} //lmdb
} //persistence
} //flexis


#endif //FLEXIS_LMDBSTORE_H

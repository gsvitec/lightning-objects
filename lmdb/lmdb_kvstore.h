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
  struct FlexisPersistence_EXPORT Factory
  {
    const std::string location, name;
    Factory(std::string location, std::string name) : location(location), name(name) {}
    operator flexis::persistence::KeyValueStore *() const;
  };
};

} //lmdb
} //persistence
} //flexis


#endif //FLEXIS_LMDBSTORE_H

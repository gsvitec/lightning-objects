//
// Created by chris on 10/7/15.
//

#ifndef FLEXIS_LMDBSTORE_H
#define FLEXIS_LMDBSTORE_H

#include <kvstore.h>

namespace flexis {
namespace persistence {
namespace lmdb {

class FlexisPersistence_EXPORT KeyValueStore : public flexis::persistence::KeyValueStore
{
public:
  struct FlexisPersistence_EXPORT Factory : public flexis::persistence::KeyValueStore::Factory
  {
    const std::string name;
    Factory(std::string name) : name(name) {}
    flexis::persistence::KeyValueStore *make(std::string location) const override;
  };
};

} //lmdb
} //persistence
} //flexis


#endif //FLEXIS_LMDBSTORE_H

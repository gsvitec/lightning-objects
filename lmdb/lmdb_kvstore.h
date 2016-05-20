/*
 * LightningObjects C++ Object Storage based on Key/Value API
 *
 * Copyright (C) 2016 GS Vitec GmbH <christian@gsvitec.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, and provided
 * in the LICENSE file in the root directory of this software.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
    const kv::StoreId storeId;
    const std::string location, name;
    const Options options;

    Factory(kv::StoreId storeId, std::string location, std::string name, Options options = Options())
        : storeId(storeId), location(location), name(name), options(options) {}
    operator flexis::persistence::KeyValueStore *() const;
  };

protected:
  KeyValueStore(kv::StoreId storeId) : flexis::persistence::KeyValueStore(storeId) {}
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

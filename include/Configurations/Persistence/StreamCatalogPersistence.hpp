/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NEBULASTREAM_STREAMCATALOGPERSISTENCE_H
#define NEBULASTREAM_STREAMCATALOGPERSISTENCE_H

#include <map>
#include <memory>

namespace NES {
class StreamCatalogPersistence;
typedef std::shared_ptr<StreamCatalogPersistence> StreamCatalogPersistencePtr;

/**
 * StreamCatalog persistence base class
 */
class StreamCatalogPersistence {
  public:
    /**
     * Persists a logical stream
     * @param logicalStreamName the name of the logical stream
     * @param schema the schema of the logical stream as string
     * @return true when persisting was successful
     */
    virtual bool persistLogicalStream(const std::string& logicalStreamName, const std::string& schema) = 0;

    /**
     * Deletes or archives a logical stream in the persistence
     * @param logicalStreamName The name of the logical stream to delete
     * @return true when deleting was successful
     */
    virtual bool deleteLogicalStream(const std::string& logicalStreamName) = 0;

    /**
     * Loads all logical streams from the persistence
     * @return A map containing all logical stream names and their schemas as key/value pairs
     */
    virtual std::map<std::string, std::string> loadLogicalStreams() = 0;
};
}// namespace NES
#endif//NEBULASTREAM_STREAMCATALOGPERSISTENCE_H

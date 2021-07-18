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

class StreamCatalogPersistence {
  public:
    virtual bool persistLogicalStream(const std::string& logicalStreamName, const std::string& schema) = 0;
    virtual bool deleteLogicalStream(const std::string& logicalStreamName) = 0;

    virtual std::map<std::string, std::string> loadLogicalStreams() = 0;
};
}// namespace NES
#endif//NEBULASTREAM_STREAMCATALOGPERSISTENCE_H

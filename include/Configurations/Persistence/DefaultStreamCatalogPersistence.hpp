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

#ifndef NEBULASTREAM_DEFAULTSTREAMCATALOGPERSISTENCE_H
#define NEBULASTREAM_DEFAULTSTREAMCATALOGPERSISTENCE_H
#include <Configurations/Persistence/StreamCatalogPersistence.hpp>

namespace NES {
class DefaultStreamCatalogPersistence;
typedef std::shared_ptr<DefaultStreamCatalogPersistence> DefaultStreamCatalogPersistencePtr;

/**
 * Default persistence for the stream catalog
 */
class DefaultStreamCatalogPersistence : public StreamCatalogPersistence {
  public:
    static DefaultStreamCatalogPersistencePtr create();

    bool persistLogicalStream(const std::string& logicalStreamName, const std::string& schema) override;
    bool deleteLogicalStream(const std::string& logicalStreamName) override;

    std::map<std::string, std::string> loadLogicalStreams() override;
};

}// namespace NES

#endif//NEBULASTREAM_DEFAULTSTREAMCATALOGPERSISTENCE_H

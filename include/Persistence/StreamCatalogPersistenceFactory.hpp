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

#ifndef NES_STREAMCATALOGPERSISTENCEFACTORY_H
#define NES_STREAMCATALOGPERSISTENCEFACTORY_H

#include <Persistence/DefaultStreamCatalogPersistence.hpp>
#include <Persistence/FileStreamCatalogPersistence.hpp>
namespace NES {
class StreamCatalogPersistenceFactory {
  public:
    static StreamCatalogPersistencePtr createForType(const std::string& persistenceType, const std::string& persistenceDir) {
        if (persistenceType == "file") {
            return std::make_shared<FileStreamCatalogPersistence>(persistenceDir);
        } else {
            return std::make_shared<DefaultStreamCatalogPersistence>();
        }
    }
};
}// namespace NES
#endif//NES_STREAMCATALOGPERSISTENCEFACTORY_H

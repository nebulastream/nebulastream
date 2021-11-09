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

#include <Configurations/Persistence/DefaultStreamCatalogPersistence.hpp>
#include <Configurations/Persistence/FileStreamCatalogPersistence.hpp>
namespace NES {
enum class StreamCatalogPersistenceType { NONE, FILE };

/**
 * Factory class to create stream catalog persistence
 */
class StreamCatalogPersistenceFactory {
  public:
    /**
     * Helper method to create StreamCatalogPersistenceType enum from a given argument string
     * @param persistenceTypeStr The type as string representation
     * @return an enum value representing the persistence type
     */
    static StreamCatalogPersistenceType getTypeForString(const std::string& persistenceTypeStr) {
        if (persistenceTypeStr == "file") {
            return StreamCatalogPersistenceType::FILE;
        }
        return StreamCatalogPersistenceType::NONE;
    }

    /**
     * Creates a persistence for the given persistence type and optional further configuration
     * @param persistenceType The given persistence type
     * @param persistenceDir An optional configuration for the persistence
     * @return The persistence instance for the given type
     */
    static StreamCatalogPersistencePtr createForType(StreamCatalogPersistenceType persistenceType,
                                                     const std::string& persistenceDir) {
        if (persistenceType == StreamCatalogPersistenceType::FILE) {
            return std::make_shared<FileStreamCatalogPersistence>(persistenceDir);
        }
        return std::make_shared<DefaultStreamCatalogPersistence>();
    }
};
}// namespace NES
#endif//NES_STREAMCATALOGPERSISTENCEFACTORY_H

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

#ifndef NES_PHYSICALSTREAMSPERSISTENCEFACTORY_H
#define NES_PHYSICALSTREAMSPERSISTENCEFACTORY_H
#include <Configurations/Persistence/DefaultPhysicalStreamsPersistence.hpp>
#include <Configurations/Persistence/FilePhysicalStreamsPersistence.hpp>
namespace NES {
enum PhysicalStreamsPersistenceType {
    NONE,
    FILE
};

/**
 * Factory class to create physical stream persistence
 */
class PhysicalStreamsPersistenceFactory {
  public:
    /**
     * Helper method to get a persistence type enum for a given parameter string
     * @param persistenceTypeStr The string representation of the persistence type
     * @return An enum representing the persistence type
     */
    static PhysicalStreamsPersistenceType getTypeForString(const std::string& persistenceTypeStr) {
        if (persistenceTypeStr == "file") {
            return PhysicalStreamsPersistenceType::FILE;
        }
        return PhysicalStreamsPersistenceType::NONE;
    }

    /**
     * Creates an instance of the persistence given the corresponding type and optional configuration
     * @param persistenceType The type of the persistence to craete
     * @param persistenceDir The optional configuration of the persitence
     * @return A persistence object
     */
    static PhysicalStreamsPersistencePtr createForType(PhysicalStreamsPersistenceType persistenceType,
                                                     const std::string& persistenceDir) {
        if (persistenceType == PhysicalStreamsPersistenceType::FILE) {
            return std::make_shared<FilePhysicalStreamsPersistence>(persistenceDir);
        }
        return std::make_shared<DefaultPhysicalStreamsPersistence>();
    }
};
}// namespace NES
#endif//NES_PHYSICALSTREAMSPERSISTENCEFACTORY_H

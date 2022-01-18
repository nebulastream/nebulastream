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

#ifndef NES_PHYSICALSOURCE_HPP
#define NES_PHYSICALSOURCE_HPP

#include <memory>
#include <string>

namespace NES {

class PhysicalSourceType;
using PhysicalSourceTypePtr = std::shared_ptr<PhysicalSourceType>;

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

/**
 * @brief Container for storing all configurations for physical stream
 */
class PhysicalSource {

  public:
    /**
     * @brief create method to construct physical Source
     * @param logicalSourceName : logical source name
     * @param physicalSourceName : physical source name
     * @param physicalSourceType : physical source type
     * @return shared pointer to a physical source
     */
    static PhysicalSourcePtr
    create(std::string logicalSourceName, std::string physicalSourceName, PhysicalSourceTypePtr physicalSourceType);

    const std::string& getLogicalSourceName() const;

    const std::string& getPhysicalSourceName() const;

    const PhysicalSourceTypePtr& getPhysicalSourceType() const;

    std::string toString();

  private:
    explicit PhysicalSource(std::string logicalSourceName,
                            std::string physicalSourceName,
                            PhysicalSourceTypePtr physicalSourceType);

    std::string logicalSourceName;
    std::string physicalSourceName;
    PhysicalSourceTypePtr physicalSourceType;
};

}// namespace NES

#endif//NES_PHYSICALSOURCE_HPP

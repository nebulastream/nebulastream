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

#ifndef NES_PHYSICALSTREAM_HPP
#define NES_PHYSICALSTREAM_HPP

#include <memory>
#include <string>

namespace NES {

namespace Configuration {

class PhysicalStreamTypeConfiguration;
using PhysicalStreamTypeConfigurationPtr = std::shared_ptr<PhysicalStreamTypeConfiguration>;

class PhysicalStream;
using PhysicalStreamPtr = std::shared_ptr<PhysicalStream>;

/**
 * @brief Container for storing all configurations for physical stream
 */
class PhysicalStream {

  public:

    static PhysicalStreamPtr create(std::string logicalStream, std::string physicalStream, );

    const std::string& getLogicalStreamName() const { return logicalStreamName; }
    void setLogicalStreamName(const std::string& logicalStreamName) { PhysicalStream::logicalStreamName = logicalStreamName; }

    const std::string& getPhysicalStreamName() const { return physicalStreamName; }
    void setPhysicalStreamName(const std::string& physicalStreamName) { PhysicalStream::physicalStreamName = physicalStreamName; }

    const PhysicalStreamTypeConfigurationPtr& getPhysicalStreamTypeConfiguration() const {
        return physicalStreamTypeConfiguration;
    }
    void setPhysicalStreamTypeConfiguration(const PhysicalStreamTypeConfigurationPtr& physicalStreamTypeConfiguration) {
        PhysicalStream::physicalStreamTypeConfiguration = physicalStreamTypeConfiguration;
    }

    std::string toString();

  private:
    std::string logicalStreamName;
    std::string physicalStreamName;
    PhysicalStreamTypeConfigurationPtr physicalStreamTypeConfiguration;
};

}// namespace Configuration

}// namespace NES

#endif//NES_PHYSICALSTREAM_HPP

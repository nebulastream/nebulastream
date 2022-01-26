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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <sstream>

namespace NES {

PhysicalSource::PhysicalSource(std::string logicalSourceName,
                               std::string physicalSourceName,
                               PhysicalSourceTypePtr physicalSourceType)
    : logicalSourceName(logicalSourceName), physicalSourceName(physicalSourceName),
      physicalSourceType(std::move(physicalSourceType)) {}

PhysicalSourcePtr
PhysicalSource::create(std::string logicalSourceName, std::string physicalSourceName, PhysicalSourceTypePtr physicalSourceType) {
    return std::make_shared<PhysicalSource>(
        PhysicalSource(std::move(logicalSourceName), std::move(physicalSourceName), std::move(physicalSourceType)));
}

PhysicalSourcePtr PhysicalSource::create(std::string logicalSourceName, std::string physicalSourceName) {
    return std::make_shared<PhysicalSource>(PhysicalSource(std::move(logicalSourceName), std::move(physicalSourceName), nullptr));
}

std::string PhysicalSource::toString() {
    std::stringstream ss;
    ss << "PhysicalSource Name: " << physicalSourceName;
    ss << "LogicalSource Name" << logicalSourceName;
    ss << "Stream Type" << physicalSourceType->toString();
    return ss.str();
}

const std::string& PhysicalSource::getLogicalSourceName() const { return logicalSourceName; }

const std::string& PhysicalSource::getPhysicalSourceName() const { return physicalSourceName; }

const PhysicalSourceTypePtr& PhysicalSource::getPhysicalSourceType() const { return physicalSourceType; }
}// namespace NES

/*
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
#include <sstream>
#include <utility>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Operator.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>

namespace NES
{

PhysicalSource::PhysicalSource(std::string logicalSourceName, PhysicalSourceTypePtr physicalSourceType)
    : logicalSourceName(std::move(logicalSourceName)), physicalSourceType(std::move(physicalSourceType))
{
}

PhysicalSourcePtr PhysicalSource::create(PhysicalSourceTypePtr physicalSourceType)
{
    auto logicalSourceName = physicalSourceType->getLogicalSourceName();
    return std::make_shared<PhysicalSource>(PhysicalSource(logicalSourceName, std::move(physicalSourceType)));
}

PhysicalSourcePtr PhysicalSource::create(std::string logicalSourceName)
{
    return std::make_shared<PhysicalSource>(PhysicalSource(std::move(logicalSourceName), nullptr));
}

std::string PhysicalSource::toString()
{
    std::stringstream ss;
    ss << "LogicalSource Name" << logicalSourceName;
    ss << "Source Type" << physicalSourceType->toString();
    return ss.str();
}

const std::string& PhysicalSource::getLogicalSourceName() const
{
    return logicalSourceName;
}

const PhysicalSourceTypePtr& PhysicalSource::getPhysicalSourceType() const
{
    return physicalSourceType;
}
} /// namespace NES

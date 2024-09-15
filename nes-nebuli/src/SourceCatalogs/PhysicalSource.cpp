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
#include <Identifiers/Identifiers.hpp>
#include <Operators/Operator.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>

namespace NES
{

PhysicalSource::PhysicalSource(std::string logicalSourceName, Sources::DescriptorSource&& DescriptorSource)
    : logicalSourceName(std::move(logicalSourceName)), descriptorSource(DescriptorSource)
{
}

std::shared_ptr<PhysicalSource> PhysicalSource::create(Sources::DescriptorSource&& descriptorSource)
{
    const auto logicalSourceName = descriptorSource.logicalSourceName;
    return std::make_shared<PhysicalSource>(PhysicalSource(logicalSourceName, std::move(descriptorSource)));
}

std::string PhysicalSource::toString()
{
    std::stringstream ss;
    ss << "LogicalSource Name" << logicalSourceName;
    ss << "Source Type" << descriptorSource;
    return ss.str();
}

const std::string& PhysicalSource::getLogicalSourceName() const
{
    return logicalSourceName;
}

std::unique_ptr<Sources::DescriptorSource> PhysicalSource::createDescriptorSource(std::shared_ptr<Schema> schema)
{
    auto copyOfConfig = descriptorSource.config;
    return std::make_unique<Sources::DescriptorSource>(
        schema, descriptorSource.logicalSourceName, descriptorSource.sourceType, descriptorSource.inputFormat, std::move(copyOfConfig));
}
} /// namespace NES

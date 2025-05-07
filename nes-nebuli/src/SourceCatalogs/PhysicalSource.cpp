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

#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include <API/Schema.hpp>
#include <InputFormatters/InputFormatterDescriptor.hpp>
#include <SourceCatalogs/PhysicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

PhysicalSource::PhysicalSource(
    std::string logicalSourceName,
    const Sources::SourceDescriptor& sourceDescriptor,
    const InputFormatters::InputFormatterDescriptor& inputFormatterDescriptor)
    : logicalSourceName(std::move(logicalSourceName))
    , sourceDescriptor(sourceDescriptor)
    , inputFormatterDescriptor(inputFormatterDescriptor)
{
}

std::shared_ptr<PhysicalSource> PhysicalSource::create(
    const Sources::SourceDescriptor& sourceDescriptor, const InputFormatters::InputFormatterDescriptor& inputFormatterDescriptor)
{
    const auto logicalSourceName = sourceDescriptor.getLogicalSourceName();
    return std::make_shared<PhysicalSource>(PhysicalSource(logicalSourceName, sourceDescriptor, inputFormatterDescriptor));
}

std::string PhysicalSource::toString()
{
    std::stringstream ss;
    ss << "LogicalSource Name" << logicalSourceName;
    ss << "Source Type" << sourceDescriptor;
    return ss.str();
}

const std::string& PhysicalSource::getLogicalSourceName() const
{
    return logicalSourceName;
}

std::unique_ptr<Sources::SourceDescriptor> PhysicalSource::createSourceDescriptor(std::shared_ptr<Schema> schema)
{
    auto copyOfConfig = sourceDescriptor.config;
    return std::make_unique<Sources::SourceDescriptor>(
        std::move(schema),
        sourceDescriptor.getLogicalSourceName(),
        sourceDescriptor.getSourceType(),
        sourceDescriptor.getNumberOfBuffersInSourceLocalBufferPool(),
        std::move(copyOfConfig));
}

std::unique_ptr<InputFormatters::InputFormatterDescriptor> PhysicalSource::createInputFormatterDescriptor(std::shared_ptr<Schema> schema)
{
    auto copyOfConfig = inputFormatterDescriptor.config;
    return std::make_unique<InputFormatters::InputFormatterDescriptor>(
        std::move(schema),
        inputFormatterDescriptor.getInputFormatterType(),
        inputFormatterDescriptor.getHasSpanningTuples(),
        std::move(copyOfConfig));
}
}

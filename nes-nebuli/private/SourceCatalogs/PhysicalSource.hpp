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
#pragma once

#include <memory>
#include <string>
#include <InputFormatters/InputFormatterDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// Container for storing all configurations for physical source
class PhysicalSource
{
public:
    static std::shared_ptr<PhysicalSource>
    create(const Sources::SourceDescriptor& sourceDescriptor, const InputFormatters::InputFormatterDescriptor& inputFormatterDescriptor);

    const std::string& getLogicalSourceName() const;

    std::unique_ptr<Sources::SourceDescriptor> createSourceDescriptor(std::shared_ptr<Schema> schema);
    std::unique_ptr<InputFormatters::InputFormatterDescriptor> createInputFormatterDescriptor(std::shared_ptr<Schema> schema);

    std::string toString();

private:
    explicit PhysicalSource(
        std::string logicalSourceName,
        const Sources::SourceDescriptor& sourceDescriptor,
        const InputFormatters::InputFormatterDescriptor& inputFormatterDescriptor);

    std::string logicalSourceName;
    Sources::SourceDescriptor sourceDescriptor;
    InputFormatters::InputFormatterDescriptor inputFormatterDescriptor;
};

}

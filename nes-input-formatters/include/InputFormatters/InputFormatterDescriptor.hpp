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
#include <ostream>
#include <string>

#include <API/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES::InputFormatters
{

class InputFormatterDescriptor : public Configurations::Descriptor
{
public:
    /// Per default, we set an 'invalid' number of buffers in source local buffer pool.
    /// Given an invalid value, the NodeEngine takes its configured value. Otherwise the source-specific configuration takes priority.
    static constexpr int INVALID_NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL = -1;

    /// Used by Sources to create a valid InputFormatterDescriptor.
    explicit InputFormatterDescriptor(
        std::shared_ptr<Schema> schema,
        std::string inputFormatterType,
        bool hasSpanningTuples,
        Configurations::DescriptorConfig::Config config);
    ~InputFormatterDescriptor() = default;

    [[nodiscard]] std::shared_ptr<Schema> getSchema() const { return this->schema; }
    [[nodiscard]] std::string getInputFormatterType() const { return this->inputFormatterType; }
    [[nodiscard]] bool getHasSpanningTuples() const { return this->hasSpanningTuples; }

private:
    /// The below members should remain immutable, i.e., there should be no setters or other functions that change the members
    std::shared_ptr<Schema> schema;
    std::string inputFormatterType;
    bool hasSpanningTuples;

    friend std::ostream& operator<<(std::ostream& out, const InputFormatterDescriptor& inputFormatterDescriptor);
    friend bool operator==(const InputFormatterDescriptor& lhs, const InputFormatterDescriptor& rhs) = default;
};

}

FMT_OSTREAM(NES::InputFormatters::InputFormatterDescriptor);

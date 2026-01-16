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


#include <cctype>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <SerializableOperator.pb.h>

#include <Nautilus/Interface/TupleBufferRef/LowerSchemaProvider.hpp>

namespace NES
{
class OperatorSerializationUtil;
}

namespace NES
{
class TupleBufferRefDescriptor final : public Descriptor
{
    friend OperatorSerializationUtil;

public:
    ~TupleBufferRefDescriptor() = default;


    [[nodiscard]] SerializableTupleBufferRefDescriptor serialize() const;
    friend std::ostream& operator<<(std::ostream& out, const TupleBufferRefDescriptor& tupleBufferRefDescriptor);
    friend bool operator==(const TupleBufferRefDescriptor& lhs, const TupleBufferRefDescriptor& rhs);

    // [[nodiscard]] std::string_view getFormatType() const;
    [[nodiscard]] MemoryLayoutType getTupleBufferRefType() const { return tupleBufferRefType; }
    [[nodiscard]] const DescriptorConfig::Config& getConfig() const { return config; }
    // [[nodiscard]] std::shared_ptr<const Schema> getSchema() const;
    // [[nodiscard]] std::string getTupleBufferRefName() const;
    // [[nodiscard]] bool isInline() const;

private:
    explicit TupleBufferRefDescriptor(MemoryLayoutType tupleBufferRefType, DescriptorConfig::Config config);

    MemoryLayoutType tupleBufferRefType;
    DescriptorConfig::Config config;

public:
    /// NOLINTNEXTLINE(cert-err58-cpp)
    // static inline const DescriptorConfig::ConfigParameter<EnumWrapper, InputFormat> INPUT_FORMAT{
    //     "input_format",
    //     std::nullopt,
    //     [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INPUT_FORMAT, config); }};


    friend struct TupleBufferRefLogicalOperator;
};
}

template <>
struct std::hash<NES::TupleBufferRefDescriptor>
{
    size_t operator()(const NES::TupleBufferRefDescriptor& tupleBufferRefDescriptor) const noexcept
    {
        return std::hash<NES::MemoryLayoutType>{}(tupleBufferRefDescriptor.getTupleBufferRefType());
    }
};

FMT_OSTREAM(NES::TupleBufferRefDescriptor);

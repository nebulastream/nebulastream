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

#include <Nautilus/Interface/TupleBufferRef/TupleBufferRefDescriptor.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>

#include <ErrorHandling.hpp>
#include <ProtobufHelper.hpp> /// NOLINT
#include <SerializableOperator.pb.h>

namespace NES
{

TupleBufferRefDescriptor::TupleBufferRefDescriptor(const MemoryLayoutType tupleBufferRefType, DescriptorConfig::Config config)
    : Descriptor(std::move(config)), tupleBufferRefType(tupleBufferRefType)
{
}


std::optional<DescriptorConfig::Config>
TupleBufferRefDescriptor::validateAndFormatConfig(const MemoryLayoutType tupleBufferRefType, std::unordered_map<std::string, std::string> configPairs)
{
    auto tupleBufferRefValidationRegistryArguments = TupleBufferRefValidationRegistryArguments{std::move(configPairs)};
    return TupleBufferRefValidationRegistry::instance().create(std::string{tupleBufferRefType}, std::move(tupleBufferRefValidationRegistryArguments));
}

std::ostream& operator<<(std::ostream& out, const TupleBufferRefDescriptor& tupleBufferRefDescriptor)
{
    out << fmt::format(
        "TupleBufferRefDescriptor: (name: {}, type: {}, Config: {})",
        tupleBufferRefDescriptor.tupleBufferRefName,
        tupleBufferRefDescriptor.tupleBufferRefType,
        tupleBufferRefDescriptor.toStringConfig());
    return out;
}

bool operator==(const TupleBufferRefDescriptor& lhs, const TupleBufferRefDescriptor& rhs)
{
    return lhs.tupleBufferRefName == rhs.tupleBufferRefName;
}

SerializableTupleBufferRefDescriptor TupleBufferRefDescriptor::serialize() const
{
    SerializableTupleBufferRefDescriptor serializedTupleBufferRefDescriptor;
    serializedTupleBufferRefDescriptor.set_tupleBufferRefname(getTupleBufferRefName());
    SchemaSerializationUtil::serializeSchema(*schema, serializedTupleBufferRefDescriptor.mutable_tupleBufferRefschema());
    serializedTupleBufferRefDescriptor.set_tupleBufferReftype(tupleBufferRefType);
    /// Iterate over TupleBufferRefDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : getConfig())
    {
        auto* kv = serializedTupleBufferRefDescriptor.mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    return serializedTupleBufferRefDescriptor;
}

}

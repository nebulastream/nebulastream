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

#include <JSONOutputFormatter.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <fmt/format.h>
#include <std/cstring.h>

#include <Configurations/Descriptor.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <CompilationContext.hpp>
#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

JSONOutputFormatter::JSONOutputFormatter(
    const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(fieldNames)
    , fieldPrefixes(
          fieldNames | std::views::transform([](const auto& id) { return fmt::format("\"{}\":", id); }) | std::ranges::to<std::vector>())
{
    serializerTypes[DataType::Type::UINT8] = "DefaultUINT8";
    serializerTypes[DataType::Type::UINT16] = "DefaultUINT16";
    serializerTypes[DataType::Type::UINT32] = "DefaultUINT32";
    serializerTypes[DataType::Type::UINT64] = "DefaultUINT64";
    serializerTypes[DataType::Type::INT8] = "DefaultINT8";
    serializerTypes[DataType::Type::INT16] = "DefaultINT16";
    serializerTypes[DataType::Type::INT32] = "DefaultINT32";
    serializerTypes[DataType::Type::INT64] = "DefaultINT64";
    serializerTypes[DataType::Type::FLOAT32] = "DefaultF32";
    serializerTypes[DataType::Type::FLOAT64] = "DefaultF64";
    serializerTypes[DataType::Type::BOOLEAN] = "DefaultBOOL";
    serializerTypes[DataType::Type::CHAR] = "JSONCHAR";
    serializerTypes[DataType::Type::VARSIZED] = "JSONVARSIZED";

    /// Override the datatype defaults for the fields that the user configured a serializer for
    fieldSerializerTypes
        = parseValueSerializerOverrides(descriptor.getFromConfig(OutputFormatterDescriptor::VALUE_SERIALIZERS), this->fieldNames);

    /// The opening brace belongs to the first field's prefix, so every field stays a single write.
    fieldPrefixes.front().insert(0, "{");

    createSerializers(ValueSerializerConfig{.quoted = true});
}

nautilus::val<uint64_t> JSONOutputFormatter::writeFormattedValue(
    CompilationContext& compilationContext,
    const VarVal& value,
    const DataType& fieldType,
    const uint64_t fieldIndex,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    /// Prefix, null literal and delimiter are trace-time constants for this column, so the serializer emits the
    /// whole field -- '"name":value,' -- in one call and one buffer write.
    const bool isLastField = fieldIndex == fieldNames.size() - 1;
    const nautilus::val<bool> isNull = value.isNullable() ? value.isNull() : nautilus::val<bool>{false};
    return getSerializerAt(fieldIndex, fieldNames.at(fieldIndex), fieldType.type)
        .serializeAndWrite(
            compilationContext,
            value,
            isNull,
            nautilus::val<const char*>{fieldPrefixes.at(fieldIndex).c_str()},
            nautilus::val<const char*>{"null"},
            nautilus::val<const char*>{isLastField ? "}\n" : ","},
            remainingSize,
            recordBuffer,
            bufferProvider,
            fieldPointer);
}

DescriptorConfig::Config JSONOutputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<OutputFormatterConfig::ConfigParametersJSON>(std::move(config), "JSON");
}

std::ostream& operator<<(std::ostream& out, const JSONOutputFormatter&)
{
    return out << fmt::format("JSONOutputFormatter()");
}

OutputFormatterValidationRegistryReturnType
OutputFormatterValidationGeneratedRegistrar::RegisterJSONOutputFormatterValidation(OutputFormatterValidationRegistryArguments args)
{
    return JSONOutputFormatter::validateAndFormat(std::move(args.config));
}

OutputFormatterRegistryReturnType OutputFormatterGeneratedRegistrar::RegisterJSONOutputFormatter(OutputFormatterRegistryArguments args)
{
    return std::make_unique<JSONOutputFormatter>(std::move(args.fieldNames), std::move(args.descriptor));
}
}

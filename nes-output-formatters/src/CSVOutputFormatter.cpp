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

#include <CSVOutputFormatter.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <fmt/format.h>
#include <std/cstring.h>

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

CSVOutputFormatter::CSVOutputFormatter(
    const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(fieldNames)
    , fieldDelimiter(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::FIELD_DELIMITER))
    , tupleDelimiter(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::TUPLE_DELIMITER))
    , quoteStrings(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersCSV::QUOTE_STRINGS))
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
    serializerTypes[DataType::Type::CHAR] = "DefaultCHAR";
    serializerTypes[DataType::Type::VARSIZED] = "DefaultVARSIZED";

    /// Override the datatype defaults for the fields that the user configured a serializer for
    fieldSerializerTypes
        = parseValueSerializerOverrides(descriptor.getFromConfig(OutputFormatterDescriptor::VALUE_SERIALIZERS), this->fieldNames);

    createSerializers(ValueSerializerConfig{.quoted = quoteStrings});
}

nautilus::val<uint64_t> CSVOutputFormatter::writeFormattedValue(
    CompilationContext& compilationContext,
    const VarVal& value,
    const DataType& fieldType,
    const uint64_t fieldIndex,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    /// A trace-time constant: the tuple delimiter after the last field, the field delimiter otherwise. Both are
    /// members of this formatter, which outlives the pipeline.
    const char* delimiter = (fieldIndex == fieldNames.size() - 1) ? tupleDelimiter.c_str() : fieldDelimiter.c_str();

    /// The serializer writes value-or-NULL and the delimiter in one go, so nothing here branches on isNull.
    const nautilus::val<bool> isNull = value.isNullable() ? value.isNull() : nautilus::val<bool>{false};
    return getSerializerAt(fieldIndex, fieldNames.at(fieldIndex), fieldType.type)
        .serializeAndWrite(
            compilationContext,
            value,
            isNull,
            nautilus::val<const char*>{""},
            nautilus::val<const char*>{"NULL"},
            nautilus::val<const char*>{delimiter},
            remainingSize,
            recordBuffer,
            bufferProvider,
            fieldPointer);
}

std::ostream& operator<<(std::ostream& out, const CSVOutputFormatter& format)
{
    return out << fmt::format("CSVOutputFormatter(Field Delimiter: {}, Tuple Delimiter: {})", format.fieldDelimiter, format.tupleDelimiter);
}

DescriptorConfig::Config CSVOutputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<OutputFormatterConfig::ConfigParametersCSV>(std::move(config), "CSV");
}

OutputFormatterValidationRegistryReturnType
OutputFormatterValidationGeneratedRegistrar::RegisterCSVOutputFormatterValidation(OutputFormatterValidationRegistryArguments args)
{
    return CSVOutputFormatter::validateAndFormat(args.config);
}

OutputFormatterRegistryReturnType OutputFormatterGeneratedRegistrar::RegisterCSVOutputFormatter(OutputFormatterRegistryArguments args)
{
    return std::make_unique<CSVOutputFormatter>(std::move(args.fieldNames), std::move(args.descriptor));
}

}

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
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <magic_enum/magic_enum.hpp>

#include <Runtime/AbstractBufferProvider.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// The output formatter is responsible for converting a Record into a string of a given Output Format like CSV or JSON
/// Output formatters are stateless, meaning that they must be able to produce valid output in a streaming fashion.
/// Output formatters are used by the OutputFormatterBufferRef during the last emit before a sink.
class OutputFormatter
{
public:
    explicit OutputFormatter(const std::vector<Record::RecordFieldIdentifier>& fieldNames) : fieldNames(fieldNames)
    {
        INVARIANT(!fieldNames.empty(), "Schema is not allowed to have 0 fields");
    }

    virtual ~OutputFormatter() noexcept = default;

    /// Format the field's contents into a string and write the string bytes into the field pointer address.
    /// If the formatted value does not fit into the record buffer, child buffers will be allocated to write the value into.
    /// The main buffer's space will always be utilized completely before writing in a child.
    /// Returns the number of bytes written into the record buffer (excluding the bytes written into the children).
    /// The CompilationContext lets formatters trace the per-type formatting once instead of per column.
    [[nodiscard]] virtual nautilus::val<uint64_t> writeFormattedValue(
        CompilationContext& compilationContext,
        const VarVal& value,
        const DataType& fieldType,
        uint64_t fieldIndex,
        const nautilus::val<int8_t*>& fieldPointer,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
        = 0;

    virtual std::ostream& toString(std::ostream&) const = 0;

    friend std::ostream& operator<<(std::ostream& os, const OutputFormatter& obj);

    /// The serializer for a field, by its index in the output schema. Resolved in setup(), so the steady-state path
    /// is a vector index rather than the hash lookups getSerializer() needs. Falls back to those when the table is
    /// empty, which is the case for a formatter constructed without a setup().
    [[nodiscard]] const ValueSerializer&
    getSerializerAt(const size_t fieldIndex, const Record::RecordFieldIdentifier& fieldName, const DataType::Type& dataType) const
    {
        if (fieldIndex < fieldSerializers.size())
        {
            return *fieldSerializers[fieldIndex];
        }
        return getSerializer(fieldName, dataType);
    }

    /// Resolves every serializer's shared nautilus function up front, and pins one per field of the output schema.
    /// Called from OutputFormatterBufferRef::setup() with the fields that its write loop iterates, so the indices
    /// agree by construction.
    void resolveSerializers(CompilationContext& compilationContext, const std::vector<DataType>& fieldDataTypes)
    {
        for (const auto& serializer : std::views::values(serializers))
        {
            serializer->resolve(compilationContext);
        }

        PRECONDITION(
            fieldNames.size() == fieldDataTypes.size(),
            "Output schema has {} names but {} data types",
            fieldNames.size(),
            fieldDataTypes.size());
        fieldSerializers.clear();
        fieldSerializers.reserve(fieldNames.size());
        for (size_t fieldIndex = 0; fieldIndex < fieldNames.size(); ++fieldIndex)
        {
            fieldSerializers.push_back(std::addressof(getSerializer(fieldNames[fieldIndex], fieldDataTypes[fieldIndex].type)));
        }
    }

protected:
    /// Call at the end of the derived formatter's constructor, once the serializer type maps are populated.
    /// Defined in the .cpp: reaching the registry from this header would drag it into every dependent module.
    void createSerializers(const ValueSerializerConfig& config);

    /// Identifiers of the fields of the output schema
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    /// Stores the default serializer for each datatype.
    std::unordered_map<DataType::Type, std::string> serializerTypes;
    /// Stores the serializer type that the user configured for a specific field. Takes precedence over the datatype default.
    std::unordered_map<Record::RecordFieldIdentifier, std::string> fieldSerializerTypes;

private:
    /// The serializer for a field. Instances are built once, at construction: writeFormattedValue() runs inside
    /// traced code, which in interpreted mode means per field per record on every worker thread, where creating
    /// one per call would allocate on the hot path and a lazy cache would need guarding.
    [[nodiscard]] const ValueSerializer& getSerializer(const Record::RecordFieldIdentifier& fieldName, const DataType::Type& dataType) const
    {
        const auto& serializerType = getSerializerType(fieldName, dataType);
        const auto it = serializers.find(serializerType);
        INVARIANT(it != serializers.end(), "No ValueSerializer instance was created for '{}'", serializerType);
        return *it->second;
    }

    /// The serializer type for a field. The datatype determines the default, which the user may override per field.
    [[nodiscard]] const std::string& getSerializerType(const Record::RecordFieldIdentifier& fieldName, const DataType::Type& dataType) const
    {
        if (const auto it = fieldSerializerTypes.find(fieldName); it != fieldSerializerTypes.end())
        {
            return it->second;
        }
        if (const auto it = serializerTypes.find(dataType); it != serializerTypes.end())
        {
            return it->second;
        }
        throw UnknownValueSerializerType("No ValueSerializer configured for DataType {}.", magic_enum::enum_name(dataType));
    }

    /// Immutable after createSerializers(), hence safe to share across worker threads.
    std::unordered_map<std::string, std::unique_ptr<ValueSerializer>> serializers;
    /// Indexed by field index; empty until resolveSerializers() runs. Points into the map above.
    std::vector<const ValueSerializer*> fieldSerializers;
};

}

template <std::derived_from<NES::OutputFormatter> OutputFormatter>
struct fmt::formatter<OutputFormatter> : fmt::ostream_formatter
{
};

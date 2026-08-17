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

#include <cstddef>
#include <memory>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <magic_enum/magic_enum.hpp>

#include <DataTypes/DataType.hpp>
#include <Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <RawBufferIndex.hpp>
#include <ValueDeserializer.hpp>
#include <ValueDeserializerUtil.hpp>

namespace NES
{

/// Implements format-specific (CSV, JSON, XML, etc.) indexing of raw buffers.
/// The InputFormatIndexerTask uses the InputFormatIndexer to determine byte offsets of all fields of a given tuple and all tuples of a given buffer.
/// The offsets allow the InputFormatIndexerTask to parse only the fields that it needs to for the particular query.
/// @Note All InputFormatIndexer implementations must be thread-safe. NebulaStream's query engine concurrently executes InputFormatIndexerTasks.
///       Thus, the InputFormatIndexerTask calls the interface functions of the InputFormatIndexer concurrently.
class InputFormatIndexer
{
public:
    explicit InputFormatIndexer() = default;
    virtual ~InputFormatIndexer() = default;

    [[nodiscard]] virtual std::unique_ptr<RawBufferIndex> indexRawBuffer(std::string_view rawBuffer) const = 0;

    [[nodiscard]] virtual std::string_view getTupleDelimitingBytes() const = 0;
    [[nodiscard]] virtual std::string_view getFieldDelimitingBytes() const = 0;
    [[nodiscard]] virtual const std::vector<std::string>& getNullValues() const = 0;

    /// The deserializer for a field, by its index in the schema. Resolved in setup(), so the steady-state path is a
    /// vector index rather than the hash lookups getDeserializer() needs. Falls back to those when the table is
    /// empty, which is the case for a formatter constructed without a setup() -- as the unit tests do.
    [[nodiscard]] const ValueDeserializer&
    getDeserializerAt(const size_t fieldIndex, const Record::RecordFieldIdentifier& fieldName, const DataType& dataType) const
    {
        if (fieldIndex < fieldDeserializers.size())
        {
            return *fieldDeserializers[fieldIndex];
        }
        return getDeserializer(fieldName, dataType);
    }

    /// Resolves every deserializer's shared nautilus function up front. Called from InputFormatter::setup().
    void resolveDeserializers(CompilationContext& compilationContext)
    {
        for (const auto& deserializer : std::views::values(deserializers))
        {
            deserializer->resolve(compilationContext);
        }
        for (const auto& deserializer : std::views::values(nullableDeserializers))
        {
            deserializer->resolve(compilationContext);
        }
    }

    /// Pins one deserializer per field of the schema, in field order. Called from InputFormatter::setup() with the
    /// same memory provider that readSpanningRecord() iterates, so the indices agree by construction.
    void
    resolveFieldDeserializers(const std::vector<Record::RecordFieldIdentifier>& fieldNames, const std::vector<DataType>& fieldDataTypes)
    {
        PRECONDITION(
            fieldNames.size() == fieldDataTypes.size(), "Schema has {} names but {} data types", fieldNames.size(), fieldDataTypes.size());
        fieldDeserializers.clear();
        fieldDeserializers.reserve(fieldNames.size());
        for (size_t fieldIndex = 0; fieldIndex < fieldNames.size(); ++fieldIndex)
        {
            fieldDeserializers.push_back(std::addressof(getDeserializer(fieldNames[fieldIndex], fieldDataTypes[fieldIndex])));
        }
    }

    friend std::ostream& operator<<(std::ostream& out, const InputFormatIndexer& indexer);

protected:
    /// Implemented by children of InputFormatIndexer. Called by '<<'. Allows to use '<<' on abstract InputFormatIndexer.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;

    /// Builds both the nullable and the non-nullable form of every configured key, since which one a field needs
    /// only follows from its datatype. Call at the end of the derived indexer's constructor.
    void createDeserializers(const bool quoted, const bool hasTrailingSpaces)
    {
        /// Which of the two a field needs only follows from its datatype, so both are built up front.
        /// provideValueDeserializer() owns the "Nullable" registry-key convention; nothing here reproduces it.
        const auto create = [&](const std::string& deserializerType)
        {
            deserializers.try_emplace(
                deserializerType,
                provideValueDeserializer(
                    deserializerType,
                    ValueDeserializerConfig{.nullable = false, .quoted = quoted, .hasTrailingSpaces = hasTrailingSpaces}));
            nullableDeserializers.try_emplace(
                deserializerType,
                provideValueDeserializer(
                    deserializerType, ValueDeserializerConfig{.nullable = true, .quoted = quoted, .hasTrailingSpaces = hasTrailingSpaces}));
        };
        for (const auto& deserializerType : std::views::values(deserializerTypes))
        {
            create(deserializerType);
        }
        for (const auto& deserializerType : std::views::values(fieldDeserializerTypes))
        {
            create(deserializerType);
        }
    }

    /// Stores the default deserializer type for each datatype.
    std::unordered_map<DataType::Type, std::string> deserializerTypes;
    /// Stores the deserializer type that the user configured for a specific field. Takes precedence over the datatype default.
    std::unordered_map<Record::RecordFieldIdentifier, std::string> fieldDeserializerTypes;

private:
    /// The deserializer for a field. Instances are built once, at construction: readSpanningRecord() runs inside
    /// traced code, which in interpreted mode means per field per record on every worker thread, where creating
    /// one per call would allocate on the hot path and a lazy cache would need guarding.
    [[nodiscard]] const ValueDeserializer& getDeserializer(const Record::RecordFieldIdentifier& fieldName, const DataType& dataType) const
    {
        const auto& deserializerType = getDeserializerType(fieldName, dataType.type);
        /// Two maps under the same key rather than one map under a "Nullable"-prefixed key: both arms are lvalues
        /// of the same type, so this binds a reference instead of composing a key string per field per record.
        const auto& registry = dataType.nullable ? nullableDeserializers : deserializers;
        const auto it = registry.find(deserializerType);
        INVARIANT(
            it != registry.end(),
            "No {}ValueDeserializer instance was created for '{}'",
            dataType.nullable ? "nullable " : "",
            deserializerType);
        return *it->second;
    }

    /// The deserializer type for a field. The datatype determines the default, which the user may override per field.
    [[nodiscard]] const std::string&
    getDeserializerType(const Record::RecordFieldIdentifier& fieldName, const DataType::Type& dataType) const
    {
        if (const auto it = fieldDeserializerTypes.find(fieldName); it != fieldDeserializerTypes.end())
        {
            return it->second;
        }
        if (const auto it = deserializerTypes.find(dataType); it != deserializerTypes.end())
        {
            return it->second;
        }
        throw UnknownValueDeserializerType("No ValueDeserializer configured for DataType {}", magic_enum::enum_name(dataType));
    }

    /// Immutable after createDeserializers(), hence safe to share across worker threads.
    std::unordered_map<std::string, std::unique_ptr<ValueDeserializer>> deserializers;
    std::unordered_map<std::string, std::unique_ptr<ValueDeserializer>> nullableDeserializers;
    /// Indexed by field index; empty until resolveFieldDeserializers() runs. Points into the two maps above.
    std::vector<const ValueDeserializer*> fieldDeserializers;
};
}

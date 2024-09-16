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
#include <optional>
#include <unordered_map>
#include <vector>
#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Memory::MemoryLayouts
{

using FIELD_SIZE = uint64_t;
class MemoryLayoutTupleBuffer;

/// @brief Reads the variable sized data from the child buffer at the provided index
/// @return Variable sized data as a string
std::string readVarSizedData(const Memory::TupleBuffer& buffer, uint64_t childBufferIdx);

/// @brief Writes the variable sized data to the buffer
/// @param buffer
/// @param value
/// @param bufferProvider
/// @return Index of the child buffer
std::optional<uint32_t>
writeVarSizedData(const Memory::TupleBuffer& buffer, const std::string_view value, Memory::AbstractBufferProvider& bufferProvider);

/// @brief A MemoryLayout defines a strategy in which a specific schema / a individual tuple is mapped to a tuple buffer.
/// To this end, it requires the definition of an schema and a specific buffer size.
/// Currently. we support a RowLayout and a ColumnLayout.
class MemoryLayout
{
public:
    /// @brief Constructor for MemoryLayout.
    /// @param bufferSize A memory layout is always created for a specific buffer size.
    /// @param schema A memory layout is always created for a specific schema.
    MemoryLayout(uint64_t bufferSize, std::shared_ptr<Schema> schema);
    MemoryLayout(const MemoryLayout&) = default;

    virtual ~MemoryLayout() = default;

    /// Gets the field index for a specific field name. If the field name not exists, we return an empty optional.
    /// @return either field index for fieldName or empty optional
    [[nodiscard]] std::optional<uint64_t> getFieldIndexFromName(const std::string& fieldName) const;

    /// @return Tuple size in bytes.
    [[nodiscard]] uint64_t getTupleSize() const;

    /// @return BufferSize in bytes.
    [[nodiscard]] uint64_t getBufferSize() const;
    void setBufferSize(uint64_t bufferSize);

    /// @brief Calculates the offset in the tuple buffer of a particular field for a specific tuple.
    /// Depending on the concrete MemoryLayout, e.g., Columnar or Row-Layout, this may result in different calculations.
    /// @throws CannotAccessBuffer if the record of the field is out of bounds.
    /// @return offset in the tuple buffer.
    [[nodiscard]] virtual uint64_t getFieldOffset(uint64_t tupleIndex, uint64_t fieldIndex) const = 0;

    /// @brief Calculates the offset in the tuple buffer of a particular field for a specific tuple.
    /// Depending on the concrete MemoryLayout, e.g., Columnar or Row-Layout, this may result in different calculations.
    /// @throws CannotAccessBuffer if the record of the field is out of bounds.
    /// @return either offset in the tuple buffer for fieldName or empty optional.
    [[nodiscard]] virtual std::optional<uint64_t> getFieldOffset(uint64_t tupleIndex, const std::string_view fieldName) const;

    /// @brief Gets the number of tuples a tuple buffer with this memory layout could occupy.
    /// Depending on the concrete memory layout this value may change, e.g., some layouts may add some padding or alignment.
    /// @return number of tuples a tuple buffer can occupy.
    [[nodiscard]] uint64_t getCapacity() const;

    /// @brief Gets the underling schema of this memory layout.
    [[nodiscard]] const SchemaPtr& getSchema() const;

    /// @brief Gets a vector of all physical fields for this memory layout.
    /// @return Reference to vector physical fields.
    [[nodiscard]] const std::vector<std::shared_ptr<PhysicalType>>& getPhysicalTypes() const;

    /// Gets a vector that contains the physical size of all tuple fields.
    /// This is crucial to calculate the potion of specific fields.
    /// @return Reference of field sizes vector.
    [[nodiscard]] const std::vector<uint64_t>& getFieldSizes() const;

    /// @brief Get the names of the key fields as a vector of strings.
    /// @return std::vector<std::string> fieldNames
    [[nodiscard]] std::vector<std::string> getKeyFieldNames() const;

    /// @brief Set the names of the key fields.
    /// @param keyFields
    void setKeyFieldNames(const std::vector<std::string>& keyFields);

    [[nodiscard]] virtual std::shared_ptr<MemoryLayout> deepCopy() const = 0;

    /// @brief Comparator methods
    bool operator==(const MemoryLayout& rhs) const;
    bool operator!=(const MemoryLayout& rhs) const;

protected:
    uint64_t bufferSize;
    std::shared_ptr<Schema> schema;
    uint64_t recordSize;
    uint64_t capacity;
    std::vector<uint64_t> physicalFieldSizes;
    std::vector<std::shared_ptr<PhysicalType>> physicalTypes;
    std::unordered_map<std::string, uint64_t> nameFieldIndexMap;
    std::vector<std::string> keyFieldNames;
};

using MemoryLayoutPtr = std::shared_ptr<NES::Memory::MemoryLayouts::MemoryLayout>;

}

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

#include <MemoryLayout/ColumnLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Memory::MemoryLayouts
{

/**
 * @brief The ColumnLayoutField enables assesses to a specific field in a columnar layout.
 * It overrides the operator[] for a more user friendly access of tuples for a predefined field.
 * As this required direct knowledge of a particular memory layout at compile-time, consider to use the TestTupleBuffer.
 * @tparam T the type of the field
 * @tparam boundaryChecks flag to identify if buffer bounds should be checked at runtime.
 * @caution This class is non-thread safe
 */
template <class T, bool boundaryChecks = true>
class ColumnLayoutField
{
public:
    static inline ColumnLayoutField<T, boundaryChecks>
    create(uint64_t fieldIndex, std::shared_ptr<ColumnLayout> layout, Memory::TupleBuffer& buffer);
    static inline ColumnLayoutField<T, boundaryChecks>
    create(const std::string& fieldName, std::shared_ptr<ColumnLayout> layout, Memory::TupleBuffer& buffer);

    /**
     * Accesses the value of this field for a specific record.
     * @param recordIndex
     * @return reference to a field attribute from the created field handler accessed by recordIndex
     */
    inline T& operator[](size_t recordIndex);

private:
    ColumnLayoutField(T* basePointer, std::shared_ptr<ColumnLayout> layout) : basePointer(basePointer), layout(std::move(layout)) { };

    T* basePointer;
    std::shared_ptr<ColumnLayout> layout;
};

template <class T, bool boundaryChecks>
inline ColumnLayoutField<T, boundaryChecks>
ColumnLayoutField<T, boundaryChecks>::create(uint64_t fieldIndex, std::shared_ptr<ColumnLayout> layout, Memory::TupleBuffer& buffer)
{
    INVARIANT(
        boundaryChecks && fieldIndex < layout->getSchema().getFieldCount(),
        "fieldIndex out of bounds {} >= {}",
        layout->getSchema().getFieldCount(),
        fieldIndex);

    auto* bufferBasePointer = &(buffer.getBuffer<uint8_t>()[0]);
    auto fieldOffset = layout->getFieldOffset(0, fieldIndex);

    T* basePointer = reinterpret_cast<T*>(bufferBasePointer + fieldOffset);
    return ColumnLayoutField<T, boundaryChecks>(basePointer, layout);
}

template <class T, bool boundaryChecks>
ColumnLayoutField<T, boundaryChecks> ColumnLayoutField<T, boundaryChecks>::create(
    const std::string& fieldName, std::shared_ptr<ColumnLayout> layout, Memory::TupleBuffer& buffer)
{
    auto fieldIndex = layout->getFieldIndexFromName(fieldName);
    INVARIANT(fieldIndex.has_value(), "Could not find fieldIndex for {}", fieldName);
    return ColumnLayoutField<T, boundaryChecks>::create(fieldIndex.value(), layout, buffer);
}

template <class T, bool boundaryChecks>
inline T& ColumnLayoutField<T, boundaryChecks>::operator[](size_t recordIndex)
{
    INVARIANT(
        boundaryChecks && recordIndex < layout->getCapacity(), "recordIndex out of bounds {} >= {}", layout->getCapacity(), recordIndex);
    return *(basePointer + recordIndex);
}

}

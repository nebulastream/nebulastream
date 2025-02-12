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

#include <cstdint>
#include <utility>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Memory::MemoryLayouts
{
/**
 * @brief The RowLayoutField enables assesses to a specific field in a row layout.
 * It overrides the operator[] for a more user friendly access of tuples for a predefined field.
 * As this required direct knowledge of a particular memory layout at compile-time, consider to use the TestTupleBuffer.
 * @tparam T the type of the field
 * @tparam boundaryChecks flag to identify if buffer bounds should be checked at runtime.
 * @caution This class is non-thread safe
 */
template <class T, bool boundaryChecks = true>
class RowLayoutField
{
public:
    /**
     *  Factory to create a RowLayoutField for a specific memory layout and a specific tuple buffer.
     * @param fieldIndex
     * @param layoutBuffer
     * @tparam boundaryChecks if true will check if access is allowed
     * @tparam T type of field
     * @return field handler via a fieldIndex and a layoutBuffer
     */
    static inline RowLayoutField<T, boundaryChecks>
    create(uint64_t fieldIndex, std::shared_ptr<RowLayout> layout, Memory::PinnedBuffer& buffer);

    /**
    * Factory to create a RowLayoutField for a specific memory layout and a specific tuple buffer.
    * @param fieldIndex the field which is accessed.
    * @param layout the memory layout
    * @tparam buffer the tuple buffer
    * @tparam T type of field
    * @return field handler
    */
    static inline RowLayoutField<T, boundaryChecks>
    create(const std::string& fieldName, std::shared_ptr<RowLayout> layout, Memory::PinnedBuffer& buffer);

    /**
     * Accesses the value of this field for a specific record.
     * @param recordIndex
     * @return reference to a field attribute from the created field handler accessed by recordIndex
     */
    inline T& operator[](size_t recordIndex);

private:
    /**
     * @brief Constructor for RowLayoutField
     * @param dynamicRowLayoutBuffer
     * @param basePointer
     * @param fieldIndex
     * @param recordSize
     */
    RowLayoutField(std::shared_ptr<RowLayout> layout, uint8_t* basePointer, uint64_t fieldIndex, uint64_t recordSize)
        : fieldIndex(fieldIndex), recordSize(recordSize), basePointer(basePointer), layout(std::move(layout)) {};

    uint64_t fieldIndex;
    uint64_t recordSize;
    uint8_t* basePointer;
    std::shared_ptr<RowLayout> layout;
};

template <class T, bool boundaryChecks>
inline RowLayoutField<T, boundaryChecks>
RowLayoutField<T, boundaryChecks>::create(uint64_t fieldIndex, std::shared_ptr<RowLayout> layout, Memory::PinnedBuffer& buffer)
{
    INVARIANT(
        boundaryChecks && fieldIndex < layout->getSchema()->getFieldCount(),
        "fieldIndex out of bounds! {} >= {}",
        layout->getSchema()->getFieldCount(),
        fieldIndex);

    /// via pointer arithmetic gets the starting field address
    auto* bufferBasePointer = &(buffer.getBuffer<uint8_t>()[0]);
    auto offSet = layout->getFieldOffset(0, fieldIndex);
    auto* basePointer = bufferBasePointer + offSet;

    return RowLayoutField<T, boundaryChecks>(layout, basePointer, fieldIndex, layout->getTupleSize());
}

template <class T, bool boundaryChecks>
inline RowLayoutField<T, boundaryChecks>
RowLayoutField<T, boundaryChecks>::create(const std::string& fieldName, std::shared_ptr<RowLayout> layout, Memory::PinnedBuffer& buffer)
{
    auto fieldIndex = layout->getFieldIndexFromName(fieldName);
    if (fieldIndex.has_value())
    {
        return RowLayoutField<T, boundaryChecks>::create(fieldIndex.value(), layout, buffer);
    }
    INVARIANT(false, "Could not find fieldIndex for {}", fieldName);
}

template <class T, bool boundaryChecks>
inline T& RowLayoutField<T, boundaryChecks>::operator[](size_t recordIndex)
{
    INVARIANT(
        boundaryChecks && recordIndex < layout->getCapacity(), "recordIndex out of bounds! {}  >= {}", layout->getCapacity(), recordIndex);
    return *reinterpret_cast<T*>(basePointer + recordSize * recordIndex);
}

}

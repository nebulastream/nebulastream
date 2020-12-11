/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef INCLUDE_NODEENGINE_PHYSICALLAYOUT_HPP_
#define INCLUDE_NODEENGINE_PHYSICALLAYOUT_HPP_
#include <NodeEngine/MemoryLayout/ArrayPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/BasicPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
#include <memory>
namespace NES {

class TupleBuffer;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class PhysicalSchema;
typedef std::shared_ptr<PhysicalSchema> PhysicalSchemaPtr;

class MemoryLayout;
typedef std::shared_ptr<MemoryLayout> MemoryLayoutPtr;

class ArrayPhysicalField;
typedef std::shared_ptr<ArrayPhysicalField> ArrayPhysicalFieldPtr;

/**
 * The MemoryLayout maps a schema to a physical representation,
 * which allows to read and write values to particular fields in a TupleBuffer.
 */
class MemoryLayout {
  public:
    virtual MemoryLayoutPtr copy() const = 0;

    /**
     * @brief Calculates the multiplier position of a particular field in a particular record.
     * @param recordIndex the index of the record we want to access
     * @param fieldIndex the index of the field we want to access
     * @return uint64_t field multiplier in the tuple buffer
     */
    virtual uint64_t getFieldOffset(uint64_t recordIndex, uint64_t fieldIndex) = 0;

    /**
     * @brief Returns the BasicPhysicalField of a specific value field
     * @throws IllegalArgumentException if field is not an value field
     * @tparam ValueType template type of this value field
     * @param recordIndex index of the record
     * @param fieldIndex index of the field
     * @return std::shared_ptr<BasicPhysicalField<ValueType>>
     */
    template<class ValueType>
    std::shared_ptr<BasicPhysicalField<ValueType>> getValueField(uint64_t recordIndex, uint64_t fieldIndex) {
        auto fieldOffset = getFieldOffset(recordIndex, fieldIndex);
        return physicalSchema->createPhysicalField(fieldIndex, fieldOffset)->asValueField<ValueType>();
    }

    /**
     * @brief Returns a typed pointer to the location of this field.
     * @tparam ValueType template type of this field
     * @param buffer memory buffer
     * @param recordIndex index of the specific record
     * @param fieldIndex index of the specific field
     * @return ValueType* typed pointer
     */
    template<class ValueType>
    ValueType* getFieldPointer(TupleBuffer& buffer, uint64_t recordIndex, uint64_t fieldIndex) {
        auto fieldOffset = getFieldOffset(recordIndex, fieldIndex);
        auto byteBuffer = buffer.getBufferAs<int8_t>();
        // interpret the target address as value type and read value from tuple buffer
        return (ValueType*) (&byteBuffer[fieldOffset]);
    }

    /**
     * @brief Returns the ArrayPhysicalField of a specific array field
     * @throws IllegalArgumentException if field is not an array field
     * @param recordIndex index of the record
     * @param fieldIndex index of the field
     * @note in this special case we return an reference to the array field as we want to access the array via the [] operator.
     * @return ArrayPhysicalField&
     */
    ArrayPhysicalField getArrayField(uint64_t recordIndex, uint64_t fieldIndex) {
        auto fieldOffset = getFieldOffset(recordIndex, fieldIndex);
        auto field = this->physicalSchema->createPhysicalField(fieldIndex, fieldOffset);
        return *field->asArrayField();
    }

  protected:
    explicit MemoryLayout(PhysicalSchemaPtr physicalSchema);
    PhysicalSchemaPtr physicalSchema;
};

/**
 * creates a row layout from a logical schema.
 * @param schema
 * @return
 */
MemoryLayoutPtr createRowLayout(SchemaPtr schema);

}// namespace NES

#endif//INCLUDE_NODEENGINE_PHYSICALLAYOUT_HPP_

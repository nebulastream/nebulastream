#ifndef INCLUDE_NODEENGINE_PHYSICALLAYOUT_HPP_
#define INCLUDE_NODEENGINE_PHYSICALLAYOUT_HPP_
#include <memory>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/BasicPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/ArrayPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
namespace NES {

class TupleBuffer;
typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;

//todo:change SchemaTemp to Schema
class SchemaTemp;
typedef std::shared_ptr<SchemaTemp> SchemaPtr;

class PhysicalSchema;
typedef std::shared_ptr<PhysicalSchema> PhysicalSchemaPtr;

class MemoryLayout;
typedef std::shared_ptr<MemoryLayout> MemoryLayoutPtr;

class ArrayPhysicalField;

/**
 * The MemoryLayout maps a schema to a physical representation,
 * which allows to read and write values to particular fields in a TupleBuffer.
 */
class MemoryLayout {
  public:

    virtual MemoryLayoutPtr copy() const = 0;

    /**
     * @brief Calculates the offset position of a particular field in a particular record.
     * @param recordIndex the index of the record we want to access
     * @param fieldIndex the index of the field we want to access
     * @return uint64_t field offset in the tuple buffer
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
    ValueType* getFieldPointer(TupleBufferPtr buffer, uint64_t recordIndex, uint64_t fieldIndex){
        auto fieldOffset = getFieldOffset(recordIndex, fieldIndex);
        auto byteBuffer = (int8_t*) buffer->getBuffer();
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
    ArrayPhysicalField& getArrayField(uint64_t recordIndex, uint64_t fieldIndex) {
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

}

#endif //INCLUDE_NODEENGINE_PHYSICALLAYOUT_HPP_

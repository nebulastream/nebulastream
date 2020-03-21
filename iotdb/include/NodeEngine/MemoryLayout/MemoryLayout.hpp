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

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

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

    template<class ValueType>
    std::shared_ptr<BasicPhysicalField<ValueType>> getValueField(uint64_t recordIndex, uint64_t fieldIndex) {
        auto fieldOffset = getFieldOffset(recordIndex, fieldIndex);
        return physicalSchema->createField(fieldIndex, fieldOffset)->asValueField<ValueType>();
    }

    template<class ValueType>
    ValueType* getFieldPointer(TupleBufferPtr buffer, uint64_t recordIndex, uint64_t fieldIndex){
        auto fieldOffset = getFieldOffset(recordIndex, fieldIndex);
        auto byteBuffer = (int8_t*) buffer->getBuffer();
        // interpret the target address as value type and read value from tuple buffer
        return (ValueType*) (&byteBuffer[fieldOffset]);
    }

    /**
     * @brief Reads a value of type ValueType from a particular field in the tuple buffer.
     * @tparam ValueType type of value we want to read
     * @param buffer tuple buffer in which we want to write
     * @param recordIndex the index of the record we want to access
     * @param fieldIndex the index of the field we want to access
     * @return return value we read
     */
    ArrayPhysicalField& getArrayField(uint64_t recordIndex, uint64_t fieldIndex) {
        auto fieldOffset = getFieldOffset(recordIndex, fieldIndex);
        auto field = this->physicalSchema->createField(fieldIndex, fieldOffset);
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
MemoryLayoutPtr createRowLayout(const SchemaPtr& schema);

}

#endif //INCLUDE_NODEENGINE_PHYSICALLAYOUT_HPP_

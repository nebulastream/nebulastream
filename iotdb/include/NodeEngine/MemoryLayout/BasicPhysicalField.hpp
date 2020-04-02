#ifndef NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_BASICPHYSICALFIELD_HPP_
#define NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_BASICPHYSICALFIELD_HPP_
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
namespace NES {
/**
 * @brief Represents an value field at a specific position in a memory buffer.
 * This class has to be implemented in the header as it is a template type.
 */
template<class ValueType>
class BasicPhysicalField : public PhysicalField {
  public:
    BasicPhysicalField(uint64_t bufferOffset) : PhysicalField(bufferOffset) {};
    BasicPhysicalField(BasicPhysicalField<ValueType>* physicalField) : PhysicalField(physicalField) {};
    ~BasicPhysicalField() {};
    const PhysicalFieldPtr copy() const override {
        return std::make_shared<BasicPhysicalField>(*this);
    }

    /**
     * @brief writes a value of type ValueType to a particular position in the buffer.
     * @param tupleBuffer target tuple buffer
     * @param value the value we want to write
     */
    void write(TupleBuffer& tupleBuffer, ValueType value) {
        auto byteBuffer = tupleBuffer.getBufferAs<int8_t>();
        // interpret the target address as value type and write value to tuple buffer(ValueType *) byteBuffer[offset]) = value;
        ((ValueType*) (&byteBuffer[bufferOffset]))[0] = value;
    };

    /**
     * Reads a value of type value type from the tuple buffer.
     * @param tupleBuffer
     * @return ValueType
     */
    ValueType read(TupleBuffer& tupleBuffer) {
        auto byteBuffer = tupleBuffer.getBufferAs<int8_t>();
        // interpret the target address as value type and read value from tuple buffer
        return ((ValueType*) (&byteBuffer[bufferOffset]))[0];
    }
};

template<class ValueType>
PhysicalFieldPtr createBasicPhysicalField(uint64_t bufferOffset) {
    return std::make_shared<BasicPhysicalField<ValueType>>(bufferOffset);
}
}

#endif //NES_INCLUDE_NODEENGINE_MEMORYLAYOUT_BASICPHYSICALFIELD_HPP_

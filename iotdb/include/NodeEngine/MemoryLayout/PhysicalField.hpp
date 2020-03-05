#ifndef INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELD_HPP_
#define INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELD_HPP_

#include <memory>
#include <NodeEngine/TupleBuffer.hpp>
namespace NES {
class PhysicalField;
typedef std::shared_ptr<PhysicalField> PhysicalFieldPtr;

class PhysicalField {
 public:
  PhysicalField() {};
  ~PhysicalField() {};
  const virtual PhysicalFieldPtr copy() const = 0;
};

template<class ValueType>
class BasicPhysicalField : public PhysicalField {
 public:
  BasicPhysicalField() {};
  ~BasicPhysicalField() {};
  BasicPhysicalField(BasicPhysicalField<ValueType> *physicalField) {};
  const PhysicalFieldPtr copy() const override {
    return std::static_pointer_cast<PhysicalField>(std::make_shared<BasicPhysicalField>(*this));
  }
  /**
   * @brief writes a value of type ValueType to a particular position in the buffer.
   * @param tupleBuffer target tuple buffer
   * @param offset offset in bytes
   * @param value the value we want to write
   */
  void write(const TupleBufferPtr &tupleBuffer, uint64_t offset, ValueType value) {
    auto byteBuffer = (int8_t *) tupleBuffer->getBuffer();
    // interpret the target address as value type and write value to tuple buffer(ValueType *) byteBuffer[offset]) = value;
    ((ValueType *) (&byteBuffer[offset]))[0] = value;
  };

  /**
   * Reads a value of type value type from the tuple buffer.
   * @param tupleBuffer
   * @param offset
   * @return ValueType
   */
  ValueType read(const TupleBufferPtr &tupleBuffer, uint64_t offset) {
    auto byteBuffer = (int8_t *) tupleBuffer->getBuffer();
    // interpret the target address as value type and read value from tuple buffer
    return ((ValueType *) (&byteBuffer[offset]))[0];
  }
};
}

#endif //INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALFIELD_HPP_

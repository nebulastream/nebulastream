#ifndef IOTDB_INCLUDE_NODEENGINE_PHYSICALLAYOUT_HPP_
#define IOTDB_INCLUDE_NODEENGINE_PHYSICALLAYOUT_HPP_
#include <memory>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>
namespace iotdb {

class TupleBuffer;
typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class PhysicalSchema;
typedef std::shared_ptr<PhysicalSchema> PhysicalSchemaPtr;

class MemoryLayout;
typedef std::shared_ptr<MemoryLayout> MemoryLayoutPtr;

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
   * @brief Writes a value of type ValueType to a particular field in the tuple buffer.
   * @tparam ValueType type of value we want to write
   * @param buffer tuple buffer in which we want to write
   * @param recordIndex the index of the record we want to access
   * @param fieldIndex the index of the field we want to access
   * @param value the value we want to write
   */
  template<class ValueType>
  void writeField(TupleBufferPtr buffer, uint64_t recordIndex, uint64_t fieldIndex, ValueType value) {
    auto fieldOffset = this->getFieldOffset(recordIndex, fieldIndex);
    this->physicalSchema->getField<ValueType>(fieldIndex)->write(buffer, fieldOffset, value);
  }

  /**
   * @brief Reads a value of type ValueType from a particular field in the tuple buffer.
   * @tparam ValueType type of value we want to read
   * @param buffer tuple buffer in which we want to write
   * @param recordIndex the index of the record we want to access
   * @param fieldIndex the index of the field we want to access
   * @return return value we read
   */
  template<class ValueType>
  ValueType readField(TupleBufferPtr buffer, uint64_t recordIndex, uint64_t fieldIndex) {
    auto fieldOffset = this->getFieldOffset(recordIndex, fieldIndex);
    return this->physicalSchema->getField<ValueType>(fieldIndex)->read(buffer, fieldOffset);
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
MemoryLayoutPtr createRowLayout(const SchemaPtr &schema);

}

#endif //IOTDB_INCLUDE_NODEENGINE_PHYSICALLAYOUT_HPP_

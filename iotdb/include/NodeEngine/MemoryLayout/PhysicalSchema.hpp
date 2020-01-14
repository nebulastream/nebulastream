#ifndef INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALSCHEMA_HPP_
#define INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALSCHEMA_HPP_
#include <memory>
#include <vector>
#include <API/Types/DataTypes.hpp>
namespace NES {
class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

template<class ValueType>
class BasicPhysicalField;


class PhysicalField;
typedef std::shared_ptr<PhysicalField> PhysicalFieldPtr;

class PhysicalSchema;
typedef std::shared_ptr<PhysicalSchema> PhysicalSchemaPtr;

/**
 * @brief the physical schema which maps a logical schema do physical fields, which can be accessed dynamically.
 */
class PhysicalSchema {
 public:
  PhysicalSchema(const SchemaPtr& schema);
  static PhysicalSchemaPtr createPhysicalSchema(SchemaPtr schema);

  /**
   * @brief returns size of one record in bytes.
   * @return uint64_t size in bytes
   */
  uint64_t getRecordSize();

  /**
   * @brief Calculated the offset of a particular field in a record.
   * @param fieldIndex index of the field we want to access.
   * @return offset in byte
   */
  uint64_t getFieldOffset(uint64_t fieldIndex);

  /**
   * Get the physical field representation at a field index
   * @param fieldIndex
   */
  PhysicalFieldPtr getField(uint64_t fieldIndex);

  /**
  * Get the physical field representation of a particular ValueType at a field index
  * @param fieldIndex
  */
  template<class ValueType>
  std::shared_ptr<BasicPhysicalField<ValueType>> getField(uint64_t fieldIndex){
    return (std::static_pointer_cast<BasicPhysicalField<ValueType>>(getField(fieldIndex)));
  }

 private:
  PhysicalFieldPtr createPhysicalField(const DataTypePtr dataType);
  SchemaPtr schema;
  std::vector<PhysicalFieldPtr> fields;
};


}

#endif //INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALSCHEMA_HPP_

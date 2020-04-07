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
    explicit PhysicalSchema(SchemaPtr schemaPtr);
    static PhysicalSchemaPtr createPhysicalSchema(SchemaPtr schema);

    /**
     * @brief returns size of one record in bytes.
     * @return uint64_t size in bytes
     */
    uint64_t getRecordSize();

    /**
     * @brief Calculated the offset of a particular field in a record.
     * @param fieldIndex index of the field we want to access.
     * @throws IllegalArgumentException if fieldIndex is not valid
     * @return offset in byte
     */
    uint64_t getFieldOffset(uint64_t fieldIndex);

    /**
     * Get the physical field representation at a field index
     * @throws IllegalArgumentException if fieldIndex is not valid
     * @param fieldIndex
     */
    PhysicalFieldPtr createPhysicalField(uint64_t fieldIndex, uint64_t bufferOffset);


  private:
    SchemaPtr schema;
    /**
     * @brief Checks if the fieldIndex is in contained in the schema
     * @param fieldIndex
     * @return true if fieldIndex is valid
     */
    bool validFieldIndex(uint64_t fieldIndex);
};

}

#endif //INCLUDE_NODEENGINE_MEMORYLAYOUT_PHYSICALSCHEMA_HPP_

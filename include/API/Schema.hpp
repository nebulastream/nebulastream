#ifndef API_SCHEMA_H
#define API_SCHEMA_H

#include <API/Types/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES {
class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class Schema {
  public:
    Schema();
    Schema(SchemaPtr query);

    /**
     * @brief Factory method to create a new SchemaPtr.
     * @return SchemaPtr
     */
    static SchemaPtr create();

    /**
     * @brief Creates a copy of this schema.
     * @note The containing AttributeFields may still reference the same objects.
     * @return A copy of the Schema
     */
    SchemaPtr copy() const;

    /**
     * @brief Copy all fields of otherSchema into this schema.
     * @param otherSchema
     * @return a copy of this schema.
     */
    SchemaPtr copyFields(SchemaPtr otherSchema);

    /**
     * @brief appends a AttributeField to the schema and returns a copy of this schema.
     * @param field
     * @return a copy of this schema.
     */
    SchemaPtr addField(AttributeFieldPtr field);

    /**
    * @brief appends a field with a basic type to the schema and returns a copy of this schema.
    * @param field
    * @return a copy of this schema.
    */
    SchemaPtr addField(const std::string& name, const BasicType& type);

    /**
    * @brief appends a field with a data type to the schema and returns a copy of this schema.
    * @param field
    * @return a copy of this schema.
    */
    SchemaPtr addField(const std::string& name, DataTypePtr data);



    /**
     * @brief Replaces a field, which is already part of the schema.
     * @param name
     * @param type
     * @return
     */
    void replaceField(const std::string& name, DataTypePtr type);

    /**
     * @brief Checks if attribute field name is defined in the schema
     * @param fieldName
     * @return bool
     */
    bool has(const std::string& fieldName);

    /**
     * @brief Finds a attribute field by name in the schema
     * @param fieldName
     * @return AttributeField
     */
    AttributeFieldPtr get(const std::string& fieldName);

    /**
     * @brief Finds a attribute field by index in the schema
     * @param index
     * @return AttributeField
     */
    AttributeFieldPtr get(uint32_t index);

    /**
     * @brief Returns the number of fields in the schema.
     * @return size_t
     */
    size_t getSize() const;

    /**
     * @brief Returns the number of bytes all fields in this schema occupy.
     * @return size_t
     */
    size_t getSchemaSizeInBytes() const;

    /**
     * @brief Checks if two Schemas are equal to each other.
     * @param schema
     * @param considerOrder takes into account if the order of fields in a schema matter.
     * @return boolean
     */
    bool equals(SchemaPtr schema, bool considerOrder = true);

    const std::string toString() const;

    std::vector<AttributeFieldPtr> fields;
};

}// namespace NES
#endif// API_SCHEMA_H

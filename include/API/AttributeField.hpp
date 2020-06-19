#ifndef INCLUDE_CORE_ATTRIBUTEFIELD_HPP_
#define INCLUDE_CORE_ATTRIBUTEFIELD_HPP_

#pragma once
#include <memory>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

/**
 * @brief Represents a typed field in a schema.
 */
class AttributeField {
  public:
    AttributeField() = default;
    AttributeField(const std::string& name, DataTypePtr dataType);

    /**
     * @brief Factory method to create a new field
     * @param name name of the field
     * @param dataType data type
     * @return AttributeFieldPtr
     */
    static AttributeFieldPtr create(std::string name, DataTypePtr dataType);

    DataTypePtr getDataType() const;
    const std::string toString() const;
    bool isEqual(AttributeFieldPtr attr);

    std::string name;
    DataTypePtr dataType;
};

}// namespace NES
#endif//INCLUDE_CORE_ATTRIBUTEFIELD_HPP_

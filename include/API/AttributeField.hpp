#ifndef INCLUDE_CORE_ATTRIBUTEFIELD_HPP_
#define INCLUDE_CORE_ATTRIBUTEFIELD_HPP_

#pragma once
#include <memory>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class AttributeField {
  public:
    AttributeField() = default;
    AttributeField(const std::string& name, DataTypePtr dataType);
    static AttributeFieldPtr create(std::string name, DataTypePtr dataType);
    std::string name;
    DataTypePtr dataType;
    DataTypePtr getDataType() const;
    const std::string toString() const;
    bool isEqual(AttributeFieldPtr attr);
};

}// namespace NES
#endif//INCLUDE_CORE_ATTRIBUTEFIELD_HPP_

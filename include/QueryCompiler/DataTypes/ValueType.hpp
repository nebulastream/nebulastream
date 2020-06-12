#ifndef INCLUDE_VALUETYPE_HPP_
#define INCLUDE_VALUETYPE_HPP_

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ValueType {
  public:
    virtual const DataTypePtr getType() const = 0;
    virtual const CodeExpressionPtr getCodeExpression() const = 0;
    virtual const ValueTypePtr copy() const = 0;
    virtual const std::string toString() const = 0;
    virtual bool isArrayValueType() const = 0;
    virtual bool operator==(const ValueType& rhs) const = 0;
    virtual bool equals(const ValueTypePtr rhs) const = 0;
    virtual ~ValueType();

  protected:
    ValueType() = default;
    ValueType(const ValueType&);
    ValueType& operator=(const ValueType&);
};
}// namespace NES

#endif//INCLUDE_VALUETYPE_HPP_

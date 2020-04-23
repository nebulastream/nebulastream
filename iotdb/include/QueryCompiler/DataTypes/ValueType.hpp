#ifndef INCLUDE_VALUETYPE_HPP_
#define INCLUDE_VALUETYPE_HPP_

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

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
  virtual const bool isArrayValueType() const = 0;
  virtual bool operator==(const ValueType &rhs) const = 0;
  virtual ~ValueType();
 protected:
  ValueType() = default;
  ValueType(const ValueType &);
  ValueType &operator=(const ValueType &);
 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, unsigned) {
  }
};
}

#endif //INCLUDE_VALUETYPE_HPP_

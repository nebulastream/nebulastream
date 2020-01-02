
#include <CodeGen/CodeExpression.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/C_CodeGen/BinaryOperatorStatement.hpp>
#include <CodeGen/DataTypes/ValueType.hpp>
#include <CodeGen/DataTypes/BasicDataType.hpp>
#include <CodeGen/DataTypes/ArrayDataType.hpp>
#include <CodeGen/DataTypes/ArrayValueType.hpp>

namespace iotdb {

/**
 * class ArrayValueType keeps a field of values of basic types
 */
ArrayValueType::ArrayValueType(const ArrayDataType &type, const std::vector<std::string> &value)
    : type_(std::make_shared<
    ArrayDataType>(type)), value_(value) {};

ArrayValueType::ArrayValueType(const ArrayDataType &type, const std::string &value)
    : type_(std::make_shared<ArrayDataType>(type)), isString_(true) {
  value_.push_back(value);
};

const DataTypePtr ArrayValueType::getType() const { return type_; }

const CodeExpressionPtr ArrayValueType::getCodeExpression() const {
  std::stringstream str;
  if (isString_) {
    str << "\"" << value_.at(0) << "\"";
    return std::make_shared<CodeExpression>(str.str());
  }
  bool isCharArray = (type_->isCharDataType());
  str << "{";
  u_int32_t i;
  for (i = 0; i < value_.size(); i++) {
    if (i != 0) str << ", ";
    if (isCharArray) str << "\'";
    str << value_.at(i);
    if (isCharArray) str << "\'";
  }
  str << "}";
  return std::make_shared<CodeExpression>(str.str());
}

const ValueTypePtr ArrayValueType::copy() const {
  return std::make_shared<ArrayValueType>(*this);
}

const bool ArrayValueType::isArrayValueType() const { return true; }

bool ArrayValueType::operator==(const ArrayValueType &rhs) const {
  return static_cast<const iotdb::ValueType &>(*this) == static_cast<const iotdb::ValueType &>(rhs) &&
      type_ == rhs.type_ &&
      isString_ == rhs.isString_ &&
      value_ == rhs.value_;
}

bool ArrayValueType::operator==(const ValueType &rhs) const {
  return type_ == dynamic_cast<const iotdb::ArrayValueType &>(rhs).type_ &&
      isString_ == dynamic_cast<const iotdb::ArrayValueType &>(rhs).isString_ &&
      value_ == dynamic_cast<const iotdb::ArrayValueType &>(rhs).value_;
}

ArrayValueType::~ArrayValueType() {}

/**
 * creates a "string"-value (means char *)
 * @param value : std:string : the string value
 * @return ValueTypePtr : the structure keeping the given values (-- here it keeps it as a single string)
 */
const ValueTypePtr createStringValueType(const std::string &value) {
  return std::make_shared<ArrayValueType>(ArrayDataType(BasicDataType(BasicType::CHAR).copy(), value.size()), value);
}

const ValueTypePtr createStringValueType(const char *value, u_int16_t dimension) {
  u_int32_t i = 0;
  std::stringstream str;
  /**
   * caused by while-loop it could make sense to include a maximum string-size here.
   * ERROR-POSSIBILITY: INFINITE-LOOP - CHARPOINTER WITHOUT PREDEFINED SIZE DOES NOT END WITH '\0'
   */
  if (dimension == 0)
    while (value[i] != '\0') {
      str << value[i];
      i++;
    }
  else {
    u_int32_t j;
    for (j = 0; j < dimension; j++) {
      str << value[j];
      if (value[j] == '\0') break;
    }
    if (j == dimension) {
      str << '\0';
      j++;
    }
    i = j;
  }

  return std::make_shared<ArrayValueType>(ArrayDataType(BasicDataType(BasicType::CHAR).copy(), i), str.str());
}

const ValueTypePtr createArrayValueType(const BasicType &type, const std::vector<std::string> &value) {
  /** \todo: as above. Missing type-check for the values*/
  return std::make_shared<ArrayValueType>(ArrayDataType(BasicDataType(type).copy(), value.size()), value);
}
}
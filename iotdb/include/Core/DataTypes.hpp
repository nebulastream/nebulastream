#ifndef CORE_DATATYPES_H
#define CORE_DATATYPES_H

#include <cstdint>
#include <memory>

namespace iotdb {

enum BasicType { INT8, UINT8, INT16, UINT16, INT32, UINT32, INT64, UINT64, FLOAT32, FLOAT64, BOOLEAN, CHAR, DATE };

/** \brief generic implementation of DataType for ground-up support of user-defined types */
class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

/** \brief represents a value of a particular DataType */
class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class DataType {
public:
  virtual ValueTypePtr getNullValue() const = 0;
  virtual uint32_t getSizeBytes() const = 0;
  virtual const std::string toString() const = 0;
  virtual const CodeExpressionPtr getCode() const = 0;
  virtual ~DataType();
};

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class AttributeField {
public:
  AttributeField(const std::string &name, DataTypePtr data_type);
  AttributeField(const std::string &name, const BasicType &);
  std::string name;
  DataTypePtr data_type;
  uint32_t getFieldSize() const;
  const std::string toString() const;
};

const AttributeFieldPtr createField(const std::string name, const BasicType & type);

const DataTypePtr createDataType(const BasicType &);
const DataTypePtr createDataTypeVarChar(const uint32_t &max_length);
}
#endif

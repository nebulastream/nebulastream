#ifndef CORE_DATATYPES_H
#define CORE_DATATYPES_H

#include <cstdint>
#include <memory>
#include <string>

namespace iotdb {

enum BasicType {
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    UINT64,
    FLOAT32,
    FLOAT64,
    BOOLEAN,
    CHAR,
    DATE,
    VOID_TYPE
};

/** \brief generic implementation of DataType for ground-up support of user-defined types */
class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

/** \brief grepresents a pointer to a particular DataType */
class PointerDataType;
typedef std::shared_ptr<PointerDataType> PointerDataTypePtr;

/** \brief represents a value of a particular DataType */
class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class DataType {
  public:
    virtual ValueTypePtr getDefaultInitValue() const = 0;
    virtual ValueTypePtr getNullValue() const = 0;
    virtual uint32_t getSizeBytes() const = 0;
    virtual const std::string toString() const = 0;
    virtual const std::string convertRawToString(void* data) const = 0;
    virtual const CodeExpressionPtr getCode() const = 0;
    virtual const CodeExpressionPtr getTypeDefinitionCode() const = 0;
    virtual const DataTypePtr copy() const = 0;
    virtual ~DataType();
protected:
  DataType();
  DataType(const DataType&);
  DataType& operator=(const DataType&);
};

class ValueType {
  public:
    virtual const DataTypePtr getType() const = 0;
    virtual const CodeExpressionPtr getCodeExpression() const = 0;
    virtual const ValueTypePtr copy() const = 0;
    virtual const bool isArrayValueType() const = 0;
    virtual ~ValueType();
protected:
  ValueType();
  ValueType(const ValueType&);
  ValueType& operator=(const ValueType&);
};

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class AttributeField {
  public:
    AttributeField(const std::string& name, DataTypePtr data_type);
    AttributeField(const std::string& name, const BasicType&);
    AttributeField(const std::string& name, uint32_t dataTypeSize);

    std::string name;
    DataTypePtr data_type;
    uint32_t getFieldSize() const;
    const DataTypePtr getDataType() const;
    const std::string toString() const;

    const AttributeFieldPtr copy() const;

    bool isEqual(const AttributeField& attr);
    bool isEqual(const AttributeFieldPtr& attr);
  //protected:
    AttributeField(const AttributeField&);
    AttributeField& operator=(const AttributeField&);
  private:

};

const AttributeFieldPtr createField(const std::string name, const BasicType& type);
const AttributeFieldPtr createField(const std::string name, uint32_t size);

const DataTypePtr createDataType(const BasicType&);
const DataTypePtr createDataTypeVarChar(const uint32_t& max_length);

const DataTypePtr createPointerDataType(const DataTypePtr& type);

const DataTypePtr createPointerDataType(const BasicType& type);

const ValueTypePtr createBasicTypeValue(const BasicType& type, const std::string& value);

const ValueTypePtr createStringTypeValue(const std::string& value, bool stringFlag = true);

} // namespace iotdb
#endif

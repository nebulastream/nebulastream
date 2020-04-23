#ifndef CORE_DATATYPES_H
#define CORE_DATATYPES_H

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class Statement;
typedef std::shared_ptr<Statement> StatementPtr;

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class AssignmentStatment;

enum BasicType {
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    FLOAT32,
    UINT64,
    FLOAT64,
    BOOLEAN,
    CHAR,
    DATE,
    VOID_TYPE,
    UNDEFINED
};

class DataType {
  public:
    virtual ValueTypePtr getDefaultInitValue() const = 0;
    virtual ValueTypePtr getNullValue() const = 0;
    virtual uint32_t getSizeBytes() const = 0;
    virtual const std::string toString() const = 0;
    virtual const std::string convertRawToString(void* data) const = 0;
    virtual const CodeExpressionPtr getDeclCode(const std::string& identifier) const = 0;
    virtual const StatementPtr getStmtCopyAssignment(const AssignmentStatment& aParam) const;
    virtual const CodeExpressionPtr getCode() const = 0;
    virtual const bool isEqual(DataTypePtr ptr) const = 0;
    virtual const bool isArrayDataType() const = 0;
    virtual const bool isCharDataType() const = 0;
    virtual const bool isUndefined() const;
    virtual const bool isNumerical() const;
    virtual const DataTypePtr join(DataTypePtr other) const;
    virtual const CodeExpressionPtr getTypeDefinitionCode() const = 0;
    virtual const DataTypePtr copy() const = 0;
    virtual ~DataType();
    virtual bool operator==(const DataType& rhs) const = 0;
  protected:
    DataType() = default;
    DataType(const DataType&);
    DataType& operator=(const DataType&);
  private:
    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {

    }
};

const AttributeFieldPtr createField(const std::string name, const BasicType& type);
const AttributeFieldPtr createField(const std::string name, uint32_t size);
const AttributeFieldPtr createField(const std::string name, DataTypePtr type);

const DataTypePtr createDataType(const BasicType&);
const DataTypePtr createUndefinedDataType();
const DataTypePtr createDataTypeVarChar(const uint32_t& max_length);
const DataTypePtr createPointerDataType(const DataTypePtr& type);
const DataTypePtr createPointerDataType(const BasicType& type);

const DataTypePtr createReferenceDataType(const DataTypePtr& type);
const DataTypePtr createReferenceDataType(const BasicType& type);


const ValueTypePtr createBasicTypeValue(const BasicType& type, const std::string& value);
const DataTypePtr createArrayDataType(const BasicType& type, uint32_t dimension);
const ValueTypePtr createStringValueType(const std::string& value);
const ValueTypePtr createStringValueType(const char* value, u_int16_t dimension = 0);
const ValueTypePtr createArrayValueType(const BasicType& type, const std::vector<std::string>& value);

} // namespace NES
#endif

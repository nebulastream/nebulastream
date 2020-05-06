#ifndef INCLUDE_ARRAYDATATYPE_HPP_
#define INCLUDE_ARRAYDATATYPE_HPP_

#pragma once

#include <API/Types/DataTypes.hpp>
#include <QueryCompiler/DataTypes/BasicDataType.hpp>

#include <boost/serialization/shared_ptr.hpp>

namespace NES {

class ArrayDataType;
typedef std::shared_ptr<ArrayDataType> ArrayDataTypePtr;

class ArrayDataType : public DataType {
  public:
    ArrayDataType() = default;
    ArrayDataType(DataTypePtr ptr, u_int32_t dimension);

    ValueTypePtr getDefaultInitValue() const override;
    ValueTypePtr getNullValue() const override;
    uint32_t getSizeBytes() const override;
    const std::string toString() const override;
    const std::string convertRawToString(void* data) const override;

    const CodeExpressionPtr getDeclCode(const std::string& identifier) const override;

    virtual const StatementPtr getStmtCopyAssignment(const AssignmentStatment& aParam) const override;

    const CodeExpressionPtr getCode() const override;

    const bool isEqual(DataTypePtr ptr) const override;
    const bool isCharDataType() const override;
    const bool isArrayDataType() const override;

    const CodeExpressionPtr getTypeDefinitionCode() const override;
    const u_int32_t getDimensions() const;
    DataTypePtr getComponentDataType();

    const DataTypePtr copy() const override;
    virtual ~ArrayDataType() override;

    bool operator==(const DataType& _rhs) const override;

  private:
    DataTypePtr componentDataType;
    u_int32_t dimensions;

    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(DataType);
        ar& componentDataType;
        ar& BOOST_SERIALIZATION_NVP(dimensions);
    }
};
}// namespace NES

#endif//INCLUDE_ARRAYDATATYPE_HPP_

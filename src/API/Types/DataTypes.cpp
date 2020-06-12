#include <API/Types/DataTypes.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>

namespace NES {

DataType::DataType(const DataType&) {
}

DataType& DataType::operator=(const DataType&) {
    return *this;
}

const StatementPtr DataType::getStmtCopyAssignment(const AssignmentStatment& aParam) const {
    return VarRef(aParam.lhs_tuple_var)[VarRef(aParam.lhs_index_var)].accessRef(VarRef(aParam.lhs_field_var)).assign(VarRef(aParam.rhs_tuple_var)[VarRef(aParam.rhs_index_var)].accessRef(VarRef(aParam.rhs_field_var))).copy();
}

bool DataType::isUndefined() const {
    return false;
}

bool DataType::isNumerical() const {
    return false;
}

const DataTypePtr DataType::join(DataTypePtr other) const {
    // if both types are equal, the join type is the same.
    if (isEqual(other)) {
        return other;
    }
    return createUndefinedDataType();
}

DataType::~DataType() {}

}// namespace NES

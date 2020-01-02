#include <API/Types/DataTypes.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>

namespace iotdb {

DataType::DataType(const DataType &) {

}

DataType &DataType::operator=(const DataType &) {
  return *this;
}

const StatementPtr DataType::getStmtCopyAssignment(const AssignmentStatment &aParam) const {
  return VarRef(aParam.lhs_tuple_var)[VarRef(aParam.lhs_index_var)].accessRef(VarRef(aParam.lhs_field_var))
      .assign(VarRef(aParam.rhs_tuple_var)[VarRef(aParam.rhs_index_var)].accessRef(VarRef(aParam.rhs_field_var)))
      .copy();
}

DataType::~DataType() {}

} // namespace iotdb

//BOOST_CLASS_EXPORT(iotdb::PointerDataType);
//BOOST_CLASS_EXPORT(iotdb::ArrayDataType);
//BOOST_CLASS_EXPORT(iotdb::ArrayValueType);

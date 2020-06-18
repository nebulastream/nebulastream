
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
namespace NES{

StatementPtr GeneratableDataType::getStmtCopyAssignment(const AssignmentStatment& aParam) {
    return VarRef(aParam.lhs_tuple_var)[VarRef(aParam.lhs_index_var)].accessRef(VarRef(aParam.lhs_field_var)).assign(VarRef(aParam.rhs_tuple_var)[VarRef(aParam.rhs_index_var)].accessRef(VarRef(aParam.rhs_field_var))).copy();
}

}
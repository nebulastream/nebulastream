
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
namespace NES {

StatementPtr GeneratableDataType::getStmtCopyAssignment(const AssignmentStatment& assignmentStatement) {
    // generates code for target = source
    auto target = VarRef(assignmentStatement.lhs_tuple_var)[VarRef(assignmentStatement.lhs_index_var)]
                      .accessRef(VarRef(assignmentStatement.lhs_field_var));
    auto source = VarRef(assignmentStatement.rhs_tuple_var)[VarRef(assignmentStatement.rhs_index_var)]
                      .accessRef(VarRef(assignmentStatement.rhs_field_var));
    return target.assign(source).copy();
}

}// namespace NES
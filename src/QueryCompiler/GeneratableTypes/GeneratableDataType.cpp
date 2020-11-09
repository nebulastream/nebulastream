/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/


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
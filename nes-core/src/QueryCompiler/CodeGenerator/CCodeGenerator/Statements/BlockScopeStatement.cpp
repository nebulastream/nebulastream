/*
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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BlockScopeStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <sstream>

namespace NES::QueryCompilation {
BlockScopeStatementPtr BlockScopeStatement::create() { return std::make_shared<BlockScopeStatement>(); }

CodeExpressionPtr BlockScopeStatement::getCode() const {
    return std::make_shared<CodeExpression>("{" + CompoundStatement::getCode()->code_ + "}");
}
}// namespace NES::QueryCompilation

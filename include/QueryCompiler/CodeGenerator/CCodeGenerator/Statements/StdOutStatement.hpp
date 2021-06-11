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

#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_STDOUT__HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_STDOUT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
namespace NES {
namespace QueryCompilation {
/**
 * @brief this statements allows us to generate code printing to std::out
 */
class StdOutStatement : public Statement {
  public:
    explicit StdOutStatement(const std::string message);

    [[nodiscard]] StatementType getStamentType() const override;

    [[nodiscard]] const CodeExpressionPtr getCode() const override;

    [[nodiscard]] const StatementPtr createCopy() const override;

    ~StdOutStatement() override;

  private:
    std::string message;
};

using StdOut = StdOutStatement;
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR__HPP_

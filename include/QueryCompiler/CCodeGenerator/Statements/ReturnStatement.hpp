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

#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_RETURNSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_RETURNSTATEMENT_HPP_

#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarRefStatement.hpp>
namespace NES {

class ReturnStatement : public Statement {
  public:
    ReturnStatement(VarRefStatement var_ref);

    StatementType getStamentType() const override;

    const CodeExpressionPtr getCode() const override;

    const StatementPtr createCopy() const override;

    ~ReturnStatement() override;

  private:
    VarRefStatement var_ref_;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_RETURNSTATEMENT_HPP_

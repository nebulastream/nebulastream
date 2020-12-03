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

#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_FUNCTIONCALLSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_FUNCTIONCALLSTATEMENT_HPP_
#include <QueryCompiler/CCodeGenerator/Statements/ExpressionStatement.hpp>
#include <vector>
namespace NES {

class FunctionCallStatement : public ExpressionStatment {
  public:
    FunctionCallStatement(const std::string functionname);

    virtual StatementType getStamentType() const;

    virtual const CodeExpressionPtr getCode() const;
    static FunctionCallStatementPtr create(const std::string functionname);

    virtual const ExpressionStatmentPtr copy() const;

    virtual void addParameter(const ExpressionStatment& expr);
    virtual void addParameter(ExpressionStatmentPtr expr);

    virtual ~FunctionCallStatement();

  private:
    std::string functionName;
    std::vector<ExpressionStatmentPtr> expressions;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_FUNCTIONCALLSTATEMENT_HPP_

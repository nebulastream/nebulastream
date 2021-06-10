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

#pragma once
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>

namespace NES {
namespace QueryCompilation {

class ExpressionStatment : public Statement {
  public:
    StatementType getStamentType() const override = 0;
    const CodeExpressionPtr getCode() const override = 0;
    /** @brief virtual copy constructor */
    virtual const ExpressionStatmentPtr copy() const = 0;
    /** @brief virtual copy constructor of base class
     *  todo create one unified version, having this twice is problematic
    */
    const StatementPtr createCopy() const final;

    BinaryOperatorStatement assign(const ExpressionStatment& ref);
    BinaryOperatorStatement assign(ExpressionStatmentPtr ref);
    BinaryOperatorStatement accessPtr(const ExpressionStatment& ref);
    BinaryOperatorStatement accessPtr(const ExpressionStatmentPtr ref);
    BinaryOperatorStatement accessRef(const ExpressionStatment& ref);
    BinaryOperatorStatement accessRef(const ExpressionStatmentPtr ref);
    BinaryOperatorStatement operator[](const ExpressionStatment& ref);

    ~ExpressionStatment() override;
};
}// namespace QueryCompilation

}// namespace NES
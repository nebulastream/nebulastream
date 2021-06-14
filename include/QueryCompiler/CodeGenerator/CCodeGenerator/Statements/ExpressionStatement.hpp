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

namespace NES::QueryCompilation {

class ExpressionStatment : public Statement {
  public:
    [[nodiscard]] StatementType getStamentType() const override = 0;
    [[nodiscard]] CodeExpressionPtr getCode() const override = 0;
    /** @brief virtual copy constructor */
    [[nodiscard]] virtual ExpressionStatmentPtr copy() const = 0;
    /** @brief virtual copy constructor of base class
     *  todo create one unified version, having this twice is problematic
    */
    [[nodiscard]] StatementPtr createCopy() const final;

    BinaryOperatorStatement assign(const ExpressionStatment& ref);
    [[nodiscard]] BinaryOperatorStatement assign(ExpressionStatmentPtr ref) const;
    BinaryOperatorStatement accessPtr(const ExpressionStatment& ref);
    [[nodiscard]] BinaryOperatorStatement accessPtr(ExpressionStatmentPtr const& ref) const;
    BinaryOperatorStatement accessRef(const ExpressionStatment& ref);
    [[nodiscard]] BinaryOperatorStatement accessRef(ExpressionStatmentPtr ref) const;
    BinaryOperatorStatement operator[](const ExpressionStatment& ref);

    ExpressionStatment() = default;
    ExpressionStatment(const ExpressionStatment&) = default;
    ~ExpressionStatment() override = default;
};

}// namespace NES::QueryCompilation
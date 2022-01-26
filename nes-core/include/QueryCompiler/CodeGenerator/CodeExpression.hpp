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

#ifndef NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CODEEXPRESSION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CODEEXPRESSION_HPP_

#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

namespace NES::QueryCompilation {

class CodeExpression {
  public:
    template<typename T, typename = std::enable_if_t<std::is_constructible_v<std::string, std::decay_t<T>>>>
    inline explicit CodeExpression(T&& code) noexcept : code_(std::forward<T>(code)) {}
    std::string const code_;
};

CodeExpressionPtr combine(CodeExpressionPtr const& lhs, CodeExpressionPtr const& rhs);

}// namespace NES::QueryCompilation
#endif  // NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CODEEXPRESSION_HPP_

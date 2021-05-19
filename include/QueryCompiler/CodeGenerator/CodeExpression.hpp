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

#include <memory>
#include <string>

namespace NES {
namespace QueryCompilation {
class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class CodeExpression {
  public:
    inline CodeExpression(std::string const& code) noexcept : code_(code) {}
    inline CodeExpression(std::string&& code) noexcept : code_(std::move(code)) {}
    std::string const code_;
};

const CodeExpressionPtr combine(const CodeExpressionPtr lhs, const CodeExpressionPtr rhs);
}// namespace QueryCompilation
}// namespace NES

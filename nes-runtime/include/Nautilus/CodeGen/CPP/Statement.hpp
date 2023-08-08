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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_STATEMENT_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_STATEMENT_HPP_

#include <Nautilus/CodeGen/CodeGenerator.hpp>

namespace NES::Nautilus::CodeGen::CPP {

/**
 * @brief A `Statement` can be any C++ code that is a declaration, assignment or `goto` statement.
 */
class Statement : public CodeGenerator {
public:
    /**
     * @brief Constructor.
     * @param statement the statement
     */
    explicit Statement(std::string statement);

    [[nodiscard]] std::string toString() const override;

private:
    std::string statement;
};

} // namespace NES::Nautilus::CodeGen::CPP

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_STATEMENT_HPP_

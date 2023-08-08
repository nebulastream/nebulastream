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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_CPPCODEGENERATOR_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_CPPCODEGENERATOR_HPP_

#include <Nautilus/CodeGen/CodeGenerator.hpp>

#include <Nautilus/CodeGen/CPP/Function.hpp>

#include <set>
#include <vector>

namespace NES::Nautilus::CodeGen::CPP {

/**
 * @brief This class is the top-level code generator for a C++ source file.
 */
class CPPCodeGenerator : public CodeGenerator {
public:
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Add a header file to the include section.
     * @param file the header file to include (e.g. `<iostream>`)
     */
    void addInclude(const std::string& file);

    /**
     * @brief Add a declaration to the declarations section.
     * @param declaration the variable/function declaration
     */
    void addDeclaration(const std::string& declaration);

    /**
     * @brief Add a function code generator to the top-level code generator.
     * @param fn the function code generator
     */
    void addFunction(const std::shared_ptr<Function>& fn);

private:
    std::set<std::string> includes;
    std::set<std::string> declarations;
    std::vector<std::shared_ptr<Function>> functions;
};

} // namespace NES::Nautilus::CodeGen::CPP

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_CPPCODEGENERATOR_HPP_

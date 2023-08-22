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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_STRUCT_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_STRUCT_HPP_

#include <Nautilus/CodeGen/CodeGenerator.hpp>

#include <vector>

namespace NES::Nautilus::CodeGen::CPP {

/**
 * @brief A `Struct` is a code generator for a C++ struct.
 */
class Struct : public CodeGenerator {
public:
    /**
     * @brief Constructor.
     * @param name the name
     */
    explicit Struct(std::string name);

    /**
     * @brief Add a field to the struct.
     * @param type the type
     * @param name the name
     */
    void addField(std::string type, std::string name);

    /**
     * @return The name of the struct.
     */
    const std::string& getName();

    [[nodiscard]] std::string toString() const override;

private:
    std::string name;
    std::vector<std::string> fields;
};

} // namespace NES::Nautilus::CodeGen::CPP

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_STRUCT_HPP_

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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_FUNCTION_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_FUNCTION_HPP_

#include <Nautilus/CodeGen/Segment.hpp>

#include <vector>

namespace NES::Nautilus::CodeGen::CPP {

/**
 * @brief A `Function` is a code generator for a C++ function.
 */
class Function : public CodeGenerator {
public:
    /**
     * @brief Constructor.
     * @param type the return type of the function (this also includes modifiers such as `extern "C"` for now)
     * @param name the function name
     * @param parameters the list of parameters with each entry defining the type and name (e.g. `int foo`)
     */
    Function(std::string type, std::string name, std::vector<std::string> parameters);

    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Add a `Segment` to the ordered list of code generators.
     */
    void addSegment(const std::shared_ptr<Segment>& segment);

    /**
     * @return Get the signature of this function, i.e. the combination of type, name and parameters.
     */
    [[nodiscard]] std::string getSignature() const;

private:
    std::string type;
    std::string name;
    std::vector<std::string> parameters;
    std::vector<std::shared_ptr<Segment>> segments;
};

} // namespace NES::Nautilus::CodeGen::CPP

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_FUNCTION_HPP_

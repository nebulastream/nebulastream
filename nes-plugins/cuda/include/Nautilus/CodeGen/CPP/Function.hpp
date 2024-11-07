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

#ifndef NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_CODEGEN_CPP_FUNCTION_HPP_
#define NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_CODEGEN_CPP_FUNCTION_HPP_

#include <Nautilus/CodeGen/Segment.hpp>

#include <vector>

namespace NES::Nautilus::CodeGen::CPP {

/**
 * @brief Represent a function that a C++ code can later be generated from it.
 * It includes the function's specifiers, return type, name, parameters, and body segments.
 */
class Function : public CodeGenerator {
  public:
    /**
     * @brief Creates a Function object.
     * @param specifiers the function specifiers (e.g. `extern "C"`)
     * @param type the return type of the function
     * @param name the function name
     * @param parameters the list of parameters with each entry defining the type and name (e.g. `int foo`)
     */
    Function(std::vector<std::string> specifiers, std::string type, std::string name, std::vector<std::string> parameters);

    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Add a `Segment` to the ordered list of code generators.
     * @param segment to be added to the function body
     */
    void addSegment(const std::shared_ptr<Segment>& segment);

    /**
     * @brief Retrieve the signature of this function
     * @return Signature of this function, i.e. the combination of return type, function name and function parameters.
     */
    [[nodiscard]] std::string getSignature() const;

  private:
    const std::vector<std::string> specifiers;
    const std::string type;
    const std::string name;
    const std::vector<std::string> parameters;
    std::vector<std::shared_ptr<Segment>> segments;
};

}// namespace NES::Nautilus::CodeGen::CPP

#endif// NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_CODEGEN_CPP_FUNCTION_HPP_

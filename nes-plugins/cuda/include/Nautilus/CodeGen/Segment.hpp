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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_SEGMENT_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_SEGMENT_HPP_

#include <Nautilus/CodeGen/CodeGenerator.hpp>

#include <vector>

namespace NES::Nautilus::CodeGen {

class Segment : public CodeGenerator {
public:
    [[nodiscard]] std::string toString() const override;

    /**
     * @brief Add a `CodeGenerator` to the prolog section, which is defined to be at the top of the generated code.
     * @param codeGen the code generator
     */
    void addToProlog(const std::shared_ptr<CodeGenerator>& codeGen);

    /**
     * @return Get the code generators assigned to the prolog section.
     */
    [[nodiscard]] const std::vector<std::shared_ptr<CodeGenerator>>& getProlog() const;

    /**
     * @brief Merge the prolog of code generators from the main section to this prolog.
     */
    void mergeProloguesToTopLevel();

    /**
     * @brief Add a `CodeGenerator` to the main section.
     * @param codeGen the code generator
     */
    void add(const std::shared_ptr<CodeGenerator>& codeGen);

private:
    std::vector<std::shared_ptr<CodeGenerator>> prolog;
    std::vector<std::shared_ptr<CodeGenerator>> codeGenerators;
};

} // namespace NES::Nautilus::CodeGen

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_SEGMENT_HPP_

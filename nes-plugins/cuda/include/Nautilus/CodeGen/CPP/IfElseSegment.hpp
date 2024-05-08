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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_IFELSESEGMENT_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_IFELSESEGMENT_HPP_

#include <Nautilus/CodeGen/Segment.hpp>

namespace NES::Nautilus::CodeGen::CPP {

/**
 * An `IfElseSegment` abstracts the code generation of an `if-else` block.
 */
class IfElseSegment : public Segment {
public:
    /**
     * @brief Constructor.
     * @param condition the condition to evaluate (represented as `std::string`)
     * @param ifSegment the code generator for the `if` branch
     * @param elseSegment the code generator for the `else` branch
     */
    IfElseSegment(std::string condition, std::shared_ptr<Segment> ifSegment, std::shared_ptr<Segment> elseSegment);

    [[nodiscard]] std::string toString() const override;

private:
    std::string condition;
    std::shared_ptr<Segment> ifSegment;
    std::shared_ptr<Segment> elseSegment;
};

} // namespace NES::Nautilus::CodeGen::CPP

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_IFELSESEGMENT_HPP_

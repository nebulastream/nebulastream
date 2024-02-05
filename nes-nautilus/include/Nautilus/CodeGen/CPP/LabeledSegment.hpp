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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_LABELEDSEGMENT_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_LABELEDSEGMENT_HPP_

#include <Nautilus/CodeGen/Segment.hpp>

namespace NES::Nautilus::CodeGen::CPP {

/**
 * @brief A `LabeledSegment` is a `Segment` but with a label attached to it with which we can reference this
 * code segment in the generated code using a `goto` statement.
 */
class LabeledSegment : public Segment {
public:
    /**
     * @brief Constructor.
     * @param label the label
     */
    explicit LabeledSegment(std::string label);

    [[nodiscard]] std::string toString() const override;

    /**
     * @return Get the label attached to this code segment.
     */
    [[nodiscard]] const std::string& getLabel() const;

private:
    std::string label;
};

} // namespace NES::Nautilus::CodeGen::CPP

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_CODEGEN_CPP_LABELEDSEGMENT_HPP_

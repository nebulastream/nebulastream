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

#pragma once
#include <Functions/NodeFunctionBinary.hpp>

namespace NES {

/// Performs a concatenation of two variable sized data types.
class NodeFunctionConcat : public NodeFunctionBinary
{
public:
    NodeFunctionConcat(std::shared_ptr<DataType> stamp);
    static std::shared_ptr<NodeFunction> create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right);
    ~NodeFunctionConcat() noexcept override = default;
    bool validateBeforeLowering() const override;
    std::shared_ptr<NodeFunction> deepCopy() override;
    bool equal(const std::shared_ptr<Node>& rhs) const override;

protected:
    [[nodiscard]] std::string toString() const override;

};

}
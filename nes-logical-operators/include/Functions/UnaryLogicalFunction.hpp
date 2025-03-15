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

#include <array>
#include <memory>
#include <utility>
#include <string>
#include <Functions/LogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
class UnaryLogicalFunction : public LogicalFunction
{
public:
    [[nodiscard]] LogicalFunction& getChild() const;
    void setChild(std::unique_ptr<LogicalFunction> child);
    std::span<const std::unique_ptr<LogicalFunction>> getChildren() const override;

protected:
    explicit UnaryLogicalFunction(std::unique_ptr<LogicalFunction> child);
    explicit UnaryLogicalFunction(const UnaryLogicalFunction& other);
    std::array<std::unique_ptr<LogicalFunction>, 1> children;
};
}

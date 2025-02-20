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
#include <memory>
#include <Functions/BinaryLogicalFunction.hpp>

namespace NES
{

class EqualsLogicalFunction final : public BinaryLogicalFunction
{
public:
    static constexpr std::string_view NAME = "Equals";

    EqualsLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right);
    ~EqualsLogicalFunction() override = default;

    [[nodiscard]] SerializableFunction serialize() const override;

    bool validateBeforeLowering() const;

    [[nodiscard]] bool operator==(const std::shared_ptr<LogicalFunction>& rhs) const override;
    [[nodiscard]]  std::shared_ptr<LogicalFunction> clone() const override;

private:
    explicit EqualsLogicalFunction(const EqualsLogicalFunction& other);
    [[nodiscard]] std::string toString() const override;
};
}
FMT_OSTREAM(NES::EqualsLogicalFunction);

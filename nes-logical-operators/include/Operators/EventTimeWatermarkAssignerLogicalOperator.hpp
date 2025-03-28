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

#include "API/TimeUnit.hpp"
#include "Functions/LogicalFunction.hpp"
#include "UnaryLogicalOperator.hpp"

namespace NES
{

class EventTimeWatermarkAssignerLogicalOperator : public UnaryLogicalOperator
{
public:
    static constexpr std::string_view NAME = "EventTimeWatermarkAssigner";

    EventTimeWatermarkAssignerLogicalOperator(std::shared_ptr<LogicalFunction> onField, Windowing::TimeUnit unit);
    std::string_view getName() const noexcept override;

    [[nodiscard]] bool operator==(const Operator& rhs) const override;
    [[nodiscard]] bool isIdentical(const Operator& rhs) const override;
    std::shared_ptr<Operator> clone() const override;
    bool inferSchema() override;

    [[nodiscard]] SerializableOperator serialize() const override;

    [[nodiscard]] std::string toString() const override;

private:
    std::shared_ptr<LogicalFunction> onField;
    Windowing::TimeUnit unit;
};
}

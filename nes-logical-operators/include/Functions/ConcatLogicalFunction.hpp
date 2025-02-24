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
#include <string>
#include <Functions/LogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

class ConcatLogicalFunction final : public BinaryLogicalFunction
{
public:
    static constexpr std::string_view NAME = "Concat";

    explicit ConcatLogicalFunction(std::shared_ptr<LogicalFunction> const& left, std::shared_ptr<LogicalFunction> const& right);
    ~ConcatLogicalFunction() noexcept override = default;

    [[nodiscard]] SerializableFunction serialize() const override;

    void inferStamp(const Schema& schema) override;
    [[nodiscard]] bool operator==(std::shared_ptr<LogicalFunction> const& rhs) const override;
    std::shared_ptr<LogicalFunction> clone() const override;

private:
    explicit ConcatLogicalFunction(const ConcatLogicalFunction& other);
    std::string toString() const override;

    const std::string constantValue;
};
}
FMT_OSTREAM(NES::ConcatLogicalFunction);

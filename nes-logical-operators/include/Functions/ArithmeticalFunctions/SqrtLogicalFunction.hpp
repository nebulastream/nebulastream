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
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnaryLogicalFunction.hpp>

#include <Common/DataTypes/DataType.hpp>
namespace NES
{

class SqrtLogicalFunction final : public UnaryLogicalFunction
{
public:
    explicit SqrtLogicalFunction(std::shared_ptr<DataType> stamp);
    ~SqrtLogicalFunction() noexcept override = default;
    [[nodiscard]] static std::shared_ptr<LogicalFunction> create(std::shared_ptr<LogicalFunction> const& child);
    [[nodiscard]] bool equal(std::shared_ptr<LogicalFunction> const& rhs) const override;

    std::shared_ptr<LogicalFunction> clone() const override;

protected:
    explicit SqrtLogicalFunction(SqrtLogicalFunction* other);

    [[nodiscard]] std::string toString() const override;
};

}

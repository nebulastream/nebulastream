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

#include <Functions/BinaryLogicalFunction.hpp>

namespace NES
{
class OrLogicalFunction final : public BinaryLogicalFunction
{
public:
    OrLogicalFunction();
    ~OrLogicalFunction() override = default;

    static std::shared_ptr<LogicalFunction>
    create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right);
    [[nodiscard]] bool equal(const std::shared_ptr<LogicalFunction>& rhs) const override;
    void inferStamp(const Schema& schema) override;

    std::shared_ptr<LogicalFunction> clone() const override;

protected:
    explicit OrLogicalFunction(OrLogicalFunction* other);

    [[nodiscard]] std::string toString() const override;
};
}

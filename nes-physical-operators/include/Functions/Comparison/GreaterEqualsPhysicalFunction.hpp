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
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES::Functions
{

class GreaterEqualsPhysicalFunction final : public PhysicalFunction
{
public:
    GreaterEqualsPhysicalFunction(std::unique_ptr<PhysicalFunction> leftPhysicalFunction, std::unique_ptr<PhysicalFunction> rightPhysicalFunction);
    [[nodiscard]] VarVal execute(const Record& record) const override;

private:
    const std::unique_ptr<PhysicalFunction> leftPhysicalFunction;
    const std::unique_ptr<PhysicalFunction> rightPhysicalFunction;
};
}

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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// A function that represents a constant value of variable size, e.g., a string.
class ConstantValueVariableSizePhysicalFunction final : public PhysicalFunctionConcept
{
public:
    explicit ConstantValueVariableSizePhysicalFunction(const int8_t* value, size_t size);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override;
    void collectRuntimeDynamicPointerBindings(
        std::string_view namePrefix, std::vector<RuntimeDynamicPointerBinding>& dynamicPointerBindings) const override
    {
        dynamicPointerName = appendDynamicPointerBindingName(namePrefix, ":data");
        if (data != nullptr && data->data() != nullptr)
        {
            dynamicPointerBindings.emplace_back(RuntimeDynamicPointerBinding::create(dynamicPointerName, data, data->data()));
        }
    }

private:
    std::shared_ptr<std::vector<int8_t>> data;
    mutable std::string dynamicPointerName;
};
}

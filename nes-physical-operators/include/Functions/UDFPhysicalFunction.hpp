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
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <UdfBackend.hpp>

namespace NES
{

/// Executes a scalar user-defined function. Evaluates its argument functions, marshals them into a
/// staging buffer, and calls the loaded UdfBackend once per record via nautilus::invoke. v1 supports
/// fixed-width scalar arguments and a scalar result (VARSIZED is rejected at lowering).
///
/// The backend is held by shared_ptr so its address is stable across the nautilus::invoke boundary,
/// mirroring InferModelPhysicalOperator's runtime wrapper.
class UDFPhysicalFunction final
{
public:
    UDFPhysicalFunction(
        std::vector<PhysicalFunction> arguments,
        std::vector<DataType> argumentTypes,
        DataType returnType,
        std::shared_ptr<UdfBackend> backend);

    VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    std::vector<PhysicalFunction> arguments;
    std::vector<DataType> argumentTypes;
    DataType returnType;
    std::shared_ptr<UdfBackend> backend;
};

}

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

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Physical function that decodes a PNG image and emits the fixed 1x3x64x64 float32 tensor expected by the AutoVI ONNX model.
class AutoViPreprocessImagePhysicalFunction final
{
public:
    explicit AutoViPreprocessImagePhysicalFunction(PhysicalFunction childPhysicalFunction);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    PhysicalFunction childPhysicalFunction;
};

static_assert(PhysicalFunctionConcept<AutoViPreprocessImagePhysicalFunction>);

}

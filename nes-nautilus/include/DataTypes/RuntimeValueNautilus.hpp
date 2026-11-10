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

#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/VarVal.hpp>

namespace NES
{

/// Runtime-only bridge, not for extracting native values while tracing a compiled pipeline.
/// toVarVal borrows VARSIZED bytes from the view's owner; neither moving nor destroying a VarVal transfers that ownership.
[[nodiscard]] VarVal toVarVal(RuntimeValueView value);

/// Materializes an independent native value, copying VARSIZED bytes and preserving the declared nullability.
[[nodiscard]] RuntimeValue fromVarVal(const VarVal& value, DataType type);

}

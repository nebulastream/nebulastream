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

#include <vector>
#include <DataTypes/DataType.hpp>

/// Comes first: DynamicBase.hpp uses PRECONDITION without including where it is defined.
#include <ErrorHandling.hpp>

#include <Util/DynamicBase.hpp>

namespace NES
{

/// Marker for a logical function that has an asynchronous implementation and no Nautilus one.
/// Detected with `LogicalFunction::tryGetAs<AsyncLogicalFunction>()`; `AsyncFunctionLiftingRule`
/// moves every such call into an operator that is executed asynchronously.
/// Its arguments are fields, constants, or directly nested async calls by the time it is executed.
struct AsyncLogicalFunction : Castable<AsyncLogicalFunction>
{
    /// Types produced by one asynchronous invocation.
    [[nodiscard]] virtual std::vector<DataType> getAsyncOutputTypes() const = 0;
};

}

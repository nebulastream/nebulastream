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
#include <string>

/// Comes first: DynamicBase.hpp uses PRECONDITION without including where it is defined.
#include <ErrorHandling.hpp>

#include <Util/DynamicBase.hpp>

namespace NES
{

/// Marker for a logical function that has an asynchronous implementation and no Nautilus one.
/// Detected with `LogicalFunction::tryGetAs<AsyncLogicalFunction>()`; `AsyncFunctionLiftingRule`
/// moves every such call into an operator that is executed asynchronously.
/// Its arguments are only ever plain fields or constants by the time it is executed.
struct AsyncLogicalFunction : Castable<AsyncLogicalFunction>
{
    /// Handed to the asynchronous implementation when it is created, for what is fixed per query rather than per row,
    /// e.g. a model from a catalog. It has to follow from what `explain` shows: calls that explain alike are evaluated once.
    [[nodiscard]] virtual std::string getAsyncConfiguration() const { return {}; }

    /// Values one call produces. The call evaluates to the first; `ASYNC_OUTPUT(call, i)` reads the i-th.
    [[nodiscard]] virtual size_t getNumberOfAsyncOutputs() const { return 1; }
};

}

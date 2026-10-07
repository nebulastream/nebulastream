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

#include <Traits/AsyncExecutionTrait.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

/// Turns a semantic model's configuration into the marker `AsyncOperatorSplitter` acts on. The
/// executor type follows the step list: "SemanticFilter" as soon as any step drops rows, otherwise
/// "SemanticMap". Only the model knows what the executor needs, so the whole payload is assembled
/// here and travels as plain data from this point on.
///
/// Used where a semantic operator gets its model — on resolution, and again when fusion replaces two
/// operators' models by one.
[[nodiscard]] AsyncExecutionTrait semanticAsyncExecution(const RegisteredSemanticModel& model);

}

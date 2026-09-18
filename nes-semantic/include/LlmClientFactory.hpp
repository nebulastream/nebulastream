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
#include <string>
#include <vector>

#include <LlmClient.hpp>
#include <SemanticModelConfig.hpp>

namespace NES
{

/// Creates the concrete `LlmClient` for a resolved semantic model, dispatched on the config's
/// base-URL scheme: `mock://` selects `MockLlmClient` (behaviour in the first path segment —
/// hermetic, answers never leave the process; production builds accept it too, a query against
/// `mock://` logs a warning and returns fake answers), `http://`/`https://` selects
/// `CurlLlmClient`. Anything else throws `CannotLoadModel`.
///
/// One call per worker thread; the returned client is single-threaded (plan §D6).
[[nodiscard]] std::unique_ptr<LlmClient>
createLlmClient(const SemanticModelConfig& config, const std::vector<std::string>& outputFieldNames);

}
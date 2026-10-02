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

#include <string>
#include <string_view>
#include <vector>

#include <SemanticModelCatalog.hpp>

namespace NES
{

/// Everything the asynchronous SEM_MAP executor needs, in the one form it can travel in.
///
/// `AsyncExecutionTrait` carries plain strings, because it is serialized into a source
/// descriptor and shipped to the worker. The model configuration is a nested structure, so
/// it goes over as one JSON string under `SemanticMapConfigKey`.
///
/// The API key is deliberately absent: the configuration names only the environment
/// variable, and the executor resolves it on the worker that runs it — the same rule
/// `LowerToPhysicalSemanticMap` follows for the synchronous path.
struct SemanticMapAsyncPayload
{
    SemanticModelConfig config;
    /// The model's declared INPUT fields, canonical names, in declared order.
    std::vector<std::string> inputFields;
    /// The model's declared OUTPUT fields, canonical names, one per step in step order.
    std::vector<std::string> outputFields;
};

/// Key under which the encoded payload sits in `AsyncExecutionTrait::config`.
inline constexpr std::string_view SemanticMapConfigKey = "semantic_model";

[[nodiscard]] std::string encodeSemanticMapPayload(const SemanticMapAsyncPayload& payload);

/// Throws `CannotDeserialize` when the string is not a payload this version wrote.
[[nodiscard]] SemanticMapAsyncPayload decodeSemanticMapPayload(std::string_view encoded);

}

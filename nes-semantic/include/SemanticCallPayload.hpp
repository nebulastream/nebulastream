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

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <SemanticModelCatalog.hpp>

namespace NES
{

/// Everything `SEM_CALL` needs at execution time, in the one form it can travel in: the string
/// `AsyncLogicalFunction::getAsyncConfiguration` hands to the async implementation, which the Async
/// source receives inside its descriptor and passes to `SemanticCallAsyncFunction` on the worker.
///
/// The API key is deliberately absent: the configuration names only the environment variable, and the
/// implementation resolves it on the worker that runs it.
struct SemanticCallPayload
{
    /// The catalog name of the model, `<first>+<second>` for a fused one. Shown by `explain`.
    std::string model;
    SemanticModelConfig config;
    /// The model's declared INPUT fields, canonical names, in declared order. `SEM_CALL` receives their
    /// values as its arguments in this order.
    std::vector<std::string> inputFields;
    /// The model's declared OUTPUT fields, canonical names, one per MAP step in step order.
    std::vector<std::string> outputFields;
    /// SEM_JOIN: the call judges one (left, right) pair with `SemanticJoinCodec`. The model is a filter model with two
    /// INPUT fields, the first one left; the call's single value is the verdict `'true'`/`'false'`.
    bool join = false;
    /// SEM_AGG: the call's single input is the JSON array JSON_AGG wrote for a window; `SemanticAggCodec` turns it into one
    /// prompt. The model is a map model with one INPUT and one OUTPUT field; the value is its answer.
    bool agg = false;
    /// SEM_SCAN and SEM_WINDOW (`config.keyed`): the id of the state the stage's function instances share, unique per
    /// translated operator. The call's arguments are the key, then for WINDOW the timestamp, then the INPUT fields.
    std::string stateId = {};
    /// A cascade's two calls (`config.proxy`). STUDENT: the call asks `config` (already the student's name, endpoint
    /// and LABEL mode) and has three values: the verdict, the route ("student", "teacher" or "audit") and p(true). TEACHER:
    /// its arguments are the route and the student's verdict, then the INPUT fields; it returns the student's verdict for
    /// a "student" row without a request, and asks the model for the others.
    enum class ProxyRole : uint8_t
    {
        NONE,
        STUDENT,
        TEACHER,
    };
    ProxyRole proxyRole = ProxyRole::NONE;
};

/// The cascade configuration as JSON, for the catalog's reflection, which keeps it as one string.
[[nodiscard]] std::string encodeSemanticProxyConfig(const SemanticProxyConfig& config);
[[nodiscard]] SemanticProxyConfig decodeSemanticProxyConfig(std::string_view encoded);

/// The keyed configuration as JSON, for the catalog's reflection, which keeps it as one string.
[[nodiscard]] std::string encodeSemanticKeyedConfig(const SemanticKeyedConfig& config);
[[nodiscard]] SemanticKeyedConfig decodeSemanticKeyedConfig(std::string_view encoded);

[[nodiscard]] std::string encodeSemanticCallPayload(const SemanticCallPayload& payload);

/// Throws `CannotDeserialize` when the string is not a payload this version wrote.
[[nodiscard]] SemanticCallPayload decodeSemanticCallPayload(std::string_view encoded);

}

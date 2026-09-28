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

#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <SemanticModelCatalog.hpp>

namespace NES
{

/// One record as the codec sees it: an id unique within one prompt plus the declared INPUT fields'
/// values, as text, in declared order.
struct RowPayload
{
    std::string rowId;
    std::vector<std::pair<std::string, std::string>> fields;
};

/// SEM_MAP's prompt layout and response decoding, following the Python reference
/// (`_process_fused_steps`). A single user message of four blocks:
///
///     <sysprompt>                describes the JSON response envelope
///     <operator prompt>          "Apply the following 1 operation to each row: ..."
///     [Dataset context: ...]     only if a dataset prompt is configured
///     Data: {"<row_id>": ..., ...}
///
/// Operator-specific by design: the transport (`SemanticBackend`) stays the same for every semantic
/// operator, while filter, agg or redact bring their own codec.
class SemanticMapCodec
{
public:
    explicit SemanticMapCodec(SemanticModelConfig config);

    [[nodiscard]] std::string buildPrompt(std::span<const RowPayload> rows) const;

    /// One entry per row, holding one answer per step in step order. Never fails: a row or field the
    /// response does not contain, an unparseable response and an answer outside the declared values
    /// all yield the step's `defaultValue`. SEM_MAP never drops a row.
    [[nodiscard]] std::vector<std::vector<std::string>> parse(std::string_view response, std::span<const RowPayload> rows) const;

private:
    SemanticModelConfig config;
};

}

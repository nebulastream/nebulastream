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

#include <SemanticModelCatalog.hpp>

namespace NES
{

/// SEM_AGG's prompt layout and response decoding, after `SEMAggOperator._process_agg_payload` of the Python reference
/// (without its optional chunking: one prompt per window):
///
///     <aggregation sysprompt>
///     Aggregation instruction: <prompt>
///     [Dataset context: ...]
///     Window:
///     [{"<input field>": "<value>"}, ...]
///
/// The model is a map model with one INPUT and one OUTPUT field; its prompt is the aggregation instruction. The values
/// arrive as the JSON array of strings JSON_AGG writes.
class SemanticAggCodec
{
public:
    SemanticAggCodec(SemanticModelConfig config, std::string inputField);

    /// Throws CannotDeserialize when `values` is not a JSON array of strings.
    [[nodiscard]] std::string buildPrompt(std::string_view values) const;

    /// The answer as free text: a whole answer in one code fence is unwrapped (the reference's `_strip_fence`), anything
    /// else kept verbatim. An empty answer gives the step's DEFAULT_VALUE.
    [[nodiscard]] std::string parse(std::string_view response) const;

private:
    SemanticModelConfig config;
    std::string inputField;
};

}

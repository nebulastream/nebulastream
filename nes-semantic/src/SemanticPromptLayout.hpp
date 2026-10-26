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
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

#include <SemanticModelCatalog.hpp>
#include <SemanticRowPayload.hpp>

/// Prompt rendering shared by the semantic codecs. Everything here follows the Python reference
/// byte for byte, because the prompt is the measured baseline's input; private to nes-semantic, the
/// public surface is the codecs.
namespace NES::detail
{

/// `self.sysprompt` of `Llm_operator`, which `_filter_with_llm` and `SEMJoinOperator` send unchanged. The typographic
/// apostrophe in "user’s" is part of the reference and goes out as UTF-8.
inline constexpr std::string_view BaseSysprompt
    = "You are a helpful AI assistant for semantic data operations.\n"
      "Always respond in JSON format.\n"
      "The response must have one key for each input row (_llm_call_id), and the value must be a JSON object with the answer and a "
      "confidence score between 0 and 1.\n"
      "Example output: {\n"
      "  \"row1\": {\"answer\": true, \"confidence\": 0.95},\n"
      "  \"row2\": {\"answer\": false, \"confidence\": 0.80}\n"
      "}\n"
      "Structure of the prompt:\n"
      "1. Operator prompt: The operator-specific instruction.\n"
      "2. User instructions: The user’s task or question.\n"
      "3. Dataset context (optional): background/domain description of the input dataset.\n"
      "4. Data: Input data, always in JSON format. Each data item (row) is a JSON object and always includes a unique row id field "
      "named _llm_call_id.\n"
      "Input data example: [ {\"_llm_call_id\": \"row1\", ...}, {\"_llm_call_id\": \"row2\", ...} ]\n"
      "Do not add explanations.";


/// A JSON string literal exactly as Python's `json.dumps` writes it with its defaults
/// (`ensure_ascii=True`).
void appendPythonJsonString(std::string& out, std::string_view text);

/// `json.dumps` separators: ", " between members, ": " between key and value.
template <typename Members, typename AppendValue>
void appendPythonJsonObject(std::string& out, const Members& members, AppendValue appendValue)
{
    out.push_back('{');
    bool first = true;
    for (const auto& member : members)
    {
        if (!first)
        {
            out += ", ";
        }
        first = false;
        appendValue(out, member);
    }
    out.push_back('}');
}

/// `str(raw)` of the reference for a non-string answer.
std::string pythonStr(const nlohmann::json& value);

/// The key each step's answer is requested under, in step order: a MAP step's output column, and
/// `__filter_<k>` for the k-th FILTER step, as `_make_filter_step` numbers them. Derived here rather
/// than read from the step, so a hand-built configuration cannot request two verdicts under one key.
std::vector<std::string> stepResponseColumns(const std::vector<SemanticStep>& steps);

/// The columns a response carries per row. Empty for a list of FILTER steps only: those are answered
/// in the flat `{row_id: {answer, confidence}}` envelope, everything else in the nested one.
std::vector<std::string> responseColumns(const std::vector<SemanticStep>& steps);

/// `_dataset_context_block`: "Dataset context: ...\n", or nothing.
std::string datasetContextBlock(const SemanticModelConfig& config);

/// "Data: " followed by `json.dumps({row_id: payload})`.
void appendDataBlock(std::string& prompt, std::span<const RowPayload> rows, PayloadFormat payloadFormat);

/// The `_process_fused_steps` layout for any step list: fused sysprompt, "Apply the following N
/// operations", optional dataset context, data block. A FILTER step is rendered with its
/// `(true/false)` suffix and a boolean example answer.
std::string buildFusedPrompt(const SemanticModelConfig& config, std::span<const RowPayload> rows);

/// The answer of one MAP step in a nested-envelope row, normalized against the step's declared
/// values; the step's default when the row, the field or its answer is missing.
std::string mapAnswer(
    const nlohmann::json* rowData, const SemanticStep& step, std::string_view column, std::string_view rowId, std::string_view response);

}

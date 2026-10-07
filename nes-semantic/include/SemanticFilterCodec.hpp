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
#include <vector>

#include <SemanticModelCatalog.hpp>
#include <SemanticRowPayload.hpp>

namespace NES
{

/// What a filter step list decides for one row.
struct FilterRowResult
{
    /// True only if every FILTER step's verdict is affirmative (see `isAffirmative`).
    bool passed = false;
    /// One answer per MAP step, in step order, exactly as `SemanticMapCodec` would produce it.
    /// Empty for a step list without MAP steps.
    std::vector<std::string> mapAnswers;
};

/// SEM_FILTER's prompt layout and response decoding, for any step list containing at least one
/// FILTER step. Two layouts, chosen the way the Python reference chooses between them:
///
/// Filter steps only (`_filter_with_llm`), a single filter or several fused ones:
///
///     <base sysprompt>           describes the flat {row_id: {answer, confidence}} envelope
///     Filter rows: <prompt>      or, for N > 1, "Filter rows that satisfy ALL of the following
///                                conditions: ..." (`_rebuild_filter_operator_prompt`)
///     [Dataset context: ...]
///     Data: {"<row_id>": ..., ...}
///
/// MAP and FILTER steps mixed (`_process_fused_steps`): the fused layout `SemanticMapCodec` uses,
/// with FILTER steps answering under `__filter_<k>` in the nested {row_id: {column: {answer, ...}}}
/// envelope.
class SemanticFilterCodec
{
public:
    explicit SemanticFilterCodec(SemanticModelConfig config);

    [[nodiscard]] std::string buildPrompt(std::span<const RowPayload> rows) const;

    /// One entry per row. Never fails: a row the response does not mention, a missing verdict and an
    /// unparseable response all drop the row, which is the reference's falsy default. MAP answers
    /// fall back to their step's default value, as in SEM_MAP.
    [[nodiscard]] std::vector<FilterRowResult> parse(std::string_view response, std::span<const RowPayload> rows) const;

private:
    SemanticModelConfig config;
    /// Whether every step is a FILTER step, which selects the flat layout and envelope.
    bool filterOnly;
    /// The key each step is answered under; see `detail::stepResponseColumns`.
    std::vector<std::string> columns;
};

}

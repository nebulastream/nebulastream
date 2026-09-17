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

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

namespace NES
{

/// Ports `_parse_llm_json` (llm_operator.py:31-74). Tries, in order: a straight parse, a parse of
/// the contents of a markdown code fence, a parse of the outermost `{...}` substring, and a parse
/// after repairing two known malformed id->object shapes (see plan §2.2). Returns an empty object
/// on total failure, exactly like the Python baseline (which then falls back to `default_value`).
[[nodiscard]] nlohmann::json parseLlmJson(std::string_view response);

/// Ports `_normalize_answer` (llm_operator.py:148-165). If `outputValues` is empty/absent, `answer`
/// is returned unchanged (free text). Otherwise cascades: uppercase exact match -> strip a
/// parenthetical then exact match -> substring containment -> fuzzy match -> `defaultValue`.
///
/// The fuzzy step diverges from Python's `difflib.get_close_matches` (ratio = 2*M/T over matching
/// blocks, cutoff 0.6): here it is a normalised Levenshtein ratio, `(lenA + lenB - distance) /
/// (lenA + lenB)`, against the same 0.6 cutoff. This is a different metric, not a re-implementation
/// of difflib's — close enough for a prototype, but expect occasional disagreement on borderline
/// inputs versus the Python baseline.
[[nodiscard]] std::string
normalizeAnswer(std::string_view answer, const std::optional<std::vector<std::string>>& outputValues, std::string_view defaultValue);

}

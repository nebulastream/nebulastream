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

/// Response decoding shared by every semantic operator's codec: they all ask for the same
/// `{"<row_id>": {"<output_field>": {"answer": ..., "confidence": ...}}}` envelope.

/// Port of the Python reference's `_parse_llm_json`. Tries, in order: the raw text, the contents of
/// the first markdown code fence, the span from the first `{` to the last `}`, and the text after
/// repairing two malformed id -> object shapes some local models produce. Returns an empty object
/// if all four fail, which the caller turns into `default_value` for every row.
[[nodiscard]] nlohmann::json parseLlmJson(std::string_view response);

/// Port of `_normalize_answer` without its fuzzy rung. With no declared `outputValues` the answer is
/// free text and returned unchanged. Otherwise the first matching rung wins: case-insensitive exact
/// match, exact match after stripping parentheticals, substring containment in declaration order,
/// and finally `defaultValue`.
///
/// The reference's last matching rung, `difflib.get_close_matches(cutoff=0.6)`, is deliberately not
/// ported: it maps near-misses such as a misspelt label onto a declared value non-reproducibly
/// across implementations, and nothing downstream can tell a fuzzy hit from a real one.
[[nodiscard]] std::string
normalizeAnswer(std::string_view answer, const std::vector<std::string>& outputValues, std::string_view defaultValue);

/// The member named `key`, preferring an exact match and falling back to a case-insensitive one;
/// null if `object` is not an object or has no such member. Output field names reach the prompt in
/// their canonical, upper-case form, and real models regularly answer with the key lower-cased; an
/// exact-only lookup would default-fill every row.
[[nodiscard]] const nlohmann::json* findMember(const nlohmann::json& object, std::string_view key);

/// Whether a filter verdict lets the row pass: JSON `true`, a non-zero number, or the string "true"
/// or "yes" in any case and surrounded by any whitespace. Everything else — `false`, null, any other
/// string, an array or object — drops the row.
///
/// Follows the reference's `if passed:` except for one deliberate deviation: Python's truthiness
/// passes every non-empty string, so a model answering "false" as a string would keep the row.
[[nodiscard]] bool isAffirmative(const nlohmann::json& verdict);

}

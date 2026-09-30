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

#include <SemanticMapCodec.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <ResponseParsing.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

/// Decodes one UTF-8 code point starting at `pos` and advances `pos`. Invalid sequences decode to
/// U+FFFD one byte at a time, so arbitrary VARSIZED bytes can never derail the prompt.
char32_t decodeUtf8(const std::string_view text, size_t& pos)
{
    constexpr char32_t Replacement = 0xFFFD;
    const auto lead = static_cast<uint8_t>(text[pos]);
    size_t length = 0;
    char32_t codePoint = 0;
    if (lead < 0x80)
    {
        ++pos;
        return lead;
    }
    if ((lead & 0xE0U) == 0xC0U)
    {
        length = 2;
        codePoint = lead & 0x1FU;
    }
    else if ((lead & 0xF0U) == 0xE0U)
    {
        length = 3;
        codePoint = lead & 0x0FU;
    }
    else if ((lead & 0xF8U) == 0xF0U)
    {
        length = 4;
        codePoint = lead & 0x07U;
    }
    else
    {
        ++pos;
        return Replacement;
    }
    if (pos + length > text.size())
    {
        ++pos;
        return Replacement;
    }
    for (size_t i = 1; i < length; ++i)
    {
        const auto continuation = static_cast<uint8_t>(text[pos + i]);
        if ((continuation & 0xC0U) != 0x80U)
        {
            ++pos;
            return Replacement;
        }
        codePoint = (codePoint << 6U) | (continuation & 0x3FU);
    }
    static constexpr std::array<char32_t, 5> MinimumForLength{0, 0, 0x80, 0x800, 0x10000};
    if (codePoint < MinimumForLength.at(length) || codePoint > 0x10FFFF || (codePoint >= 0xD800 && codePoint <= 0xDFFF))
    {
        ++pos;
        return Replacement;
    }
    pos += length;
    return codePoint;
}

/// A JSON string literal exactly as Python's `json.dumps` writes it with its defaults
/// (`ensure_ascii=True`): everything outside printable ASCII becomes a `\uXXXX` escape. The prompt
/// is the measured baseline's input, so its bytes follow the reference rather than nlohmann's
/// UTF-8-preserving `dump()`.
void appendPythonJsonString(std::string& out, const std::string_view text)
{
    out.push_back('"');
    for (size_t pos = 0; pos < text.size();)
    {
        const auto codePoint = decodeUtf8(text, pos);
        switch (codePoint)
        {
            case '"':
                out += R"(\")";
                break;
            case '\\':
                out += R"(\\)";
                break;
            case '\n':
                out += R"(\n)";
                break;
            case '\r':
                out += R"(\r)";
                break;
            case '\t':
                out += R"(\t)";
                break;
            case '\b':
                out += R"(\b)";
                break;
            case '\f':
                out += R"(\f)";
                break;
            default:
                if (codePoint >= 0x20 && codePoint < 0x7F)
                {
                    out.push_back(static_cast<char>(codePoint));
                }
                else if (codePoint <= 0xFFFF)
                {
                    out += fmt::format("\\u{:04x}", static_cast<uint32_t>(codePoint));
                }
                else
                {
                    const auto shifted = static_cast<uint32_t>(codePoint) - 0x10000U;
                    out += fmt::format("\\u{:04x}\\u{:04x}", 0xD800U + (shifted >> 10U), 0xDC00U + (shifted & 0x3FFU));
                }
        }
    }
    out.push_back('"');
}

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

bool equalsIgnoreCase(const std::string_view lhs, const std::string_view rhs)
{
    return std::ranges::equal(
        lhs, rhs, [](const unsigned char left, const unsigned char right) { return std::tolower(left) == std::tolower(right); });
}

/// The member named `key`, preferring an exact match and falling back to a case-insensitive one.
/// Output field names reach the prompt in their canonical, upper-case form, and real models
/// regularly answer with the key lower-cased; an exact-only lookup would default-fill every row.
const nlohmann::json* findMember(const nlohmann::json& object, const std::string_view key)
{
    if (!object.is_object())
    {
        return nullptr;
    }
    if (const auto exact = object.find(std::string(key)); exact != object.end())
    {
        return &*exact;
    }
    for (const auto& [memberKey, value] : object.items())
    {
        if (equalsIgnoreCase(memberKey, key))
        {
            return &value;
        }
    }
    return nullptr;
}

/// `str(raw)` of the reference for a non-string answer.
std::string pythonStr(const nlohmann::json& value)
{
    if (value.is_string())
    {
        return value.get<std::string>();
    }
    if (value.is_boolean())
    {
        return value.get<bool>() ? "True" : "False";
    }
    if (value.is_null())
    {
        return "None";
    }
    return value.dump();
}

}

SemanticMapCodec::SemanticMapCodec(SemanticModelConfig config) : config(std::move(config))
{
    PRECONDITION(!this->config.steps.empty(), "SemanticMapCodec needs at least one step");
}

std::string SemanticMapCodec::buildPrompt(const std::span<const RowPayload> rows) const
{
    std::string outputFields;
    std::string exampleRow;
    std::string operations;
    for (size_t i = 0; i < config.steps.size(); ++i)
    {
        const auto& step = config.steps[i];
        const auto* const separator = i == 0 ? "" : ", ";
        outputFields += fmt::format(R"({}"{}")", separator, step.outputColumn);
        exampleRow += fmt::format(R"({}"{}": {{"answer": "...", "confidence": 0.9}})", separator, step.outputColumn);
        operations += fmt::format("\n  {}. MAP: {} → output field: \"{}\"", i + 1, step.prompt, step.outputColumn);
    }

    std::string prompt = fmt::format(
        "You are a helpful AI assistant for semantic data operations.\n"
        "Always respond in JSON format.\n"
        "For each input row (_llm_call_id), return a JSON object where each key is an output field name "
        "and the value is a JSON object with 'answer' and 'confidence' (0-1).\n"
        "Output fields: {}\n"
        "Example output: {{\"row1\": {{{}}}}}\n"
        "Do not add explanations.\n"
        "Apply the following {} {} to each row:{}\n",
        outputFields,
        exampleRow,
        config.steps.size(),
        config.steps.size() == 1 ? "operation" : "operations",
        operations);

    if (!config.datasetPrompt.empty())
    {
        prompt += fmt::format("Dataset context: {}\n", config.datasetPrompt);
    }

    prompt += "Data: ";
    appendPythonJsonObject(
        prompt,
        rows,
        [this](std::string& out, const RowPayload& row)
        {
            appendPythonJsonString(out, row.rowId);
            out += ": ";
            switch (config.payloadFormat)
            {
                case PayloadFormat::SPACE_JOINED: {
                    /// The reference's `" ".join(str(row.get(c, "")) for c in depends_on)`: values
                    /// only, field names dropped. Lossy, but it is what the paper measured.
                    std::string joined;
                    for (size_t i = 0; i < row.fields.size(); ++i)
                    {
                        if (i > 0)
                        {
                            joined.push_back(' ');
                        }
                        joined += row.fields[i].second;
                    }
                    appendPythonJsonString(out, joined);
                    break;
                }
                case PayloadFormat::JSON_OBJECT:
                    appendPythonJsonObject(
                        out,
                        row.fields,
                        [](std::string& inner, const std::pair<std::string, std::string>& field)
                        {
                            appendPythonJsonString(inner, field.first);
                            inner += ": ";
                            appendPythonJsonString(inner, field.second);
                        });
                    break;
            }
        });
    return prompt;
}

std::vector<std::vector<std::string>> SemanticMapCodec::parse(const std::string_view response, const std::span<const RowPayload> rows) const
{
    const auto parsed = parseLlmJson(response);

    std::vector<std::vector<std::string>> answers;
    answers.reserve(rows.size());
    for (const auto& row : rows)
    {
        /// Row ids are generated by the codec itself, so they are matched exactly.
        const nlohmann::json* const rowData = parsed.is_object() && parsed.contains(row.rowId) ? &parsed.at(row.rowId) : nullptr;

        auto& rowAnswers = answers.emplace_back();
        rowAnswers.reserve(config.steps.size());
        for (const auto& step : config.steps)
        {
            /// The envelope's "confidence" is requested by the sysprompt (the prompt must stay
            /// identical to the measured baseline) but deliberately not read: SEM_MAP projects the
            /// answer only.
            std::optional<std::string> raw;
            if (rowData != nullptr)
            {
                if (const auto* const fieldData = findMember(*rowData, step.outputColumn); fieldData != nullptr && fieldData->is_object())
                {
                    if (const auto answer = fieldData->find("answer"); answer != fieldData->end())
                    {
                        raw = pythonStr(*answer);
                    }
                }
            }
            if (!raw.has_value())
            {
                NES_DEBUG(
                    "Semantic map response has no answer for row {} field {}, writing the default: {}",
                    row.rowId,
                    step.outputColumn,
                    response);
            }
            auto answer = normalizeAnswer(raw.value_or(step.defaultValue), step.outputValues, step.defaultValue);
            if (raw.has_value() && !step.outputValues.empty() && answer == step.defaultValue && *raw != step.defaultValue)
            {
                NES_DEBUG(
                    "Semantic map answer '{}' for row {} field {} is not a declared value, writing the default",
                    *raw,
                    row.rowId,
                    step.outputColumn);
            }
            rowAnswers.push_back(std::move(answer));
        }
    }
    return answers;
}

}

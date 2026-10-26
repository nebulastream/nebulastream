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

#include <SemanticPromptLayout.hpp>

#include <algorithm>
#include <array>
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
#include <ResponseParsing.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticRowPayload.hpp>

namespace NES::detail
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

}

/// Everything outside printable ASCII becomes a `\uXXXX` escape. The prompt is the measured
/// baseline's input, so its bytes follow the reference rather than nlohmann's UTF-8-preserving
/// `dump()`.
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

std::vector<std::string> stepResponseColumns(const std::vector<SemanticStep>& steps)
{
    std::vector<std::string> columns;
    columns.reserve(steps.size());
    size_t filters = 0;
    for (const auto& step : steps)
    {
        switch (step.kind)
        {
            case SemanticStep::Kind::MAP:
                columns.push_back(step.outputColumn);
                break;
            case SemanticStep::Kind::FILTER:
                columns.push_back(fmt::format("__filter_{}", filters++));
                break;
        }
    }
    return columns;
}

std::vector<std::string> responseColumns(const std::vector<SemanticStep>& steps)
{
    const bool filterOnly = std::ranges::all_of(steps, [](const SemanticStep& step) { return step.kind == SemanticStep::Kind::FILTER; });
    return filterOnly ? std::vector<std::string>{} : stepResponseColumns(steps);
}

std::string datasetContextBlock(const SemanticModelConfig& config)
{
    return config.datasetPrompt.empty() ? std::string{} : fmt::format("Dataset context: {}\n", config.datasetPrompt);
}

void appendDataBlock(std::string& prompt, const std::span<const RowPayload> rows, const PayloadFormat payloadFormat)
{
    prompt += "Data: ";
    appendPythonJsonObject(
        prompt,
        rows,
        [payloadFormat](std::string& out, const RowPayload& row)
        {
            appendPythonJsonString(out, row.rowId);
            out += ": ";
            switch (payloadFormat)
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
}

std::string buildFusedPrompt(const SemanticModelConfig& config, const std::span<const RowPayload> rows)
{
    const auto columns = stepResponseColumns(config.steps);
    std::string outputFields;
    std::string exampleRow;
    std::string operations;
    for (size_t i = 0; i < config.steps.size(); ++i)
    {
        const auto& step = config.steps[i];
        const auto& column = columns[i];
        const auto* const separator = i == 0 ? "" : ", ";
        const bool isFilter = step.kind == SemanticStep::Kind::FILTER;
        outputFields += fmt::format(R"({}"{}")", separator, column);
        exampleRow += fmt::format(R"({}"{}": {{"answer": {}, "confidence": 0.9}})", separator, column, isFilter ? "true" : R"("...")");
        operations += isFilter ? fmt::format("\n  {}. FILTER: {} → output field: \"{}\" (true/false)", i + 1, step.prompt, column)
                               : fmt::format("\n  {}. MAP: {} → output field: \"{}\"", i + 1, step.prompt, column);
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
    prompt += datasetContextBlock(config);
    appendDataBlock(prompt, rows, config.payloadFormat);
    return prompt;
}

std::string mapAnswer(
    const nlohmann::json* const rowData,
    const SemanticStep& step,
    const std::string_view column,
    const std::string_view rowId,
    const std::string_view response)
{
    /// The envelope's "confidence" is requested by the sysprompt (the prompt must stay identical to
    /// the measured baseline) but deliberately not read: a semantic operator projects the answer only.
    std::optional<std::string> raw;
    if (rowData != nullptr)
    {
        if (const auto* const fieldData = findMember(*rowData, column); fieldData != nullptr && fieldData->is_object())
        {
            if (const auto answer = fieldData->find("answer"); answer != fieldData->end())
            {
                raw = pythonStr(*answer);
            }
        }
    }
    if (!raw.has_value())
    {
        NES_DEBUG("Semantic response has no answer for row {} field {}, writing the default: {}", rowId, column, response);
    }
    auto answer = normalizeAnswer(raw.value_or(step.defaultValue), step.outputValues, step.defaultValue);
    if (raw.has_value() && !step.outputValues.empty() && answer == step.defaultValue && *raw != step.defaultValue)
    {
        NES_DEBUG("Semantic answer '{}' for row {} field {} is not a declared value, writing the default", *raw, rowId, column);
    }
    return answer;
}

}

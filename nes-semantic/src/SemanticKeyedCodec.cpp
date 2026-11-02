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

#include <SemanticKeyedCodec.hpp>

#include <cmath>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <ErrorHandling.hpp>
#include <ResponseParsing.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticPromptLayout.hpp>

namespace NES
{

namespace
{

/// `_SCAN_INIT_SYSPROMPT` and `_SCAN_FOLD_SYSPROMPT` of `operators/sem_scan.py`.
constexpr std::string_view ScanInitSysprompt
    = "You are a semantic scan assistant computing a running per-key accumulator.\n"
      "This is the FIRST tuple seen for this key -- there is no prior state, so establish the initial accumulator from this tuple "
      "alone.\n"
      "Always respond in JSON format.\n"
      "The response must have one key for the input row (_llm_call_id), and the value must be a JSON object with 'answer' (the "
      "computed accumulator/result, as a string -- encode structured content as a JSON string if needed) and 'confidence' (a float "
      "between 0 and 1).\n"
      "Example output: {\"row1\": {\"answer\": \"...\", \"confidence\": 0.9}}\n"
      "Do not add explanations.";

constexpr std::string_view ScanFoldSysprompt
    = "You are a semantic scan assistant computing a running per-key accumulator.\n"
      "A prior accumulator for this key is provided below -- fold the new tuple into it to produce the updated accumulator.\n"
      "Always respond in JSON format.\n"
      "The response must have one key for the input row (_llm_call_id), and the value must be a JSON object with 'answer' (the "
      "updated accumulator/result, as a string -- encode structured content as a JSON string if needed) and 'confidence' (a float "
      "between 0 and 1).\n"
      "Example output: {\"row1\": {\"answer\": \"...\", \"confidence\": 0.9}}\n"
      "Do not add explanations.";

/// `_WINDOW_SYSPROMPT` of `operators/sem_window.py`.
constexpr std::string_view WindowSysprompt
    = "You are a semantic stream windowing assistant.\n"
      "Given a new tuple and the contexts of several active semantic windows, score how well the tuple fits each window.\n"
      "Respond only in JSON keyed by window_id. Each value must have these three keys:\n"
      "  continuity_score: float 0-1 \xe2\x80\x94 how well this tuple belongs to that window (1 = perfect fit, 0 = completely "
      "unrelated).\n"
      "  window_description: updated concise description of that window's semantic theme assuming this tuple were added to it.\n"
      "  new_window_description: concise description of a brand-new window that would start with this tuple (used only if the "
      "tuple does not fit that window).\n"
      "Do not add explanations outside the JSON.";

constexpr std::string_view ScanRowId = "row1";
constexpr size_t RecentExampleLength = 500;
constexpr size_t NewWindowDescriptionLength = 200;

/// `float(entry.get("continuity_score", 0.0))`: a number, or a string holding one.
double scoreOf(const nlohmann::json& value)
{
    if (value.is_number())
    {
        return value.get<double>();
    }
    if (value.is_string())
    {
        try
        {
            return std::stod(value.get<std::string>());
        }
        catch (const std::exception&)
        {
            return 0.0;
        }
    }
    return 0.0;
}

}

namespace detail
{
std::string truncateCodePoints(const std::string_view text, const size_t count)
{
    size_t codePoints = 0;
    size_t position = 0;
    while (position < text.size())
    {
        /// A continuation byte never starts a code point.
        if ((static_cast<unsigned char>(text[position]) & 0xC0U) != 0x80U)
        {
            if (codePoints == count)
            {
                break;
            }
            ++codePoints;
        }
        ++position;
    }
    return std::string{text.substr(0, position)};
}

std::string pythonFloat(const double value)
{
    return nlohmann::json(value).dump();
}
}

SemanticScanCodec::SemanticScanCodec(SemanticModelConfig config) : config(std::move(config))
{
}

std::string SemanticScanCodec::buildPrompt(const std::string_view text, const std::optional<std::string>& prior) const
{
    /// `"\n".join(p for p in parts if p)`; the dataset block ends in its own newline, as in the reference.
    std::vector<std::string> parts;
    parts.emplace_back(prior ? ScanFoldSysprompt : ScanInitSysprompt);
    parts.push_back(fmt::format("Instruction: {}", prior ? config.steps.front().prompt : config.keyed->initPrompt));
    parts.push_back(detail::datasetContextBlock(config));
    if (prior)
    {
        std::string state = "Prior state for this key: ";
        detail::appendPythonJsonString(state, *prior);
        parts.push_back(std::move(state));
    }
    std::string data = "Data: {";
    detail::appendPythonJsonString(data, ScanRowId);
    data += ": ";
    detail::appendPythonJsonString(data, text);
    data += "}";
    parts.push_back(std::move(data));

    std::string prompt;
    for (const auto& part : parts)
    {
        if (part.empty())
        {
            continue;
        }
        if (!prompt.empty())
        {
            prompt.push_back('\n');
        }
        prompt += part;
    }
    return prompt;
}

std::optional<SemanticScanCodec::Answer> SemanticScanCodec::parse(const std::string_view response) const
{
    const auto data = parseLlmJson(response);
    if (!data.is_object())
    {
        return std::nullopt;
    }
    const nlohmann::json* entry = nullptr;
    if (data.contains(ScanRowId))
    {
        entry = &data[std::string{ScanRowId}];
    }
    else if (data.size() == 1)
    {
        entry = &data.begin().value();
    }
    /// An empty entry is falsy in the reference and counts as a failed extraction.
    if (entry == nullptr || entry->empty())
    {
        return std::nullopt;
    }
    const auto& defaultValue = config.steps.front().defaultValue;
    if (!entry->is_object())
    {
        return Answer{.state = defaultValue, .confidence = ""};
    }
    return Answer{
        .state = entry->contains("answer") ? detail::pythonStr((*entry)["answer"]) : defaultValue,
        .confidence = entry->contains("confidence") ? detail::pythonStr((*entry)["confidence"]) : ""};
}

SemanticWindowCodec::SemanticWindowCodec(SemanticModelConfig config) : config(std::move(config))
{
}

std::string SemanticWindowCodec::buildPrompt(const std::string_view text, const std::vector<const SemanticWindow*>& windows) const
{
    const auto& keyed = *config.keyed;
    std::string payload;
    detail::appendPythonJsonObject(
        payload,
        windows,
        [&keyed](std::string& out, const SemanticWindow* window)
        {
            detail::appendPythonJsonString(out, window->id);
            out += ": {\"description\": ";
            detail::appendPythonJsonString(out, window->description);
            if (keyed.strategy == "pairwise")
            {
                out += ", \"most_recent_tuple\": ";
                detail::appendPythonJsonString(out, window->lastRow);
            }
            else
            {
                out += ", \"recent_examples\": [";
                bool first = true;
                for (const auto& example : window->recentExamples)
                {
                    if (!first)
                    {
                        out += ", ";
                    }
                    first = false;
                    detail::appendPythonJsonString(out, example);
                }
                out += "]";
            }
            out += "}";
        });
    return fmt::format(
        "{}\n\nThreshold: {}\n{}\n\n{}Windows to score:\n{}\n\nNew tuple: {}",
        WindowSysprompt,
        detail::pythonFloat(keyed.threshold),
        config.steps.front().prompt,
        detail::datasetContextBlock(config),
        payload,
        text);
}

std::vector<SemanticWindowCodec::Score> SemanticWindowCodec::parse(
    const std::string_view response, const std::string_view text, const std::vector<const SemanticWindow*>& windows) const
{
    const auto data = parseLlmJson(response);
    std::vector<Score> scores;
    scores.reserve(windows.size());
    for (const auto* window : windows)
    {
        Score score{
            .continuity = 0.0,
            .windowDescription = window->description,
            .newWindowDescription = detail::truncateCodePoints(text, NewWindowDescriptionLength)};
        if (data.is_object() && data.contains(window->id) && data[window->id].is_object())
        {
            const auto& entry = data[window->id];
            if (entry.contains("continuity_score"))
            {
                score.continuity = scoreOf(entry["continuity_score"]);
            }
            if (entry.contains("window_description"))
            {
                score.windowDescription = detail::pythonStr(entry["window_description"]);
            }
            if (entry.contains("new_window_description"))
            {
                score.newWindowDescription = detail::pythonStr(entry["new_window_description"]);
            }
        }
        scores.push_back(std::move(score));
    }
    return scores;
}

SemanticWindow& SemanticWindowCodec::open(SemanticWindowState& state, const std::string_view start, std::string description) const
{
    const auto id = fmt::format("w{}", ++state.opened);
    const auto cap = config.keyed->maxActiveWindows;
    while (cap > 0 && state.active.size() >= cap)
    {
        state.active.erase(state.active.begin());
    }
    state.active.push_back(id);
    auto& window = state.windows[id];
    window = SemanticWindow{
        .id = id, .description = std::move(description), .start = std::string{start}, .rows = 0, .lastRow = {}, .recentExamples = {}};
    return window;
}

void SemanticWindowCodec::update(SemanticWindow& window, const std::string_view text, std::string description) const
{
    window.description = std::move(description);
    ++window.rows;
    window.lastRow = std::string{text};
    if (window.recentExamples.size() >= config.keyed->maxExamples && !window.recentExamples.empty())
    {
        window.recentExamples.pop_front();
    }
    window.recentExamples.push_back(detail::truncateCodePoints(text, RecentExampleLength));
}

std::vector<std::string> SemanticWindowCodec::assign(
    SemanticWindowState& state, const std::string_view text, const std::string_view start, const std::vector<Score>& scores) const
{
    SemanticWindow* assigned = nullptr;
    double continuity = 1.0;
    bool boundary = true;
    if (state.active.empty())
    {
        /// A key's first row opens its first window without a call.
        assigned = &open(state, start, "");
        update(*assigned, text, "");
    }
    else
    {
        INVARIANT(scores.size() == state.active.size(), "SEM_WINDOW needs one score per active window");
        /// The first of equal scores wins, as Python's max does.
        size_t best = 0;
        for (size_t index = 1; index < scores.size(); ++index)
        {
            if (scores[index].continuity > scores[best].continuity)
            {
                best = index;
            }
        }
        continuity = scores[best].continuity;
        if (continuity >= config.keyed->threshold)
        {
            assigned = &state.windows.at(state.active[best]);
            update(*assigned, text, scores[best].windowDescription);
            boundary = false;
        }
        else
        {
            assigned = &open(state, start, scores[best].newWindowDescription);
            update(*assigned, text, scores[best].newWindowDescription);
        }
    }
    return {assigned->id, assigned->description, assigned->start, detail::pythonFloat(continuity), boundary ? "true" : "false"};
}

std::shared_ptr<SemanticKeyedState> SemanticKeyedState::acquire(const std::string& stateId)
{
    static std::mutex registryMutex;
    static std::unordered_map<std::string, std::weak_ptr<SemanticKeyedState>> registry;
    const std::scoped_lock lock(registryMutex);
    if (auto existing = registry[stateId].lock())
    {
        return existing;
    }
    std::erase_if(registry, [](const auto& entry) { return entry.second.expired(); });
    auto state = std::make_shared<SemanticKeyedState>();
    registry[stateId] = state;
    return state;
}

}

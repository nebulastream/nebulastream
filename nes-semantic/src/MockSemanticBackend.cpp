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

#include <MockSemanticBackend.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include <ErrorHandling.hpp>
#include <SemanticBackend.hpp>

namespace NES
{

namespace
{

constexpr std::string_view LabelPrefix = "label:";

/// Separates the optional delay from the behaviour, as in 'echo@200'.
constexpr char DelaySeparator = '@';

struct Behaviour
{
    MockSemanticBackend::Mode mode;
    std::string label;
    /// How long one request takes. Zero unless the behaviour names a delay.
    std::chrono::milliseconds delay{0};
};

std::optional<Behaviour> parseBehaviour(std::string_view behaviour)
{
    /// A trailing '@<ms>' makes the mock take that long per request, which is what a model does
    /// and what nothing else in a hermetic test can stand in for: it is the only way to measure
    /// the asynchronous framework's throughput, or to show that a blocking call occupies a worker
    /// thread, without a model server. It composes with every behaviour below.
    std::chrono::milliseconds delay{0};
    if (const auto separator = behaviour.rfind(DelaySeparator); separator != std::string_view::npos)
    {
        const auto digits = behaviour.substr(separator + 1);
        int64_t milliseconds = 0;
        const auto* const end = digits.data() + digits.size();
        const auto parsed = std::from_chars(digits.data(), end, milliseconds);
        /// Only a suffix that is entirely digits is a delay. Anything else belongs to the
        /// behaviour itself — a label is free text and may well contain an '@'.
        if (!digits.empty() && parsed.ec == std::errc{} && parsed.ptr == end)
        {
            delay = std::chrono::milliseconds{milliseconds};
            behaviour = behaviour.substr(0, separator);
        }
    }

    const auto withDelay = [delay](Behaviour parsed)
    {
        parsed.delay = delay;
        return std::make_optional(parsed);
    };

    if (behaviour == "echo")
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::ECHO, .label = {}});
    }
    if (behaviour == "unparseable")
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::UNPARSEABLE, .label = {}});
    }
    if (behaviour == "fail")
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::FAIL, .label = {}});
    }
    if (behaviour.starts_with(LabelPrefix))
    {
        return withDelay(Behaviour{.mode = MockSemanticBackend::Mode::LABEL, .label = std::string(behaviour.substr(LabelPrefix.size()))});
    }
    return std::nullopt;
}

std::string toUpper(std::string text)
{
    std::ranges::transform(text, text.begin(), [](const unsigned char character) { return static_cast<char>(std::toupper(character)); });
    return text;
}

std::string toLower(std::string text)
{
    std::ranges::transform(text, text.begin(), [](const unsigned char character) { return static_cast<char>(std::tolower(character)); });
    return text;
}

/// The row payload is the prompt's last line. JSON escapes every newline inside it, so the last
/// "\nData: " is the real block even when a row's text contains that string. Ordered, so a
/// JSON_OBJECT row keeps its fields in declared order.
nlohmann::ordered_json dataBlock(const std::string& prompt)
{
    constexpr std::string_view Marker = "\nData: ";
    const auto position = prompt.rfind(Marker);
    if (position == std::string::npos)
    {
        return nlohmann::ordered_json::object();
    }
    auto parsed = nlohmann::ordered_json::parse(prompt.substr(position + Marker.size()), nullptr, false);
    return parsed.is_object() ? parsed : nlohmann::ordered_json::object();
}

/// The row's text: a SPACE_JOINED payload is already a string, a JSON_OBJECT payload has its
/// values joined the same way.
std::string rowText(const nlohmann::ordered_json& payload)
{
    if (payload.is_string())
    {
        return payload.get<std::string>();
    }
    std::string joined;
    for (const auto& value : payload)
    {
        if (!joined.empty())
        {
            joined.push_back(' ');
        }
        joined += value.is_string() ? value.get<std::string>() : value.dump();
    }
    return joined;
}

}

MockSemanticBackend::MockSemanticBackend(const std::string_view behaviour, std::vector<std::string> outputColumns)
    : mode(Mode::FAIL), outputColumns(std::move(outputColumns))
{
    auto parsed = parseBehaviour(behaviour);
    if (!parsed.has_value())
    {
        throw InvalidSemanticModel(
            "Unknown mock backend behaviour '{}' (expected echo, label:<X>, unparseable or fail, each optionally followed by @<ms>)",
            behaviour);
    }
    mode = parsed->mode;
    label = std::move(parsed->label);
    delay = parsed->delay;
}

bool MockSemanticBackend::isValidBehaviour(const std::string_view behaviour)
{
    return parseBehaviour(behaviour).has_value();
}

std::expected<std::string, BackendError> MockSemanticBackend::complete(const CompletionRequest& request)
{
    if (delay > std::chrono::milliseconds{0})
    {
        std::this_thread::sleep_for(delay);
    }

    switch (mode)
    {
        case Mode::FAIL:
            /// Fails on the first attempt regardless of maxRetries, so the system test stays fast.
            return std::unexpected{BackendError{.kind = BackendError::Kind::UNREACHABLE, .message = "mock endpoint is unreachable"}};
        case Mode::UNPARSEABLE:
            return "I am sorry, but I cannot classify these rows.";
        case Mode::ECHO:
        case Mode::LABEL: {
            auto response = nlohmann::json::object();
            for (const auto& [rowId, payload] : dataBlock(request.prompt).items())
            {
                const auto answer = mode == Mode::ECHO ? toUpper(rowText(payload)) : label;
                if (outputColumns.empty())
                {
                    /// A filter-only prompt asks for one verdict per row, without field names.
                    response[rowId] = {{"answer", answer}, {"confidence", 1.0}};
                    continue;
                }
                auto fields = nlohmann::json::object();
                for (const auto& column : outputColumns)
                {
                    fields[toLower(column)] = {{"answer", answer}, {"confidence", 1.0}};
                }
                response[rowId] = std::move(fields);
            }
            return response.dump();
        }
    }
    std::unreachable();
}

}

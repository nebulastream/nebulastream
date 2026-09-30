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
#include <expected>
#include <optional>
#include <string>
#include <string_view>
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

struct Behaviour
{
    MockSemanticBackend::Mode mode;
    std::string label;
};

std::optional<Behaviour> parseBehaviour(const std::string_view behaviour)
{
    if (behaviour == "echo")
    {
        return Behaviour{.mode = MockSemanticBackend::Mode::ECHO, .label = {}};
    }
    if (behaviour == "unparseable")
    {
        return Behaviour{.mode = MockSemanticBackend::Mode::UNPARSEABLE, .label = {}};
    }
    if (behaviour == "fail")
    {
        return Behaviour{.mode = MockSemanticBackend::Mode::FAIL, .label = {}};
    }
    if (behaviour.starts_with(LabelPrefix))
    {
        return Behaviour{.mode = MockSemanticBackend::Mode::LABEL, .label = std::string(behaviour.substr(LabelPrefix.size()))};
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
        throw InvalidSemanticModel("Unknown mock backend behaviour '{}' (expected echo, label:<X>, unparseable or fail)", behaviour);
    }
    mode = parsed->mode;
    label = std::move(parsed->label);
}

bool MockSemanticBackend::isValidBehaviour(const std::string_view behaviour)
{
    return parseBehaviour(behaviour).has_value();
}

std::expected<std::string, BackendError> MockSemanticBackend::complete(const CompletionRequest& request)
{
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

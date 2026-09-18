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

#include <LlmClientFactory.hpp>

#include <string_view>
#include <utility>
#include <vector>

#include <CurlLlmClient.hpp>
#include <MockLlmClient.hpp>
#include <ErrorHandling.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

namespace
{

constexpr std::string_view MOCK_SCHEME_PREFIX = "mock://";
constexpr std::string_view HTTP_SCHEME_PREFIX = "http://";
constexpr std::string_view HTTPS_SCHEME_PREFIX = "https://";

bool hasPrefix(const std::string_view text, const std::string_view prefix)
{
    return text.size() >= prefix.size() && text.substr(0, prefix.size()) == prefix;
}

}

std::unique_ptr<LlmClient>
createLlmClient(const SemanticModelConfig& config, const std::vector<std::string>& outputFieldNames)
{
    /// Prefix matching rather than URI parsing keeps this translation unit free of a URL library;
    /// the catalog already guaranteed the string is a well-formed URI at registration time
    /// (SemanticModelCatalog::registerModel), and URI::tryParse accepts any scheme.
    if (hasPrefix(config.baseUrl, MOCK_SCHEME_PREFIX))
    {
        NES_WARNING("Semantic model request will be answered by the hermetic mock client ({}) — nothing leaves the process", config.baseUrl);
        return std::make_unique<MockLlmClient>(config, outputFieldNames);
    }
    if (hasPrefix(config.baseUrl, HTTP_SCHEME_PREFIX) || hasPrefix(config.baseUrl, HTTPS_SCHEME_PREFIX))
    {
        return std::make_unique<CurlLlmClient>(config, outputFieldNames);
    }
    throw CannotLoadModel("Semantic model BASE_URL '{}' has no supported scheme (expected mock://, http:// or https://)", config.baseUrl);
}

}
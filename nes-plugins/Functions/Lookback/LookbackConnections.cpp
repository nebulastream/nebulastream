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

#include <LookbackConnections.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <mutex>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>
#include <yaml-cpp/exceptions.h>
/// Supplies the YAML::convert specializations that Node::as<T>() instantiates below.
#include <yaml-cpp/node/convert.h> /// NOLINT(misc-include-cleaner)
/// Definitions for the node_data lookups the templates above instantiate; the yaml-cpp umbrella
/// header pulls it in, the granular headers do not.
#include <yaml-cpp/node/detail/impl.h> /// NOLINT(misc-include-cleaner)
#include <yaml-cpp/node/impl.h>
#include <yaml-cpp/node/iterator.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <ErrorHandling.hpp>
#include <ODBCConnection.hpp>

namespace NES::Lookback
{

namespace
{

constexpr auto CONFIG_ENVIRONMENT_VARIABLE = "NES_LOOKBACK_CONFIG";

/// The keys a connection entry may carry. Anything else is a typo — reported rather than ignored,
/// because a silently dropped `password` key would surface as a confusing connect failure.
constexpr std::array<std::string_view, 13> KNOWN_KEYS{
    "db_host",
    "db_port",
    "database",
    "username",
    "password",
    "driver",
    "trust_server_certificate",
    "query_timeout_ms",
    "max_rows",
    "max_result_bytes",
    "strict",
    "on_failure",
    "rows"};

std::string requiredString(const YAML::Node& node, const std::string& key, const std::string& connectionName)
{
    if (!node[key])
    {
        throw CannotLoadConfig("Lookback connection '{}' is missing the required key '{}'", connectionName, key);
    }
    return LookbackConnectionRegistry::expandEnvironment(node[key].as<std::string>());
}

template <typename T>
T optionalScalar(const YAML::Node& node, const std::string& key, T fallback)
{
    if (!node[key])
    {
        return fallback;
    }
    return node[key].as<T>();
}

}

std::string LookbackConnectionRegistry::expandEnvironment(const std::string& value)
{
    std::string result;
    for (size_t i = 0; i < value.size(); ++i)
    {
        if (value[i] != '$' || i + 1 >= value.size() || value[i + 1] != '{')
        {
            result.push_back(value[i]);
            continue;
        }
        const auto closing = value.find('}', i + 2);
        if (closing == std::string::npos)
        {
            throw CannotLoadConfig("Lookback config value has an unmatched '${{' : '{}'", value);
        }
        const auto name = value.substr(i + 2, closing - i - 2);
        /// getenv is flagged as mt-unsafe against a concurrent setenv; nothing in NES mutates the
        /// environment after startup, and this runs at query submission.
        const auto* const fromEnvironment = std::getenv(name.c_str()); /// NOLINT(concurrency-mt-unsafe)
        if (fromEnvironment == nullptr)
        {
            throw CannotLoadConfig("Lookback config references environment variable '{}', which is not set", name);
        }
        result.append(fromEnvironment);
        i = closing;
    }
    return result;
}

std::unordered_map<std::string, LookbackConnection> LookbackConnectionRegistry::parse(const std::string& yamlText)
{
    YAML::Node document;
    try
    {
        document = YAML::Load(yamlText);
    }
    catch (const YAML::Exception& exception)
    {
        throw CannotLoadConfig("Lookback config is not valid YAML: {}", exception.what());
    }

    const auto connectionsNode = document["connections"];
    if (!connectionsNode || !connectionsNode.IsMap())
    {
        throw CannotLoadConfig("Lookback config must contain a 'connections' map");
    }

    std::unordered_map<std::string, LookbackConnection> parsed;
    for (const auto& entry : connectionsNode)
    {
        auto name = entry.first.as<std::string>();
        const auto& node = entry.second;
        if (!node.IsMap())
        {
            throw CannotLoadConfig("Lookback connection '{}' must be a map", name);
        }

        for (const auto& field : node)
        {
            const auto key = field.first.as<std::string>();
            if (std::ranges::find(KNOWN_KEYS, key) == KNOWN_KEYS.end())
            {
                throw CannotLoadConfig(
                    "Lookback connection '{}' has unknown key '{}'; known keys are {}", name, key, fmt::join(KNOWN_KEYS, ", "));
            }
        }

        LookbackConnection connection;
        connection.name = name;
        connection.limits.queryTimeoutMs = optionalScalar<uint32_t>(node, "query_timeout_ms", connection.limits.queryTimeoutMs);
        connection.limits.maxRows = optionalScalar<uint32_t>(node, "max_rows", connection.limits.maxRows);
        connection.limits.maxResultBytes = optionalScalar<uint32_t>(node, "max_result_bytes", connection.limits.maxResultBytes);
        connection.limits.strict = optionalScalar<bool>(node, "strict", connection.limits.strict);
        if (node["on_failure"])
        {
            connection.onFailure = node["on_failure"].as<std::string>();
            /// An empty marker is indistinguishable from "found nothing", which is the one thing it
            /// exists to avoid.
            if (connection.onFailure->empty())
            {
                throw CannotLoadConfig("Lookback connection '{}' sets an empty 'on_failure'; leave it out instead", name);
            }
            if (connection.limits.strict)
            {
                throw CannotLoadConfig(
                    "Lookback connection '{}' sets both 'strict' and 'on_failure'; a failure either fails the query or is marked", name);
            }
        }

        if (node["rows"])
        {
            if (node["db_host"])
            {
                throw CannotLoadConfig(
                    "Lookback connection '{}' sets both 'rows' and 'db_host'; a connection is either a fixture or a database", name);
            }
            std::vector<std::vector<std::string>> rows;
            for (const auto& row : node["rows"])
            {
                rows.emplace_back(row.as<std::vector<std::string>>());
            }
            connection.staticRows = std::move(rows);
        }
        else
        {
            connection.odbc = ODBC::ConnectionInformation{
                .server = requiredString(node, "db_host", name),
                .port = requiredString(node, "db_port", name),
                .database = requiredString(node, "database", name),
                .username = requiredString(node, "username", name),
                .password = requiredString(node, "password", name),
                .driver = requiredString(node, "driver", name),
                .trustServerCertificate = optionalScalar<bool>(node, "trust_server_certificate", false)};
        }

        parsed.emplace(std::move(name), std::move(connection));
    }
    return parsed;
}

void LookbackConnectionRegistry::loadFromYaml(const std::string& yamlText)
{
    auto parsed = parse(yamlText);
    const std::scoped_lock lock(mutex);
    connections = std::move(parsed);
    loaded = true;
}

void LookbackConnectionRegistry::ensureLoaded()
{
    if (loaded)
    {
        return;
    }
    loaded = true;

    const auto* const path = std::getenv(CONFIG_ENVIRONMENT_VARIABLE); /// NOLINT(concurrency-mt-unsafe)
    if (path == nullptr)
    {
        NES_DEBUG("{} is not set; no lookback connections are configured", CONFIG_ENVIRONMENT_VARIABLE);
        return;
    }

    std::ifstream file{path};
    if (!file)
    {
        throw CannotLoadConfig("Cannot read lookback config '{}' named by {}", path, CONFIG_ENVIRONMENT_VARIABLE);
    }
    const std::string contents{std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};

    connections = parse(contents);
    NES_INFO("Loaded {} lookback connection(s) from {}", connections.size(), path);
}

bool LookbackConnectionRegistry::contains(const std::string& name)
{
    const std::scoped_lock lock(mutex);
    ensureLoaded();
    return connections.contains(name);
}

LookbackConnection LookbackConnectionRegistry::get(const std::string& name)
{
    const std::scoped_lock lock(mutex);
    ensureLoaded();
    const auto found = connections.find(name);
    if (found == connections.end())
    {
        auto configured = connections | std::views::keys | std::ranges::to<std::vector>();
        throw UnknownLookbackConnection(
            "Lookback connection '{}' is not configured. Configured connections: {}. Connections are read from the YAML file named by {}.",
            name,
            configured,
            CONFIG_ENVIRONMENT_VARIABLE);
    }
    return found->second;
}

LookbackConnectionRegistry& LookbackConnectionRegistry::instance()
{
    static LookbackConnectionRegistry registry;
    return registry;
}

}

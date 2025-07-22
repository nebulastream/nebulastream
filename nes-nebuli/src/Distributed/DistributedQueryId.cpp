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
#include <Distributed/DistributedQueryId.hpp>

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

#include <yaml-cpp/yaml.h>

namespace YAML
{
template <>
struct convert<NES::Distributed::QueryId::ConnectionQueryIdPair>
{
    static bool decode(const YAML::Node& node, NES::Distributed::QueryId::ConnectionQueryIdPair& rhs)
    {
        rhs.connection = node["connection"].as<std::string>();
        rhs.id = node["id"].as<size_t>();
        return true;
    }
    static Node encode(const NES::Distributed::QueryId::ConnectionQueryIdPair& rhs)
    {
        YAML::Node node;
        node["id"] = rhs.id;
        node["connection"] = rhs.connection;
        return node;
    }
};

template <>
struct convert<NES::Distributed::QueryId>
{
    static bool decode(const YAML::Node& node, NES::Distributed::QueryId& rhs)
    {
        rhs.queries = node["queries"].as<std::vector<NES::Distributed::QueryId::ConnectionQueryIdPair>>();
        return true;
    }
    static Node encode(const NES::Distributed::QueryId& rhs)
    {
        YAML::Node node;
        node["queries"] = rhs.queries;
        return node;
    }
};
}

namespace NES::Distributed
{
std::string QueryId::save(const QueryId& queryId)
{
    const std::string name = std::tmpnam(nullptr);
    if (std::ofstream out(name); out.is_open())
    {
        const YAML::Node node = YAML::convert<QueryId>::encode(queryId);
        out << node;
    }

    auto withoutPrefix = std::string_view(name);
    withoutPrefix.remove_prefix(5);
    return std::string{withoutPrefix};
}

QueryId QueryId::load(const std::string& identifier)
{
    return YAML::LoadFile(std::filesystem::path("/tmp") / identifier).as<QueryId>();
}
}

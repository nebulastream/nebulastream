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

#include <FaultConfigParser.hpp>

#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <assert.h>
#include <boost/asio/config.hpp>
#include <yaml-cpp/yaml.h>
#include <FaultSimulator.hpp>

namespace NES
{

namespace
{

FaultAction parseAction(const YAML::Node& node)
{
    auto action = node.as<std::string>();
    if (action == "CRASH")
    {
        return FaultAction::CRASH;
    }
    if (action == "DISCONNECT")
    {
        return FaultAction::DISCONNECT;
    }
    if (action == "UDF")
    {
        return FaultAction::UDF;
    }
    throw std::runtime_error("unknown action: " + action);
}

std::unique_ptr<FaultTrigger> parseTrigger(const YAML::Node& node)
{
    auto type = node["type"].as<std::string>();

    if (type == "ALWAYS")
    {
        return std::make_unique<AlwaysFaultTrigger>();
    }

    if (type == "COUNT")
    {
        auto count = node["count"].as<uint64_t>();
        return std::make_unique<CountBasedFaultTrigger>(count);
    }

    if (type == "TIME")
    {
        auto after = std::chrono::milliseconds(node["after_ms"].as<uint64_t>());
        return std::make_unique<TimeBasedFaultTrigger>(std::chrono::system_clock::now(), after);
    }

    if (type == "TIMEFRAME")
    {
        auto start = std::chrono::milliseconds(node["start_ms"].as<uint64_t>());
        auto end = std::chrono::milliseconds(node["end_ms"].as<uint64_t>());
        return std::make_unique<TimeFrameFaultTrigger>(std::chrono::system_clock::now(), start, end);
    }
    throw std::runtime_error("unknown trigger: " + type);
}

std::vector<std::string> parseSystests(const YAML::Node& node)
{
    std::vector<std::string> result;
    for (const auto& test : node)
    {
        result.emplace_back(test.as<std::string>());
    }
    return result;
}

// enable paths relative to the fault testing config file
std::string resolveRelativePath(const std::filesystem::path& baseDir, const std::string& pathStr)
{
    std::filesystem::path p{pathStr};
    if (p.is_absolute())
    {
        return pathStr;
    }
    return (baseDir / p).lexically_normal().string();
}

FaultTestCase parseTestcase(const YAML::Node& node, const std::filesystem::path& baseDir)
{
    auto mode = node["mode"].as<std::string>();
    assert(mode == "SYSTESTS");
    auto queries = node["queries"];
    auto systests = parseSystests(queries);

    std::stringstream ss;
    ss << node;
    auto globalConfig = ss.str();

    FaultTestCase ftc;
    ftc.globalConfig = globalConfig;
    for (const auto& systestFile : systests)
    {
        ftc.systestFiles.push_back(resolveRelativePath(baseDir, systestFile));
    }
    if (const auto& topology = node["topology"])
    {
        ftc.topologyConfig = resolveRelativePath(baseDir, topology.as<std::string>());
    }
    return ftc;
}

std::vector<FaultTestCase> parseFromRoot(YAML::Node& root, const std::filesystem::path& baseDir)
{
    auto testcases = root["testcases"];

    std::vector<FaultTestCase> result;
    for (const auto& testcase : testcases)
    {
        auto parsed = parseTestcase(testcase, baseDir);
        result.push_back(parsed);
    }
    return result;
}

}

std::vector<FaultTestCase> parseFaultConfigFile(std::string& path)
{
    auto root = YAML::LoadFile(path);
    auto baseDir = std::filesystem::absolute(std::filesystem::path{path}).parent_path();
    return parseFromRoot(root, baseDir);
}

void parseConfigAndInitFaultContext(std::string globalConfig, FaultInjectionContext& context)
{
    auto config = YAML::Load(globalConfig);

    for (const auto& fp : config["failpoints"])
    {
        if (Host{fp["host"].as<std::string>()} != context.host && fp["host"].as<std::string>() != "global")
        {
            continue;
        }
        auto name = fp["name"].as<std::string>();
        auto action = parseAction(fp["action"]);
        auto trigger = parseTrigger(fp["trigger"]);
        context.rules[name].emplace_back(FaultRule{std::move(trigger), action});
    }
}

}

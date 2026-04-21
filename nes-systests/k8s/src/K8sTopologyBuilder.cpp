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

#include "K8sTopologyBuilder.hpp"
#include <fstream>
#include <sstream>
#include <regex>
#include <nlohmann/json.hpp>
#include <Util/Strings.hpp>
#include "SystestState.hpp"
#include "NameUtils.hpp"
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>

namespace NES::Systest
{

namespace {
/// Read entire file into a string. If the file can't be read, return an empty string.
std::string readFileContents(const std::filesystem::path& path)
{
    std::ifstream ifs(path);
    if (!ifs.good()) {
        return {};
    }
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
}

/// Extract the sink name from a query definition string and return it.
std::string extractSinkName(const std::string& queryDefinition)
{
    std::regex intoPattern(R"(\bINTO\s+(\w+))", std::regex::icase);
    std::smatch match;
    if (std::regex_search(queryDefinition, match, intoPattern) && match.size() > 1) {
        return match[1].str();
    }
    return "unknown-sink";
}

/// Extract the sink type from the query plan.
/// Returns the sink type (e.g., "File", "Print", "Checksum") if available, otherwise returns empty string.
/// TODO: also return other configs?
std::string extractSinkTypeFromPlan(const SystestQuery& q)
{
    if (!q.planInfoOrException) {
        return {};
    }

    try {
        const auto& plan = q.planInfoOrException->queryPlan.getGlobalPlan();
        auto sinkOps = NES::getOperatorByType<NES::SinkLogicalOperator>(plan);

        if (!sinkOps.empty()) {
            // Get the first sink operator
            auto sinkDescriptor = sinkOps[0]->getSinkDescriptor();
            if (sinkDescriptor) {
                return sinkDescriptor->getSinkType();
            }
        }
    } catch (...) {
        // If anything goes wrong, fall back to empty string
    }

    return {};
}
}

std::size_t K8sTopologyBuilder::counter = 0;

TopologyBuildResult K8sTopologyBuilder::buildWithSourceData(const SystestQuery& q)
{
    counter++;
    uint64_t queryId = counter;
    std::string topologyName = makeTopologyName(queryId);

    if (q.configurationOverride.overrideParameters.empty()) {
        std::cerr << "No config overrides present\n";
    } else {
        for (const auto& [key, value] : q.configurationOverride.overrideParameters) {
            std::cerr << "Config override: " << key << " = " << value << std::endl;
        }
    }

    nlohmann::json root;
    root["apiVersion"] = "nebulastream.com/v1";
    root["kind"] = "NesTopology";
    root["metadata"]["name"] = topologyName;

    nlohmann::json spec;

    /// Sinks
    nlohmann::json sinks = nlohmann::json::array();
    nlohmann::json sink;
    sink["name"] = extractSinkName(q.queryDefinition);
    sink["host"] = "worker-1";

    /// Determine sink type from the query plan
    std::string sinkType = extractSinkTypeFromPlan(q);
    std::cerr << "Sink type: " << sinkType << std::endl;
    sink["type"] = sinkType;

    // Configure sink-specific settings
    sink["config"]["file_path"] = "/sink-output/sink-output.csv";
    sink["config"]["output_format"] = "CSV";

    if (q.planInfoOrException)
    {
        nlohmann::json schemaNode = nlohmann::json::array();
        for (const auto& fld : q.planInfoOrException->sinkOutputSchema)
        {
            nlohmann::json f;
            f["name"] = fld.name;
            f["type"] = magic_enum::enum_name(fld.dataType.type);
            f["nullable"] = fld.dataType.nullable;
            schemaNode.push_back(f);
        }
        sink["schema"] = schemaNode;
    }
    sink["parser_config"] = nlohmann::json::object();
    sinks.push_back(sink);
    spec["sinks"] = sinks;

    /// Logical sources
    nlohmann::json logicalSources = nlohmann::json::array();
    if (q.planInfoOrException)
    {
        std::unordered_set<std::string> seenLogicalNames;
        for (const auto& pair : q.planInfoOrException->sourcesToFilePathsAndCounts)
        {
            const SourceDescriptor& desc = pair.first;
            const std::string logicalName = desc.getLogicalSource().getLogicalSourceName();

            if (!seenLogicalNames.insert(logicalName).second) {
                continue;
            }

            nlohmann::json ls;
            ls["name"] = logicalName;

            nlohmann::json schemaNode = nlohmann::json::array();
            for (const auto& fld : desc.getLogicalSource().getSchema()->getFields())
            {
                nlohmann::json f;
                f["name"] = fld.getUnqualifiedName();
                f["type"] = magic_enum::enum_name(fld.dataType.type);
                f["nullable"] = fld.dataType.nullable;
                schemaNode.push_back(f);
            }
            ls["schema"] = schemaNode;
            logicalSources.push_back(ls);
        }
    } else
    {
        std::cerr << "Query '" << q.testName << "' has no plan info available, skipping logical source generation.\n";
    }

    if (logicalSources.size() > 0)
    {
        spec["logical"] = logicalSources;
    }

    /// Physical sources
    nlohmann::json physicalSources = nlohmann::json::array();
    std::unordered_map<std::string, std::string> sourceFileData;
    if (q.planInfoOrException)
    {
        std::cerr << "Query '" << q.testName << "' has " << q.planInfoOrException->sourcesToFilePathsAndCounts.size() << " physical sources.\n";
        for (const auto& pair : q.planInfoOrException->sourcesToFilePathsAndCounts)
        {
            const SourceDescriptor& desc = pair.first;
            const SourceInputFile& sif = pair.second.first;
            nlohmann::json ps;
            ps["logical"] = desc.getLogicalSource().getLogicalSourceName();
            ps["host"] = "worker-1";
            ps["parser_config"]["type"] = desc.getParserConfig().parserType;
            ps["parser_config"]["field_delimiter"] = desc.getParserConfig().fieldDelimiter;
            ps["type"] = desc.getSourceType();

            /// Pod-local path where the operator should mount the source data PVC (PersistentVolumeClaim)
            std::string filename = sif.getRawValue().filename().string();
            ps["source_config"]["file_path"] = "/data/" + filename;
            std::string logicalName = desc.getLogicalSource().getLogicalSourceName();
            std::cerr << "Configuring source: " << logicalName << " -> file_path=/data/" << filename << std::endl;

            /// Systest will read these into the operator-managed PVC after the topology is reconciled
            std::string contents = readFileContents(sif.getRawValue());
            std::cerr << "Source file " << filename << " has " << contents.size() << " bytes" << std::endl;
            if (!contents.empty()) {
                sourceFileData[filename] = std::move(contents);
            } else {
                std::cerr << "WARNING: Source file " << filename << " is empty!" << std::endl;
            }

            physicalSources.push_back(ps);
        }
    }
    if (!physicalSources.empty())
    {
        spec["physical"] = physicalSources;
    }

    /// Worker nodes
    nlohmann::json workerNodes = nlohmann::json::array();
    nlohmann::json wn;
    wn["host"] = "worker-1";
    wn["image"] = "nebulastream/worker:distributed-poc";
    wn["max_operators"] = 100;

    for (const auto& [key, value] : q.configurationOverride.overrideParameters) {
        wn["config"][key] = value;
    }

    workerNodes.push_back(wn);
    spec["workers"] = workerNodes;

    root["spec"] = spec;

    std::string pvcName = "source-data-pvc";

    return TopologyBuildResult{
        .topologyJson = std::move(root),
        .sourceFileData = std::move(sourceFileData),
        .topologyName = std::move(topologyName),
        .sourceDataPVCName = std::move(pvcName),
    };
}

nlohmann::json K8sTopologyBuilder::build(const SystestQuery& q)
{
    return buildWithSourceData(q).topologyJson;
}

std::string K8sTopologyBuilder::toJsonString(const SystestQuery& q)
{
    nlohmann::json node = build(q);
    return node.dump();
}

}

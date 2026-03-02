#include "K8sTopologyBuilder.hpp"
#include <fstream>
#include <sstream>
#include <regex>
#include <nlohmann/json.hpp>
#include <Util/Strings.hpp>
#include "SystestState.hpp"
#include "NameUtils.hpp"

namespace NES::Systest
{

namespace {
/// Read entire file into a string. Returns empty string on failure.
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

/// Extract the sink name from a query definition string.
/// Looks for "INTO <sink_name>" (case-insensitive) and returns the sink name.
std::string extractSinkName(const std::string& queryDefinition)
{
    std::regex intoPattern(R"(\bINTO\s+(\w+))", std::regex::icase);
    std::smatch match;
    if (std::regex_search(queryDefinition, match, intoPattern) && match.size() > 1) {
        return match[1].str();
    }
    return "unknown-sink";
}
} // anonymous namespace

std::size_t K8sTopologyBuilder::counter = 0;

TopologyBuildResult K8sTopologyBuilder::buildWithSourceData(const SystestQuery& q)
{
    counter++;
    uint64_t queryId = counter;
    std::string topologyName = makeTopologyName(queryId);

    nlohmann::json root;
    root["apiVersion"] = "nebulastream.com/v1";
    root["kind"] = "NesTopology";
    root["metadata"]["name"] = topologyName;

    nlohmann::json spec;

    // sinks
    nlohmann::json sinks = nlohmann::json::array();
    nlohmann::json sink;
    sink["name"] = extractSinkName(q.queryDefinition);
    sink["type"] = "Print";
    sink["config"]["inputFormat"] = "CSV";
    if (q.planInfoOrException)
    {
        nlohmann::json schemaNode = nlohmann::json::array();
        for (const auto& fld : q.planInfoOrException->sinkOutputSchema)
        {
            nlohmann::json f;
            f["name"] = toLowerCase(fld.name);
            f["dataType"] = magic_enum::enum_name(fld.dataType.type);
            schemaNode.push_back(f);
        }
        sink["schema"] = schemaNode;
    }
    sink["host"] = "worker-1";
    sinks.push_back(sink);
    spec["sinks"] = sinks;

    // logical sources
    nlohmann::json logicalSources = nlohmann::json::array();
    if (q.planInfoOrException)
    {
        for (const auto& pair : q.planInfoOrException->sourcesToFilePathsAndCounts)
        {
            const SourceDescriptor& desc = pair.first;
            nlohmann::json ls;
            ls["name"] = toLowerCase(desc.getLogicalSource().getLogicalSourceName());
            nlohmann::json schemaNode = nlohmann::json::array();
            for (const auto& fld : desc.getLogicalSource().getSchema()->getFields())
            {
                nlohmann::json f;
                f["name"] = toLowerCase(fld.getUnqualifiedName());
                f["dataType"] = magic_enum::enum_name(fld.dataType.type);
                schemaNode.push_back(f);
            }
            ls["schema"] = schemaNode;
            logicalSources.push_back(ls);
        }
    }
    if (logicalSources.size() > 0)
    {
        spec["logicalSources"] = logicalSources;
    }

    // physical sources — reference filenames; actual data goes via ConfigMap patch
    nlohmann::json physicalSources = nlohmann::json::array();
    std::unordered_map<std::string, std::string> sourceFileData;
    if (q.planInfoOrException)
    {
        for (const auto& pair : q.planInfoOrException->sourcesToFilePathsAndCounts)
        {
            const SourceDescriptor& desc = pair.first;
            const SourceInputFile& sif = pair.second.first;
            nlohmann::json ps;
            ps["logicalSource"] = toLowerCase(desc.getLogicalSource().getLogicalSourceName());
            ps["host"] = "worker-1";
            ps["parserConfig"]["type"] = "CSV";
            ps["type"] = "File";

            /// Pod-local path where the operator should mount the source data ConfigMap.
            std::string filename = sif.getRawValue().filename().string();
            ps["sourceConfig"]["filePath"] = "/data/" + filename;

            /// Read the file contents — the systest will patch these into the
            /// operator-managed ConfigMap after the topology is reconciled.
            std::string contents = readFileContents(sif.getRawValue());
            if (!contents.empty()) {
                sourceFileData[filename] = std::move(contents);
            }

            physicalSources.push_back(ps);
        }
    }
    if (!physicalSources.empty())
    {
        spec["physicalSources"] = physicalSources;
    }

    // worker nodes
    nlohmann::json workerNodes = nlohmann::json::array();
    nlohmann::json wn;
    wn["host"] = "worker-1";
    wn["image"] = "nebulastream/worker:v2distributed";
    wn["capacity"] = 100;
    wn["worker.defaultQueryExecution.executionMode"] = "INTERPRETER"; //TODO: this part needs to be configurable (extract worker config)
    workerNodes.push_back(wn);
    spec["workerNodes"] = workerNodes;

    root["spec"] = spec;

    std::string pvcName = "source-data-pvc";

    return TopologyBuildResult{
        .topologyJson = std::move(root),
        .sourceFileData = std::move(sourceFileData),
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
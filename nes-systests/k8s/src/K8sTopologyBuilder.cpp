#include "K8sTopologyBuilder.hpp"
#include <nlohmann/json.hpp>
#include "SystestState.hpp"
#include "NameUtils.hpp"

namespace NES::Systest
{
std::size_t K8sTopologyBuilder::counter = 0;

nlohmann::json K8sTopologyBuilder::build(const SystestQuery& q)
{
    counter++;
    uint64_t queryId = counter;

    nlohmann::json root;
    root["apiVersion"] = "nebulastream.com/v1";
    root["kind"] = "NesTopology";
    root["metadata"]["name"] = makeTopologyName(queryId);

    nlohmann::json spec;

    // sinks
    nlohmann::json sinks = nlohmann::json::array();
    nlohmann::json sink;
    sink["name"] = makeSinkName(q.testName, queryId);
    sink["type"] = "Print";
    sink["config"]["inputFormat"] = "CSV";
    if (q.planInfoOrException)
    {
        nlohmann::json schemaNode = nlohmann::json::array();
        for (const auto& fld : q.planInfoOrException->sinkOutputSchema)
        {
            nlohmann::json f;
            f["name"] = fld.name;
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
            ls["name"] = desc.getLogicalSource().getLogicalSourceName();
            nlohmann::json schemaNode = nlohmann::json::array();
            for (const auto& fld : desc.getLogicalSource().getSchema()->getFields())
            {
                nlohmann::json f;
                f["name"] = fld.name;
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

    // physical sources
    nlohmann::json physicalSources = nlohmann::json::array();
    if (q.planInfoOrException)
    {
        for (const auto& pair : q.planInfoOrException->sourcesToFilePathsAndCounts)
        {
            const SourceDescriptor& desc = pair.first;
            const SourceInputFile& sif = pair.second.first;
            nlohmann::json ps;
            ps["logicalSource"] = desc.getLogicalSource().getLogicalSourceName();
            ps["host"] = "worker-1";
            ps["parserConfig"]["type"] = "CSV";
            ps["type"] = "File";
            ps["sourceConfig"]["filePath"] = sif.getRawValue().string();
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
    return root;
}

std::string K8sTopologyBuilder::toJsonString(const SystestQuery& q)
{
    nlohmann::json node = build(q);
    return node.dump();
}

}
#include "K8sTopologyBuilder.hpp"
#include <yaml-cpp/yaml.h>
#include "SystestState.hpp"
#include  "NameUtils.hpp"

namespace NES::Systest
{
std::size_t K8sTopologyBuilder::counter = 0;

YAML::Node K8sTopologyBuilder::build(const SystestQuery& q)
{
    counter++;
    uint64_t queryId = counter;

    YAML::Node root;
    root["apiVersion"] = "nebulastream.com/v1";
    root["kind"] = "NesTopology";
    root["metadata"]["name"] = makeTopologyName(queryId);

    YAML::Node spec;

    // sinks
    YAML::Node sinks = YAML::Node(YAML::NodeType::Sequence);
    YAML::Node sink;
    sink["name"] = makeSinkName(q.testName, queryId);
    sink["type"] = "Print";
    sink["config"]["inputFormat"] = "CSV";
    if (q.planInfoOrException)
    {
        YAML::Node schemaNode = YAML::Node(YAML::NodeType::Sequence);
        for (const auto& fld : q.planInfoOrException->sinkOutputSchema)
        {
            YAML::Node f;
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
    YAML::Node logicalSources = YAML::Node(YAML::NodeType::Sequence);
    if (q.planInfoOrException)
    {
        for (const auto& pair : q.planInfoOrException->sourcesToFilePathsAndCounts)
        {
            const SourceDescriptor& desc = pair.first;
            YAML::Node ls;
            ls["name"] = desc.getLogicalSource().getLogicalSourceName();
            YAML::Node schemaNode = YAML::Node(YAML::NodeType::Sequence);
            for (const auto& fld : desc.getLogicalSource().getSchema()->getFields())
            {
                YAML::Node f;
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
    YAML::Node physicalSources = YAML::Node(YAML::NodeType::Sequence);
    if (q.planInfoOrException)
    {
        for (const auto& pair : q.planInfoOrException->sourcesToFilePathsAndCounts)
        {
            const SourceDescriptor& desc = pair.first;
            const SourceInputFile& sif = pair.second.first;
            YAML::Node ps;
            ps["logicalSource"] = desc.getLogicalSource().getLogicalSourceName();
            ps["host"] = "worker-1";
            ps["parserConfig"]["type"] = "CSV";
            ps["type"] = "File";
            ps["sourceConfig"]["filePath"] = sif.getRawValue().string();
            physicalSources.push_back(ps);
        }
    }
    if (physicalSources.size() > 0)
    {
        spec["physicalSources"] = physicalSources;
    }

    // worker nodes
    YAML::Node workerNodes = YAML::Node(YAML::NodeType::Sequence);
    YAML::Node wn;
    wn["host"] = "worker-1";
    wn["image"] = "nebulastream/worker:v2distributed";
    wn["capacity"] = 100;
    wn["worker.defaultQueryExecution.executionMode"] = "INTERPRETER"; //TODO: this part needs to be configurable (extract worker config)
    workerNodes.push_back(wn);
    spec["workerNodes"] = workerNodes;

    root["spec"] = spec;
    return root;
}

std::string K8sTopologyBuilder::toYamlString(const SystestQuery& q)
{
    YAML::Node node = build(q);
    YAML::Emitter out;
    out << node;
    return std::string(out.c_str());
}
}

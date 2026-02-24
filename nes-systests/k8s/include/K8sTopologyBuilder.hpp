#pragma once
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include "SystestState.hpp"

namespace NES::Systest
{

struct SystestQuery;

/// Result of building a topology CR, bundling the JSON with any source-file data
/// that must be pushed into the operator-managed ConfigMap.
struct TopologyBuildResult {
    nlohmann::json topologyJson;
    /// Map of filename → file contents for source data files.
    /// The systest patches these into the ConfigMap "<topology-name>-source-data"
    /// which the operator creates when reconciling the topology.
    std::unordered_map<std::string, std::string> sourceFileData;
    /// The ConfigMap name the operator is expected to create for source data.
    std::string sourceDataConfigMapName;
};

class K8sTopologyBuilder {
public:
    static TopologyBuildResult buildWithSourceData(const SystestQuery& q);
    static nlohmann::json build(const SystestQuery& q);
    static std::string toJsonString(const SystestQuery& q);
    static std::size_t counter;
};
}
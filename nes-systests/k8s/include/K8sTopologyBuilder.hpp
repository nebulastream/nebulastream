#pragma once
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include "SystestState.hpp"

namespace NES::Systest
{

struct SystestQuery;

/// Result of building a topology CR, bundling the JSON with any source-file data
/// that must be written into a PVC for worker pods to consume.
struct TopologyBuildResult {
    nlohmann::json topologyJson;
    /// Map of filename → file contents for source data files.
    /// The systest writes these into a PVC via a short-lived writer pod.
    std::unordered_map<std::string, std::string> sourceFileData;
    /// The PVC name used for source data (mounted at /data in worker pods).
    std::string sourceDataPVCName;
};

class K8sTopologyBuilder {
public:
    static TopologyBuildResult buildWithSourceData(const SystestQuery& q);
    static nlohmann::json build(const SystestQuery& q);
    static std::string toJsonString(const SystestQuery& q);
    static std::size_t counter;
};
}
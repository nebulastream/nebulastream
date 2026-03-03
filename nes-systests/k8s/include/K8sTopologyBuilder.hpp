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

#pragma once
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include "SystestState.hpp"

namespace NES::Systest
{

struct SystestQuery;

/// Result of building a topology CR, bundling the JSON with any source-file data
struct TopologyBuildResult {
    nlohmann::json topologyJson;
    std::unordered_map<std::string, std::string> sourceFileData;
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
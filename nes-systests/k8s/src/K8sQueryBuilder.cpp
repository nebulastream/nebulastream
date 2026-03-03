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

#include "K8sQueryBuilder.hpp"
#include <regex>
#include <nlohmann/json.hpp>
#include "NameUtils.hpp"
#include "SystestState.hpp"

namespace NES::Systest
{

namespace
{
/// Rewrite local source file paths in the query definition to pod-local paths
std::string rewriteSourcePaths(const SystestQuery& q)
{
    std::string query = q.queryDefinition;
    if (q.planInfoOrException)
    {
        for (const auto& [desc, fileAndCount] : q.planInfoOrException->sourcesToFilePathsAndCounts)
        {
            const auto& resolvedPath = fileAndCount.first.getRawValue();
            std::string filename = resolvedPath.filename().string();
            std::string podPath = "/data/" + filename;

            /// Match any path (possibly with directory components) ending with the filename, e.g. 'small/stream8.csv' or just 'stream8.csv'
            std::string escapedFilename = std::regex_replace(filename, std::regex(R"([.])"), "\\.");
            std::regex pathPattern("(['\"])[^'\"]*" + escapedFilename + "\\1");

            query = std::regex_replace(query, pathPattern, "$1" + podPath + "$1");
        }
    }
    return query;
}
}

nlohmann::json K8sQueryBuilder::build(const SystestQuery& q, const std::string& topologyName)
{
    uint64_t queryId = q.queryIdInFile.getRawValue();
    nlohmann::json root;
    root["apiVersion"] = "nebulastream.com/v1";
    root["kind"] = "NesQuery";
    root["metadata"]["name"] = makeQueryName(queryId);

    nlohmann::json spec;
    spec["query"] = rewriteSourcePaths(q);
    spec["topologyName"] = topologyName;
    nlohmann::json nebuli;
    nebuli["image"] = "sidondocker/sido-nebuli";
    nebuli["args"] = "start";
    spec["nebuli"] = nebuli;
    root["spec"] = spec;
    return root;
}
}
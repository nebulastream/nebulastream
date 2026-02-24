#include "K8sQueryBuilder.hpp"
#include <nlohmann/json.hpp>
#include <regex>
#include "NameUtils.hpp"
#include "SystestState.hpp"

namespace NES::Systest
{

namespace {
/// Rewrite local source file paths in the query definition to pod-local paths.
/// The queryDefinition contains the original relative path as written in the test file
/// (e.g. 'small/stream8.csv'), while sourcesToFilePathsAndCounts contains the resolved
/// absolute path (e.g. /path/to/testdata/small/stream8.csv). We extract the filename
/// from the resolved path and search-replace any path ending with that filename in the query.
std::string rewriteSourcePaths(const SystestQuery& q)
{
    std::string query = q.queryDefinition;
    if (q.planInfoOrException) {
        for (const auto& [desc, fileAndCount] : q.planInfoOrException->sourcesToFilePathsAndCounts) {
            const auto& resolvedPath = fileAndCount.first.getRawValue();
            std::string filename = resolvedPath.filename().string();
            std::string podPath = "/data/" + filename;

            /// Match any path (possibly with directory components) ending with the filename,
            /// e.g. 'small/stream8.csv' or just 'stream8.csv'. The path appears inside quotes
            /// in the query, so we match everything between quotes that ends with the filename.
            std::string escapedFilename = std::regex_replace(filename, std::regex(R"([.])"), "\\.");
            std::regex pathPattern("(['\"])[^'\"]*" + escapedFilename + "\\1");

            query = std::regex_replace(query, pathPattern, "$1" + podPath + "$1");
        }
    }
    return query;
}
} // anonymous namespace

nlohmann::json K8sQueryBuilder::build(const SystestQuery& q)
{
    nlohmann::json root;
    root["apiVersion"] = "nebulastream.com/v1";
    root["kind"] = "NesQuery";
    root["metadata"]["name"] = makeQueryName(q.queryIdInFile.getRawValue());

    nlohmann::json spec;
    spec["query"] = rewriteSourcePaths(q);
    nlohmann::json nebuli;
    nebuli["image"] = "sidondocker/sido-nebuli";
    nebuli["args"] = "start";
    spec["nebuli"] = nebuli;
    root["spec"] = spec;
    return root;
}

std::string K8sQueryBuilder::toJsonString(const SystestQuery& q)
{
    nlohmann::json node = build(q);
    return node.dump();
}
}
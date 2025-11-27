#include "K8sQueryBuilder.hpp"
#include <nlohmann/json.hpp>
#include "NameUtils.hpp"
#include "SystestState.hpp"

namespace NES::Systest
{
nlohmann::json K8sQueryBuilder::build(const SystestQuery& q)
{
    nlohmann::json root;
    root["apiVersion"] = "nebulastream.com/v1";
    root["kind"] = "NesQuery";
    root["metadata"]["name"] = makeQueryName(q.queryIdInFile.getRawValue());

    nlohmann::json spec;
    spec["query"] = q.queryDefinition;
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
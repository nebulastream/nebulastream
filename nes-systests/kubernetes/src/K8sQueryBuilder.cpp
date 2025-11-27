#include "K8sQueryBuilder.hpp"
#include <yaml-cpp/yaml.h>
#include "SystestState.hpp"
#include  "NameUtils.hpp"

namespace NES::Systest
{
YAML::Node K8sQueryBuilder::build(const SystestQuery& q){
    YAML::Node root;
    root["apiVersion"]="nebulastream.com/v1";
    root["kind"]="NesQuery";
    root["metadata"]["name"]=makeQueryName(q.queryIdInFile.getRawValue());


    YAML::Node spec;
    spec["query"]=q.queryDefinition;
    YAML::Node nebuli;
    nebuli["image"]="sidondocker/sido-nebuli";
    nebuli["args"]="start";
    spec["nebuli"]=nebuli;
    root["spec"]=spec;
    return root;
}

std::string K8sQueryBuilder::toYamlString(const SystestQuery& q){
    YAML::Node node=build(q);
    YAML::Emitter out; out<<node;
    return std::string(out.c_str());
}
}


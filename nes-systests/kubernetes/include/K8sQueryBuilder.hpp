#pragma once
#include <string>
#include <yaml-cpp/yaml.h>

namespace NES::Systest
{

struct SystestQuery;

class K8sQueryBuilder
{
public:
    static YAML::Node build(const SystestQuery& q);
    static std::string toYamlString(const SystestQuery& q);
};
}

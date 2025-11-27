#pragma once
#include <yaml-cpp/yaml.h>
#include <string>
#include "SystestState.hpp"

namespace NES::Systest
{

struct SystestQuery;

class K8sTopologyBuilder {
public:
    static YAML::Node build(const SystestQuery& q);
    static std::string toYamlString(const SystestQuery& q);
    static std::size_t counter;
};
}

#pragma once
#include <nlohmann/json.hpp>
#include <string>
#include "SystestState.hpp"

namespace NES::Systest
{

struct SystestQuery;

class K8sTopologyBuilder {
public:
    static nlohmann::json build(const SystestQuery& q);
    static std::string toJsonString(const SystestQuery& q);
    static std::size_t counter;
};
}
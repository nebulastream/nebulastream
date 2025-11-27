#pragma once
#include <string>
#include <nlohmann/json.hpp>

namespace NES::Systest
{

struct SystestQuery;

class K8sQueryBuilder
{
public:
    static nlohmann::json build(const SystestQuery& q);
    static std::string toJsonString(const SystestQuery& q);
};
}
#include "Services/QueryService.hpp"
#include <Operators/OperatorJsonUtil.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
using namespace NES;

json::value QueryService::getQueryPlanForQueryId(std::string userQuery) {

    //build query from string
    QueryPtr query = getQueryFromQueryString(userQuery);
    //build the query plan
    const json::value& basePlan = OperatorJsonUtil::getBasePlan(query->getQueryPlan());
    return basePlan;
}

QueryPtr QueryService::getQueryFromQueryString(std::string userQuery) {
    return UtilityFunctions::createQueryFromCodeString(userQuery);
}

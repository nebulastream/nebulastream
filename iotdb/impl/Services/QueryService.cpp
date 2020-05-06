#include "Services/QueryService.hpp"
#include <Operators/OperatorJsonUtil.hpp>
#include <Util/UtilityFunctions.hpp>

using namespace NES;

json::value QueryService::generateBaseQueryPlanFromQueryString(std::string userQuery) {

    //build query from string
    InputQueryPtr inputQuery = getInputQueryFromQueryString(userQuery);

    //build the query plan
    OperatorJsonUtil queryPlanBuilder;
    const json::value& basePlan = queryPlanBuilder.getBasePlan(inputQuery);

    return basePlan;
}

InputQueryPtr QueryService::getInputQueryFromQueryString(std::string userQuery) {
    return UtilityFunctions::createQueryFromCodeString(userQuery);
}

#include <QueryCompiler/QueryPlanBuilder.hpp>
#include "Services/QueryService.hpp"

using namespace iotdb;

json::value QueryService::generateBaseQueryPlanFromQueryString(std::string userQuery) {

    //build query from string
    InputQueryPtr inputQuery = getInputQueryFromQueryString(userQuery);

    //build the query plan
    QueryPlanBuilder queryPlanBuilder;
    const json::value &basePlan = queryPlanBuilder.getBasePlan(inputQuery);

    return basePlan;
}

InputQueryPtr QueryService::getInputQueryFromQueryString(std::string userQuery) {
  return iotdb::createQueryFromCodeString(userQuery);
}
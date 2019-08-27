#include <API/InputQuery.hpp>
#include <CodeGen/QueryPlanBuilder.hpp>
#include "Services/QueryService.hpp"

using namespace iotdb;

json::value QueryService::generateBaseQueryPlanFromQueryString(std::string userQuery) {

    //build query from string
    InputQuery inputQuery(iotdb::createQueryFromCodeString(userQuery));

    //build the query plan
    QueryPlanBuilder queryPlanBuilder;
    const json::value &basePlan = queryPlanBuilder.getBasePlan(inputQuery);

    return basePlan;
}
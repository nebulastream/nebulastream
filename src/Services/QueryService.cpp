#include "Services/QueryService.hpp"
#include <Operators/OperatorJsonUtil.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
using namespace NES;

QueryService::QueryService(StreamCatalogPtr streamCatalog)
    : streamCatalog(streamCatalog) {
    NES_DEBUG("QueryService()");
}

json::value QueryService::generateBaseQueryPlanFromQueryString(std::string userQuery) {

    //build query from string
    QueryPtr query = getQueryFromQueryString(userQuery);

    //build the query plan
    OperatorJsonUtil queryPlanBuilder;
    const json::value& basePlan = queryPlanBuilder.getBasePlan(query);

    return basePlan;
}

QueryPtr QueryService::getQueryFromQueryString(std::string userQuery) {
    return UtilityFunctions::createQueryFromCodeString(userQuery, streamCatalog);
}

#include <Catalogs/QueryCatalogEntry.hpp>

namespace NES {

QueryCatalogEntry::QueryCatalogEntry(std::string queryId, std::string queryString, std::string queryPlacementStrategy, QueryPlanPtr queryPlanPtr, QueryStatus queryStatus)
    : queryId(queryId), queryString(queryString), queryPlacementStrategy(queryPlacementStrategy), queryPlanPtr(queryPlanPtr), queryStatus(queryStatus){}

std::string QueryCatalogEntry::getQueryId() {
    return queryId;
}

std::string QueryCatalogEntry::getQueryString() {
    return queryString;
}

const QueryPlanPtr QueryCatalogEntry::getQueryPlan() const {
    return queryPlanPtr;
}

QueryStatus QueryCatalogEntry::getQueryStatus() const {
    return queryStatus;
}

std::string QueryCatalogEntry::getQueryStatusAsString() const {
    return queryStatusToStringMap[queryStatus];
}

void QueryCatalogEntry::setQueryStatus(QueryStatus queryStatus) {
    this->queryStatus = queryStatus;
}


const std::string& QueryCatalogEntry::getQueryPlacementStrategy() const {
    return queryPlacementStrategy;
}

QueryCatalogEntry QueryCatalogEntry::copy() {
    return QueryCatalogEntry(queryId, queryString, queryPlacementStrategy, queryPlanPtr, queryStatus);
}

}

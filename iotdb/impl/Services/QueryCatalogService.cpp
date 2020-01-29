#include "Services/QueryCatalogService.hpp"
#include "Catalogs/QueryCatalog.hpp"

namespace NES {

std::map<string, string> QueryCatalogService::getQueriesWithStatus(std::string status) {

    boost::to_upper(status);

    if (StringToQueryStatus.find(status) == StringToQueryStatus.end()) {
        throw std::invalid_argument("Unknown query status " + status);
    }

    QueryStatus queryStatus = StringToQueryStatus[status];
    map<string, QueryCatalogEntryPtr> queries = QueryCatalog::instance().getQueries(queryStatus);

    map<string, string> result;
    for (auto[key, value] : queries) {
        result[key] = value->queryString;
    }
    return result;
}

std::map<std::string, std::string> QueryCatalogService::getAllRegisteredQueries() {

    map<string, QueryCatalogEntryPtr> registeredQueries = QueryCatalog::instance().getRegisteredQueries();

    map<string, string> result;
    for (auto[key, value] : registeredQueries) {
        result[key] = value->queryString;
    }
    return result;
}

}

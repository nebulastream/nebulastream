#include <Catalogs/QueryCatalog.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/Logger.hpp>
#include <boost/algorithm/string.hpp>

namespace NES {

std::map<string, string> QueryCatalogService::getQueriesWithStatus(std::string status) {

    NES_INFO("QueryCatalogService : fetching all queries with status " << status);

    boost::to_upper(status);

    if (StringToQueryStatus.find(status) == StringToQueryStatus.end()) {
        throw std::invalid_argument("Unknown query status " + status);
    }

    QueryStatus queryStatus = StringToQueryStatus[status];
    map<string, QueryCatalogEntryPtr> queries = QueryCatalog::instance().getQueries(queryStatus);

    map<string, string> result;
    for (auto [key, value] : queries) {
        result[key] = value->getQueryString();
    }

    NES_INFO("QueryCatalogService : found " << result.size() << " all queries with status " << status);
    return result;
}

std::map<std::string, std::string> QueryCatalogService::getAllRegisteredQueries() {

    NES_INFO("QueryCatalogService : get all registered queries");

    map<string, QueryCatalogEntryPtr> registeredQueries = QueryCatalog::instance().getRegisteredQueries();

    map<string, string> result;
    for (auto [key, value] : registeredQueries) {
        result[key] = value->getQueryString();
    }

    NES_INFO("QueryCatalogService : found " << result.size() << " registered queries");
    return result;
}

}// namespace NES

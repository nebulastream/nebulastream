#include "Services/QueryCatalogService.hpp"
#include "Catalogs/QueryCatalog.hpp"

namespace NES {

std::map<string, string> QueryCatalogService::getAllRunningQueries() {
    map<string, QueryCatalogEntryPtr> runningQueries = QueryCatalog::instance().getRunningQueries();
    map<string, string> result;

    for(auto [key, value] : runningQueries) {
        result[key] = value->queryString;
    }

    return result:
}

}

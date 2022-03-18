/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {
QueryCatalogServicePtr QueryCatalogService::create(QueryCatalogPtr queryCatalog) {
    return std::make_shared<QueryCatalogService>(new QueryCatalogService(queryCatalog));
}

QueryCatalogService::QueryCatalogService(QueryCatalogPtr queryCatalog) : queryCatalog(queryCatalog) {}

bool QueryCatalogService::checkSoftStopPossible(QueryId queryId) {

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        //TODO: Throw exception
    }

    //Fetch the current entry for the query
    auto queryEntry = queryCatalog->getQueryCatalogEntry(queryId);
    QueryStatus currentQueryStatus = queryEntry->getQueryStatus();

    //If query is doing hard stop or has failed or already stopped then soft stop can not be triggered
    if (currentQueryStatus == MarkedForStop || currentQueryStatus == Failed || currentQueryStatus == Stopped) {
        NES_WARNING("QueryCatalogService: Soft stop can not be initiated as query is " << queryEntry->getQueryStatusAsString());
        return false;
    }
    return true;
}

QueryCatalogEntryPtr QueryCatalogService::getEntryForQuery(QueryId queryId) {
    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        //TODO: Throw exception
    }

    //return query catalog entry
    return queryCatalog->getQueryCatalogEntry(queryId);
}

std::map<uint64_t, std::string> QueryCatalogService::getAllEntriesInStatus(std::string queryStatus) {

    //

    //return queries with status
    return queryCatalog->getQueriesWithStatus(queryStatus);
}

bool QueryCatalogService::updateQueryStatus(QueryId queryId, QueryStatus queryStatus) { return false; }

}// namespace NES

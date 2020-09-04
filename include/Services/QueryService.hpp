
#ifndef QUERYSERVICE_HPP
#define QUERYSERVICE_HPP

#include <API/Query.hpp>
#include <Plans/Query/QueryId.hpp>
#include <cpprest/json.h>

namespace NES {

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class QueryRequestQueue;
typedef std::shared_ptr<QueryRequestQueue> QueryRequestQueuePtr;

/**
 * @brief: This class is responsible for handling requests related to submitting, fetching information, and deleting different queries.
 */
class QueryService {

  public:
    explicit QueryService(QueryCatalogPtr queryCatalog, QueryRequestQueuePtr queryRequestQueue);

    ~QueryService();

    /**
     * Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryString : query in string form.
     * @param placementStrategyName : name of the placement strategy to be used.
     * @return queryId : query id of the valid input query.
     * @throws InvalidQueryException : when query string is not valid.
     * @throws InvalidArgumentException : when the placement strategy is not valid.
     */
    uint64_t validateAndQueueAddRequest(std::string queryString, std::string placementStrategyName);

    /**
     * Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryId : query id of the query to be stopped.
     * @returns: true if successful
     * @throws QueryNotFoundException : when query id is not found in the query catalog.
     * @throws InvalidQueryStatusException : when the query is found to be in an invalid state.
     */
    bool validateAndQueueStopRequest(QueryId queryId);

    /**
     * This method is used for generating the base query plan from the input query as string.
     * @param queryId : user query as string
     * @return a json object representing the query plan
     */
    web::json::value getQueryPlanAsJson(QueryId queryId);

  private:
    QueryCatalogPtr queryCatalog;
    QueryRequestQueuePtr queryRequestQueue;
};

};// namespace NES

#endif//QUERYSERVICE_HPP

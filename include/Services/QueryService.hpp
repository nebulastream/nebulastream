
#ifndef QUERYSERVICE_HPP
#define QUERYSERVICE_HPP

#include <API/Query.hpp>
#include <cpprest/json.h>

namespace NES {

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

/**
 * @brief: This class is responsible for handling requests related to submitting, fetching information, and deleting different queries.
 */
class QueryService {

  public:
    explicit QueryService(QueryCatalogPtr queryCatalog);

    ~QueryService() = default;

    /**
     * Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryString : query in string form.
     * @param placementStrategyName : name of the placement strategy to be used.
     * @return queryId : query id of the valid input query.
     * @throws InvalidQueryException : when query string is not valid.
     * @throws InvalidArgumentException : when the placement strategy is not valid.
     */
    std::string validateAndQueueAddRequest(std::string queryString, std::string placementStrategyName);

    /**
     * Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryId : query id of the query to be stopped.
     * @returns: true if successful
     * @throws QueryNotFoundException : when query id is not found in the query catalog.
     * @throws InvalidQueryStatusException : when the query is found to be in an invalid state.
     */
    bool validateAndQueueStopRequest(std::string queryId);

    /**
     * This method is used for generating the base query plan from the input query as string.
     * @param queryId : user query as string
     * @return a json object representing the query plan
     */
    web::json::value getQueryPlanAsJson(std::string queryId);

  private:
    QueryCatalogPtr queryCatalog;
};

};// namespace NES

#endif//QUERYSERVICE_HPP

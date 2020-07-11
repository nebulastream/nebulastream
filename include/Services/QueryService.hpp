
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
    std::string validateAndRegisterQuery(std::string queryString, std::string placementStrategyName);

    /**
     * This method is used for generating the base query plan from the input query as string.
     *
     * @param queryId : user query as string
     * @return a json object representing the query plan
     */
    web::json::value getQueryPlanAsJson(std::string queryId);

    /**
     * This method is used for generating the query object from the input query as string.
     *
     * @param userQuery : user query as string
     * @return a json object representing the query plan
     */
    QueryPtr getQueryFromQueryString(std::string userQuery);

    /**
     * @brief method for deploying and starting the query
     * @param queryId : the query Id of the query to be deployed and started
     * @return true if successful else false
     */
    bool deployAndStartQuery(std::string queryId);

    /**
     * @brief method to stop a query
     * @param queryId
     * @return bool indicating success
     */
    bool stopQuery(std::string queryId);

//    /**
//     * @brief ungregisters a query
//     * @param queryID
//     * @return bool indicating success
//     */
//    bool unregisterQuery(std::string queryId);
//
//    /**
//     * @brief method to deregister, undeploy, and stop a query
//     * @param queryID
//     * @return bool indicating success
//     */
//    bool removeQuery(std::string queryId);

  private:

    /**
     * @brief method send query to nodes
     * @param queryId
     * @return bool indicating success
     */
    bool deployQuery(std::string queryId);

    /**
     * @brief method to start a already deployed query
     * @param queryId
     * @return bool indicating success
     */
    bool startQuery(std::string queryId);

    /**
     * @brief method remove query from nodes
     * @param queryId
     * @return bool indicating success
     */
    bool undeployQuery(std::string queryId);

    QueryCatalogPtr queryCatalog;
};

};// namespace NES

#endif//QUERYSERVICE_HPP


#ifndef QUERYSERVICE_HPP
#define QUERYSERVICE_HPP

#include <API/Query.hpp>
#include <cpprest/json.h>

namespace NES {

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

/**\brief:
 *          This class is used for serving different requests related to user query.
 *
 * Note: Please extend the header if new Input query related functionality needed to be added.
 *
 */
class QueryService {

  public:
    ~QueryService() = default;

    QueryService(StreamCatalogPtr streamCatalog);

    /**
     * This method is used for generating the base query plan from the input query as string.
     *
     * @param userQuery : user query as string
     * @return a json object representing the query plan
     */
    web::json::value generateBaseQueryPlanFromQueryString(std::string userQuery);

    /**
     * This method is used for generating the query object from the input query as string.
     *
     * @param userQuery : user query as string
     * @return a json object representing the query plan
     */
    QueryPtr getQueryFromQueryString(std::string userQuery);

  private:
    StreamCatalogPtr streamCatalog;
};

};// namespace NES

#endif//QUERYSERVICE_HPP

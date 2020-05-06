#ifndef NES_IMPL_SERVICES_QUERYCATALOGSERVICE_HPP_
#define NES_IMPL_SERVICES_QUERYCATALOGSERVICE_HPP_

#include <map>
#include <memory>

namespace NES {

class QueryCatalogService;
typedef std::shared_ptr<QueryCatalogService> QueryCatalogServicePtr;

/**
 * This class is responsible for accessing query catalog and servicing the requests from Actor or REST framework.
 */
class QueryCatalogService {

  public:
    static QueryCatalogServicePtr getInstance() {
        static QueryCatalogServicePtr instance{new QueryCatalogService};
        return instance;
    }

    /**
     * @brief Get the queries in the user defined status
     * @param status : query status
     * @return returns map of query Id and query string
     * @throws exception in case of invalid status
     */
    std::map<std::string, std::string> getQueriesWithStatus(std::string status);

    /**
     * @brief Get all queries registered in the system
     * @return map of query ids and query string with query status
     */
    std::map<std::string, std::string> getAllRegisteredQueries();

    ~QueryCatalogService() = default;

  private:
    QueryCatalogService() = default;
};

}// namespace NES

#endif//NES_IMPL_SERVICES_QUERYCATALOGSERVICE_HPP_

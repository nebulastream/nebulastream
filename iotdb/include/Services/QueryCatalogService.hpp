#ifndef NES_IMPL_SERVICES_QUERYCATALOGSERVICE_HPP_
#define NES_IMPL_SERVICES_QUERYCATALOGSERVICE_HPP_

#include <iostream>
#include <map>

namespace NES {

class QueryCatalogService {

  public:

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

};

}

#endif //NES_IMPL_SERVICES_QUERYCATALOGSERVICE_HPP_

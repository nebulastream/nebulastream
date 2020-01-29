#ifndef NES_IMPL_SERVICES_QUERYCATALOGSERVICE_HPP_
#define NES_IMPL_SERVICES_QUERYCATALOGSERVICE_HPP_

namespace NES {

class QueryCatalogService {

  public:

    std::map<std::string, std::string> getAllRunningQueries();

};

}

#endif //NES_IMPL_SERVICES_QUERYCATALOGSERVICE_HPP_

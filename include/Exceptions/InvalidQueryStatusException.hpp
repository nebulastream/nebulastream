#ifndef NES_INVALIDQUERYSTATUSEXCEPTION_HPP
#define NES_INVALIDQUERYSTATUSEXCEPTION_HPP

#include <Catalogs/QueryCatalogEntry.hpp>
#include <stdexcept>
#include <vector>

namespace NES{
class InvalidQueryStatusException : virtual public std::runtime_error {
  public:

    explicit InvalidQueryStatusException(std::vector<QueryStatus> expectedStatus, QueryStatus actualStatus);

    const char* what() const throw();

  private:
    std::vector<QueryStatus> expectedStatuses;
    QueryStatus actualStatus;
};
}
#endif//NES_INVALIDQUERYSTATUSEXCEPTION_HPP

#ifndef NES_INVALIDQUERYSTATUSEXCEPTION_HPP
#define NES_INVALIDQUERYSTATUSEXCEPTION_HPP

#include <Catalogs/QueryCatalogEntry.hpp>
#include <stdexcept>
#include <vector>

namespace NES {
class InvalidQueryStatusException : public std::exception {
  public:
    explicit InvalidQueryStatusException(std::vector<QueryStatus> expectedStatus, QueryStatus actualStatus);
    const char* what() const throw();

  private:
    std::string message;
};
}// namespace NES
#endif//NES_INVALIDQUERYSTATUSEXCEPTION_HPP

/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INVALIDQUERYSTATUSEXCEPTION_HPP
#define NES_INVALIDQUERYSTATUSEXCEPTION_HPP

#include <Catalogs/QueryCatalogEntry.hpp>
#include <stdexcept>
#include <vector>

namespace NES {
/**
 * @brief Exception is raised when the query is in an Invalid status
 */
class InvalidQueryStatusException : public std::exception {
  public:
    explicit InvalidQueryStatusException(std::vector<QueryStatus> expectedStatus, QueryStatus actualStatus);
    const char* what() const throw();

  private:
    std::string message;
};
}// namespace NES
#endif//NES_INVALIDQUERYSTATUSEXCEPTION_HPP

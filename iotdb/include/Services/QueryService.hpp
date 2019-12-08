
#ifndef IOTDB_QUERYSERVICE_HPP
#define IOTDB_QUERYSERVICE_HPP

#include <API/InputQuery.hpp>
#include <cpprest/json.h>

namespace iotdb {

/**\brief:
 *          This class is used for serving different requests related to user query.
 *
 * Note: Please extend the header if new Input query related functionality needed to be added.
 *
 */
class QueryService {

 private:

 public:

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
  InputQueryPtr getInputQueryFromQueryString(std::string userQuery);

};

};

#endif //IOTDB_QUERYSERVICE_HPP

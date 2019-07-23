
#ifndef IOTDB_QUERYSERVICE_HPP
#define IOTDB_QUERYSERVICE_HPP

#include <cpprest/json.h>

namespace iotdb {

    using namespace std;

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
        json::value generateBaseQueryPlanFromQueryString(string userQuery);

    };

};


#endif //IOTDB_QUERYSERVICE_HPP

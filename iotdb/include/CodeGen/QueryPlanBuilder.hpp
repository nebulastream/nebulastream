//
// Created by achaudhary on 07.07.19.
//

#ifndef IOTDB_QUERYPLANBUILDER_HPP
#define IOTDB_QUERYPLANBUILDER_HPP

#include <API/InputQuery.hpp>
#include <cpprest/json.h>

using namespace web;

namespace iotdb {

    class QueryPlanBuilder {

    public:
        QueryPlanBuilder(InputQuery userQuery);
        ~QueryPlanBuilder();
        json::value getBasePlan();

    private:
        InputQuery userquery;
        json::value emptyPlan();
        std::vector<json::value> getChildren(const OperatorPtr &root);
    };
}


#endif //IOTDB_QUERYPLANBUILDER_HPP

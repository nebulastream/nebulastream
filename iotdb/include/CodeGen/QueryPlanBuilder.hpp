//
// Created by achaudhary on 07.07.19.
//

#ifndef IOTDB_QUERYPLANBUILDER_HPP
#define IOTDB_QUERYPLANBUILDER_HPP

#include <API/InputQuery.hpp>
#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include <cpprest/json.h>

using namespace web;

namespace iotdb {

    class QueryPlanBuilder {

    public:
        QueryPlanBuilder();
        ~QueryPlanBuilder();
        json::value getBasePlan(InputQuery inputQuery);

    private:
        void getChildren(const OperatorPtr &root, std::vector<json::value> &nodes, std::vector<json::value> &edges);
    };
}


#endif //IOTDB_QUERYPLANBUILDER_HPP

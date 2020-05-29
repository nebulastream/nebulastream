#ifndef QUERYPLANBUILDER_HPP
#define QUERYPLANBUILDER_HPP

#include <API/InputQuery.hpp>
#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include <cpprest/json.h>

using namespace web;

namespace NES {

class OperatorJsonUtil {

  public:
    OperatorJsonUtil();
    ~OperatorJsonUtil();
    json::value getBasePlan(InputQueryPtr inputQuery);

  private:
    void getChildren(const OperatorPtr& root, std::vector<json::value>& nodes, std::vector<json::value>& edges);
};
}// namespace NES

#endif//QUERYPLANBUILDER_HPP

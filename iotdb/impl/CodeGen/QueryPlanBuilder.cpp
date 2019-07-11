#include "CodeGen/QueryPlanBuilder.hpp"
#include <Operators/Operator.hpp>

namespace iotdb {

    QueryPlanBuilder::QueryPlanBuilder() {};

    QueryPlanBuilder::~QueryPlanBuilder() {};

    json::value QueryPlanBuilder::getBasePlan(InputQuery inputQuery) {

        const OperatorPtr &root = inputQuery.getRoot();

        if (!root) {
            return emptyPlan();
        }

        auto basePlan = json::value::object();
        basePlan["name"] = json::value::string(root->toString());
        std::vector<json::value> children = getChildren(root);
        if (!children.empty()) {
            basePlan["children"] = json::value::array(children);
        }

        return basePlan;
    }

    json::value QueryPlanBuilder::emptyPlan() {

        auto emptyPlan = json::value::object();
        emptyPlan["name"] = json::value::string("Empty-0");
        return emptyPlan;
    }

    std::vector<json::value> QueryPlanBuilder::getChildren(const OperatorPtr &root) {

        std::vector<json::value> childrenNode;

        std::vector<OperatorPtr> &children = root->childs;
        if (children.empty()) {
            return childrenNode;
        }

        for (OperatorPtr child : children) {
            
            auto childJson = json::value::object();
            childJson["name"] = json::value::string(child->toString());
            std::vector<json::value> grandChildren = getChildren(child);

            if(!grandChildren.empty()) {
                childJson["children"] = json::value::array(grandChildren);
            }
            childrenNode.push_back(childJson);
        }

        return childrenNode;
    }
}
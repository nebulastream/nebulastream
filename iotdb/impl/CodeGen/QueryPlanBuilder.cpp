#include "CodeGen/QueryPlanBuilder.hpp"
#include <Operators/Operator.hpp>

namespace iotdb {

    QueryPlanBuilder::QueryPlanBuilder(iotdb::InputQuery userQuery) : userquery(userQuery) {};

    QueryPlanBuilder::~QueryPlanBuilder() {};

    json::value QueryPlanBuilder::getBasePlan() {

        const OperatorPtr &root = userquery.getRoot();

        if (!root) {
            return emptyPlan();
        }

        auto basePlan = json::value::object();
        basePlan["name"] = json::value::string(root->toString());
        std::vector<json::value> children = getChildren(root);
        if (!children.empty()) {
            basePlan["children"] = json::value::array(children);
        }

//
//        auto lvl2Node = json::value::object();
//        lvl2Node["name"] = json::value::string("Sink-3");
//
//        std::vector<json::value> listOfLvl2Children;
//        listOfLvl2Children.push_back(lvl2Node);
//
//        auto lvl1Node = json::value::object();
//        lvl1Node["name"] = json::value::string("Map-1");
//        lvl1Node["children"] = json::value::array(listOfLvl2Children);
//
//        std::vector<json::value> listOfLvl1Children;
//        listOfLvl1Children.push_back(lvl1Node);
//
//        auto rootNode = json::value::object();
//        rootNode["name"] = json::value::string("Source-0");
//        rootNode["children"] = json::value::array(listOfLvl1Children);

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
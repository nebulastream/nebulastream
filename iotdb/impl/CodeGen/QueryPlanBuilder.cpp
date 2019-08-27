#include "CodeGen/QueryPlanBuilder.hpp"
#include <Operators/Operator.hpp>

namespace iotdb {

    QueryPlanBuilder::QueryPlanBuilder() {};

    QueryPlanBuilder::~QueryPlanBuilder() {};

    json::value QueryPlanBuilder::getBasePlan(InputQuery inputQuery) {

        json::value result{};
        std::vector<json::value> nodes{};
        std::vector<json::value> edges{};
        const OperatorPtr &root = inputQuery.getRoot();

        if (!root) {
            auto node = json::value::object();
            node["id"] = json::value::string("NONE");
            node["title"] = json::value::string("NONE");
            nodes.push_back(node);
        } else {

            auto node = json::value::object();
            node["id"] = json::value::string(root->toString() + std::to_string(root->cost));
            node["title"] = json::value::string(root->toString() + std::to_string(root->cost));
            if (root->getOperatorType() == OperatorType::SOURCE_OP ||
                root->getOperatorType() == OperatorType::SINK_OP) {
                node["nodeType"] = json::value::string("Source");
            } else {
                node["nodeType"] = json::value::string("Processor");
            }
            nodes.push_back(node);
            getChildren(root, nodes, edges);
        }

        result["nodes"] = json::value::array(nodes);
        result["edges"] = json::value::array(edges);

        return result;
    }

    void QueryPlanBuilder::getChildren(const OperatorPtr &root, std::vector<json::value> &nodes,
                                       std::vector<json::value> &edges) {

        std::vector<json::value> childrenNode;

        std::vector<OperatorPtr> &children = root->childs;
        if (children.empty()) {
            return;
        }

        for (OperatorPtr child : children) {
            auto node = json::value::object();
            node["id"] = json::value::string(child->toString() + std::to_string(child->cost));
            node["title"] = json::value::string(child->toString() + std::to_string(child->cost));

            if (child->getOperatorType() == OperatorType::SOURCE_OP ||
                child->getOperatorType() == OperatorType::SINK_OP) {
                node["nodeType"] = json::value::string("Source");
            } else {
                node["nodeType"] = json::value::string("Processor");
            }

            nodes.push_back(node);

            auto edge = json::value::object();
            edge["source"] = json::value::string(child->toString() + std::to_string(child->cost));
            edge["target"] = json::value::string(root->toString() + std::to_string(root->cost));

            edges.push_back(edge);
            getChildren(child, nodes, edges);
        }
    }
}
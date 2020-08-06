#include <API/Query.hpp>
#include <Operators/Operator.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
namespace NES {

OperatorJsonUtil::OperatorJsonUtil(){};

OperatorJsonUtil::~OperatorJsonUtil(){};

json::value OperatorJsonUtil::getBasePlan(QueryPlanPtr queryPlan) {

    json::value result{};
    std::vector<json::value> nodes{};
    std::vector<json::value> edges{};

    // FIXME this should not be translated to legacy here, Because we are at the coordinator we have no buffermanger.
    TranslateToLegacyPlanPhasePtr translator = TranslateToLegacyPlanPhase::create();
    //FIXME: this needs to be changed but should be covered in another issue
    const OperatorNodePtr root = queryPlan->getRootOperators()[0];
    OperatorPtr rootLegacyOperator = translator->transform(root, nullptr);// TODO check if this is ok here

    if (!root) {
        auto node = json::value::object();
        node["id"] = json::value::string("NONE");
        node["title"] = json::value::string("NONE");
        nodes.push_back(node);
    } else {

        auto node = json::value::object();
        node["id"] = json::value::string(
            operatorTypeToString[rootLegacyOperator->getOperatorType()] + "(OP-" + std::to_string(rootLegacyOperator->getOperatorId()) + ")");
        node["title"] =
            json::value::string(
                operatorTypeToString[rootLegacyOperator->getOperatorType()] + +"(OP-" + std::to_string(rootLegacyOperator->getOperatorId()) + ")");
        if (rootLegacyOperator->getOperatorType() == OperatorType::SOURCE_OP || rootLegacyOperator->getOperatorType() == OperatorType::SINK_OP) {
            node["nodeType"] = json::value::string("Source");
        } else {
            node["nodeType"] = json::value::string("Processor");
        }
        nodes.push_back(node);
        getChildren(rootLegacyOperator, nodes, edges);
    }

    result["nodes"] = json::value::array(nodes);
    result["edges"] = json::value::array(edges);

    return result;
}

void OperatorJsonUtil::getChildren(const OperatorPtr root, std::vector<json::value>& nodes,
                                   std::vector<json::value>& edges) {

    std::vector<json::value> childrenNode;

    std::vector<OperatorPtr> children = root->getChildren();
    if (children.empty()) {
        return;
    }

    for (OperatorPtr child : children) {
        auto node = json::value::object();
        node["id"] =
            json::value::string(
                operatorTypeToString[child->getOperatorType()] + "(OP-" + std::to_string(child->getOperatorId()) + ")");
        node["title"] =
            json::value::string(
                operatorTypeToString[child->getOperatorType()] + "(OP-" + std::to_string(child->getOperatorId()) + ")");

        if (child->getOperatorType() == OperatorType::SOURCE_OP || child->getOperatorType() == OperatorType::SINK_OP) {
            node["nodeType"] = json::value::string("Source");
        } else {
            node["nodeType"] = json::value::string("Processor");
        }

        nodes.push_back(node);

        auto edge = json::value::object();
        edge["source"] =
            json::value::string(
                operatorTypeToString[child->getOperatorType()] + "(OP-" + std::to_string(child->getOperatorId()) + ")");
        edge["target"] =
            json::value::string(
                operatorTypeToString[root->getOperatorType()] + "(OP-" + std::to_string(root->getOperatorId()) + ")");

        edges.push_back(edge);
        getChildren(child, nodes, edges);
    }
}
}// namespace NES
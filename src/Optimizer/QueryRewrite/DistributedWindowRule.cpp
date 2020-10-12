#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/WindowDefinition.hpp>
#include <algorithm>

namespace NES {

DistributeWindowRule::DistributeWindowRule() {}

DistributeWindowRulePtr DistributeWindowRule::create() {
    return std::make_shared<DistributeWindowRule>(DistributeWindowRule());
}

QueryPlanPtr DistributeWindowRule::apply(QueryPlanPtr queryPlan) {
    NES_INFO("DistributeWindowRule: Apply DistributeWindowRule.");

    auto windowOps = queryPlan->getOperatorByType<WindowLogicalOperatorNode>();
    if (!windowOps.empty()) {
        NES_DEBUG("DistributeWindowRule::apply: found " << windowOps.size() << " window operators");
        for (auto& windowOp : windowOps) {
            NES_DEBUG("DistributeWindowRule::apply: window operator " << windowOp << " << windowOp->toString()");
            if (windowOp->getChildren().size() < CHILD_NODE_THRESHOLD) {
                NES_DEBUG("DistributeWindowRule::apply: introduce centralized window operator for window " << windowOp << " << windowOp->toString()");
                auto newWindowOp = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowOp->getWindowDefinition());
                newWindowOp->setInputSchema(windowOp->getInputSchema());
                newWindowOp->setOutputSchema(windowOp->getOutputSchema());
                newWindowOp->setId(UtilityFunctions::getNextOperatorId());

                NES_DEBUG("DistributeWindowRule::apply: newNode=" << newWindowOp->toString() << " old node=" << windowOp->toString());
                NES_DEBUG("DistributeWindowRule::apply: plan before replace " << queryPlan->toString());
                windowOp->replace(newWindowOp);
                NES_DEBUG("DistributeWindowRule::apply: plan after replace " << queryPlan->toString());
            } else {
                NES_DEBUG("DistributeWindowRule::apply: introduce distributed window operator for window " << windowOp << " << windowOp->toString()");
                auto winDef = windowOp->getWindowDefinition();

                auto newWinDef = WindowDefinition::create(winDef->getOnKey(),
                                                          winDef->getWindowAggregation(),
                                                          winDef->getWindowType(),
                                                          DistributionCharacteristic::createCombiningWindowType(),
                                                          windowOp->getChildren().size());

                auto newWindowOp = LogicalOperatorFactory::createWindowComputationSpecializedOperator(newWinDef);
                newWindowOp->setInputSchema(windowOp->getInputSchema());
                newWindowOp->setOutputSchema(windowOp->getOutputSchema());
                newWindowOp->setId(UtilityFunctions::getNextOperatorId());

                //replace logical window op with combiner
                NES_DEBUG("DistributeWindowRule::apply: newNode=" << newWindowOp->toString() << " old node=" << windowOp->toString());
                NES_DEBUG("DistributeWindowRule::apply: plan before replace " << queryPlan->toString());
                windowOp->replace(newWindowOp);
                NES_DEBUG("DistributeWindowRule::apply: plan after replace " << queryPlan->toString());

                //add a slicer to each child
                std::vector<NodePtr> copyOfChilds = newWindowOp->getChildren();
                for (auto& child : copyOfChilds) {
                    NES_DEBUG("DistributeWindowRule::apply: process child " << child->toString());
                    NES_DEBUG("DistributeWindowRule::apply: plan before insert child " << queryPlan->toString());
                    auto newWinDef2 = WindowDefinition::create(winDef->getOnKey(),
                                                               winDef->getWindowAggregation(),
                                                               winDef->getWindowType(),
                                                               DistributionCharacteristic::createSlicingWindowType(), 1);
                    auto sliceOp = LogicalOperatorFactory::createSliceCreationSpecializedOperator(newWinDef2);
                    sliceOp->setId(UtilityFunctions::getNextOperatorId());
                    child->insertBetweenThisAndParentNodes(sliceOp);
                    NES_DEBUG("DistributeWindowRule::apply: plan after insert child " << queryPlan->toString());
                }
            }
        }
    } else {
        NES_DEBUG("DistributeWindowRule::apply: no window operator in query");
    }
    return queryPlan;
}

}// namespace NES

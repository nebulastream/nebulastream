#include <Optimizer/QueryRewrite/DistributeWindowRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <API/Window/WindowDefinition.hpp>

namespace NES {

DistributeWindowRule::DistributeWindowRule() {}

DistributeWindowRulePtr DistributeWindowRule::create( ) {
    return std::make_shared<DistributeWindowRule>(DistributeWindowRule());
}

QueryPlanPtr DistributeWindowRule::apply(QueryPlanPtr queryPlan) {
    NES_INFO("DistributeWindowRule: Apply DistributeWindowRule.");

    std::vector<OperatorNodePtr> windowOps = queryPlan->getOperatorByType<WindowLogicalOperatorNode>();
    if (windowOps.size() > 0) {
        NES_DEBUG("DistributeWindowRule::apply: found " << windowOps.size()  << " window operators");
        for (auto& windowOp : windowOps) {
            NES_DEBUG("DistributeWindowRule::apply: window operator " << windowOp << " << windowOp->toString()");
            if(windowOp->getChildren().size() < 2) {//TODO:change this back
                NES_DEBUG("DistributeWindowRule::apply: introduce centralized window operator for window " << windowOp << " << windowOp->toString()");

                WindowLogicalOperatorNode* winOp = dynamic_cast<WindowLogicalOperatorNode*>(windowOp.get());
                LogicalOperatorNodePtr newWindowOp = createCentralWindowSpecializedOperatorNode(winOp->getWindowDefinition());
                newWindowOp->setInputSchema(winOp->getInputSchema());
                newWindowOp->setOutputSchema(winOp->getOutputSchema());
                newWindowOp->setId(UtilityFunctions::getNextOperatorId());

                NES_DEBUG("DistributeWindowRule::apply: newNode=" << newWindowOp->toString() << " old node=" << windowOp->toString());
                NES_DEBUG("DistributeWindowRule::apply: plan before replace " << queryPlan->toString());
                windowOp->replace(newWindowOp);
                NES_DEBUG("DistributeWindowRule::apply: plan after replace " << queryPlan->toString());
            }
            else
            {
                NES_DEBUG("DistributeWindowRule::apply: introduce distributed window operator for window " << windowOp << " << windowOp->toString()");

                WindowLogicalOperatorNode* winOp = dynamic_cast<WindowLogicalOperatorNode*>(windowOp.get());
                WindowDefinitionPtr winDef = winOp->getWindowDefinition();

                WindowDefinitionPtr newWinDef = createWindowDefinition(winDef->onKey, winDef->windowAggregation, winDef->windowType, DistributionCharacteristic::createCombiningWindowType());

                LogicalOperatorNodePtr newWindowOp = createWindowComputationSpecializedOperatorNode(newWinDef);
                newWindowOp->setInputSchema(winOp->getInputSchema());
                newWindowOp->setOutputSchema(winOp->getOutputSchema());
                newWindowOp->setId(UtilityFunctions::getNextOperatorId());

                //replace logical window op with combiner
                NES_DEBUG("DistributeWindowRule::apply: newNode=" << newWindowOp->toString() << " old node=" << windowOp->toString());
                NES_DEBUG("DistributeWindowRule::apply: plan before replace " << queryPlan->toString());
                windowOp->replace(newWindowOp);
                NES_DEBUG("DistributeWindowRule::apply: plan after replace " << queryPlan->toString());

                //add a slicer to each child
                std::vector<NodePtr> copyOfChilds = newWindowOp->getChildren();
                for(auto& child : copyOfChilds)
                {
                    NES_DEBUG("DistributeWindowRule::apply: process child " << child->toString());
                    NES_DEBUG("DistributeWindowRule::apply: plan before insert child " << queryPlan->toString());
//                    WindowDefinitionPtr windDef = winOp->getWindowDefinition();
//                    windDef->setDistributionCharacteristic(DistributionCharacteristic::createSlicingWindowType());
                    WindowDefinitionPtr newWinDef2 = createWindowDefinition(winDef->onKey, winDef->windowAggregation, winDef->windowType, DistributionCharacteristic::createSlicingWindowType());
                    LogicalOperatorNodePtr sliceOp = createSliceCreationSpecializedOperatorNode(newWinDef2);

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

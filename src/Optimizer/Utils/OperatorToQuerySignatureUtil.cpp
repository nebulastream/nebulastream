/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/BroadcastLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/Utils/DataTypeToZ3ExprUtil.hpp>
#include <Optimizer/Utils/ExpressionToZ3ExprUtil.hpp>
#include <Optimizer/Utils/OperatorToQuerySignatureUtil.hpp>
#include <Optimizer/Utils/Z3ExprAndFieldMap.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <regex>
#include <z3++.h>

namespace NES::Optimizer {

QuerySignaturePtr OperatorToQuerySignatureUtil::createForOperator(OperatorNodePtr operatorNode,
                                                                  std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                                  z3::ContextPtr context) {
    NES_DEBUG("OperatorToQueryPlanSignatureUtil: Creating Z3 expression for operator " << operatorNode->toString());
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        if (!childrenQuerySignatures.empty()) {
            NES_THROW_RUNTIME_ERROR("Source operator can't have children : " + operatorNode->toString());
        }
        //Will return a Z3 expression equivalent to: streamName = <logical stream name>
        SourceLogicalOperatorNodePtr sourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        auto var = context->constant(context->str_symbol("streamName"), context->string_sort());
        auto val = context->string_val(sourceOperator->getSourceDescriptor()->getStreamName());
        auto conds = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_eq(*context, var, val)));
        return QuerySignature::create(conds, {});
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        if (childrenQuerySignatures.empty()) {
            NES_THROW_RUNTIME_ERROR("Sink operator can't have empty children set : " + operatorNode->toString());
        }
        //Will return a z3 expression computed by CNF of children signatures
        return buildFromChildrenSignatures(context, childrenQuerySignatures);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        //We do not expect a filter to have no or more than 1 children
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Filter operator can't have empty or more than one children : " + operatorNode->toString());
        }

        NES_TRACE("OperatorToQueryPlanSignatureUtil: Computing Z3Expression and filed map for filter predicate");
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        return createForFilter(context, childrenQuerySignatures, filterOperator);
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        //TODO: Will be done in issue #1272
        NES_NOT_IMPLEMENTED();
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        //We do not expect a broadcast operator to have no or more than 1 children
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Broadcast operator can't have empty or more than one children : "
                                    + operatorNode->toString());
        }
        return buildFromChildrenSignatures(context, childrenQuerySignatures);
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        //We do not expect a Map operator to have no or more than 1 children
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Map operator can't have empty or more than one children : " + operatorNode->toString());
        }

        // compute the expression for only the right side of the assignment operator
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        return createForMap(context, childrenQuerySignatures, mapOperator);
    } else if (operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        //We do not expect a window operator to have no or more than 1 children
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Window operator can't have empty or more than one children : " + operatorNode->toString());
        }
        NES_TRACE("OperatorToQueryPlanSignatureUtil: Computing Signature for window operator");
        auto windowOperator = operatorNode->as<WindowLogicalOperatorNode>();
        return createForWindow(context, childrenQuerySignatures, windowOperator);
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for operator: " + operatorNode->toString());
}

QuerySignaturePtr
OperatorToQuerySignatureUtil::buildFromChildrenSignatures(z3::ContextPtr context,
                                                          std::vector<QuerySignaturePtr> childrenQuerySignatures) {

    NES_DEBUG("OperatorToQueryPlanSignatureUtil: Computing Signature from children signatures");

    z3::expr_vector allConds(*context);
    std::map<std::string, std::vector<z3::ExprPtr>> allCols;

    for (auto& subQuerySignature : childrenQuerySignatures) {
        //Merge the columns together from different children signatures
        if (allCols.empty()) {
            allCols = subQuerySignature->getCols();
        } else {
            for (auto [colName, colExprs] : subQuerySignature->getCols()) {
                if (allCols.find(colName) != allCols.end()) {
                    std::vector<z3::ExprPtr> existingColExprs = allCols[colName];
                    existingColExprs.insert(existingColExprs.end(), colExprs.begin(), colExprs.end());
                    allCols[colName] = existingColExprs;
                } else {
                    allCols[colName] = colExprs;
                }
            }
        }
        //Add condition to the array
        allConds.push_back(*subQuerySignature->getConds());
    }

    //Create a CNF using all conditions from children signatures
    z3::ExprPtr conds = std::make_shared<z3::expr>(z3::mk_and(allConds));
    return QuerySignature::create(conds, allCols);
}

QuerySignaturePtr OperatorToQuerySignatureUtil::createForWindow(z3::ContextPtr context,
                                                                std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                                WindowLogicalOperatorNodePtr windowOperator) {
    auto windowDetails = SerializableOperator_WindowDetails();
    auto windowDefinition = windowOperator->getWindowDefinition();

    if (windowDefinition->isKeyed()) {
        ExpressionSerializationUtil::serializeExpression(windowDefinition->getOnKey(), windowDetails.mutable_onkey());
    }
    windowDetails.set_numberofinputedges(windowDefinition->getNumberOfInputEdges());

    auto windowType = windowDefinition->getWindowType();
    auto timeCharacteristic = windowType->getTimeCharacteristic();
    auto timeCharacteristicDetails = SerializableOperator_WindowDetails_TimeCharacteristic();
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::EventTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_WindowDetails_TimeCharacteristic_Type_EventTime);
        timeCharacteristicDetails.set_field(timeCharacteristic->getField()->name);
    } else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        timeCharacteristicDetails.set_type(SerializableOperator_WindowDetails_TimeCharacteristic_Type_IngestionTime);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
    }
    timeCharacteristicDetails.set_multiplier(timeCharacteristic->getTimeUnit().getMultiplier());

    if (windowType->isTumblingWindow()) {
        auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType);
        auto tumblingWindowDetails = SerializableOperator_WindowDetails_TumblingWindow();
        tumblingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        tumblingWindowDetails.set_size(tumblingWindow->getSize().getTime());
        windowDetails.mutable_windowtype()->PackFrom(tumblingWindowDetails);
    } else if (windowType->isSlidingWindow()) {
        auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType);
        auto slidingWindowDetails = SerializableOperator_WindowDetails_SlidingWindow();
        slidingWindowDetails.mutable_timecharacteristic()->CopyFrom(timeCharacteristicDetails);
        slidingWindowDetails.set_size(slidingWindow->getSize().getTime());
        slidingWindowDetails.set_slide(slidingWindow->getSlide().getTime());
        windowDetails.mutable_windowtype()->PackFrom(slidingWindowDetails);
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
    }

    // serialize aggregation
    auto windowAggregation = windowDetails.mutable_windowaggregation();
    ExpressionSerializationUtil::serializeExpression(windowDefinition->getWindowAggregation()->as(),
                                                     windowAggregation->mutable_asfield());
    ExpressionSerializationUtil::serializeExpression(windowDefinition->getWindowAggregation()->on(),
                                                     windowAggregation->mutable_onfield());

    switch (windowDefinition->getWindowAggregation()->getType()) {
        case Windowing::WindowAggregationDescriptor::Count:
            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_COUNT);
            break;
        case Windowing::WindowAggregationDescriptor::Max:
            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MAX);
            break;
        case Windowing::WindowAggregationDescriptor::Min:
            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_MIN);
            break;
        case Windowing::WindowAggregationDescriptor::Sum:
            windowAggregation->set_type(SerializableOperator_WindowDetails_Aggregation_Type_SUM);
            break;
        default: NES_FATAL_ERROR("OperatorSerializationUtil: could not cast aggregation type");
    }

    auto windowTrigger = windowDetails.mutable_triggerpolicy();

    switch (windowDefinition->getTriggerPolicy()->getPolicyType()) {
        case Windowing::TriggerType::triggerOnTime: {
            windowTrigger->set_type(SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnTime);
            Windowing::OnTimeTriggerDescriptionPtr triggerDesc =
                std::dynamic_pointer_cast<Windowing::OnTimeTriggerPolicyDescription>(windowDefinition->getTriggerPolicy());
            windowTrigger->set_timeinms(triggerDesc->getTriggerTimeInMs());
            break;
        }
        case Windowing::TriggerType::triggerOnRecord: {
            windowTrigger->set_type(SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnRecord);
            break;
        }
        case Windowing::TriggerType::triggerOnBuffer: {
            windowTrigger->set_type(SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnBuffer);
            break;
        }
        case Windowing::TriggerType::triggerOnWatermarkChange: {
            windowTrigger->set_type(SerializableOperator_WindowDetails_TriggerPolicy_Type_triggerOnWatermarkChange);
            break;
        }
        default: {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not cast aggregation type");
        }
    }

    auto windowAction = windowDetails.mutable_action();
    switch (windowDefinition->getTriggerAction()->getActionType()) {
        case Windowing::ActionType::WindowAggregationTriggerAction: {
            windowAction->set_type(SerializableOperator_WindowDetails_TriggerAction_Type_Complete);
            break;
        }
        case Windowing::ActionType::SliceAggregationTriggerAction: {
            windowAction->set_type(SerializableOperator_WindowDetails_TriggerAction_Type_Slicing);
            break;
        }
        default: {
            NES_FATAL_ERROR("OperatorSerializationUtil: could not cast action type");
        }
    }

    auto distributionCharacteristics = SerializableOperator_WindowDetails_DistributionCharacteristic();
    if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Complete) {
        windowDetails.mutable_distrchar()->set_distr(
            SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Complete);
    } else if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Combining) {
        windowDetails.mutable_distrchar()->set_distr(
            SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Combining);
    } else if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Slicing) {
        windowDetails.mutable_distrchar()->set_distr(
            SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Slicing);
    } else if (windowDefinition->getDistributionType()->getType() == Windowing::DistributionCharacteristic::Merging) {
        windowDetails.mutable_distrchar()->set_distr(
            SerializableOperator_WindowDetails_DistributionCharacteristic_Distribution_Merging);
    } else {
        NES_NOT_IMPLEMENTED();
    }
    return windowDetails;
}

QuerySignaturePtr OperatorToQuerySignatureUtil::createForMap(z3::ContextPtr context,
                                                             std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                             MapLogicalOperatorNodePtr mapOperator) {
    z3::ExprPtr expr = ExpressionToZ3ExprUtil::createForExpression(mapOperator->getMapExpression(), context)->getExpr();

    //Fetch the signature of only children and get the column values
    auto cols = childrenQuerySignatures[0]->getCols();

    //Prepare all combinations of col expression for the given map by substituting the previous col values in the assignment expression
    std::vector<z3::ExprPtr> allColExprForMap{expr};
    for (unsigned i = 0; i < expr->num_args(); i++) {
        const auto& exprArg = expr->arg(i);
        auto exprArgString = exprArg.to_string();
        if (cols.find(exprArgString) != cols.end()) {
            auto exprArgCol = cols[exprArgString];
            z3::expr_vector from(*context);
            from.push_back(exprArg);

            std::vector<z3::ExprPtr> updatedColExprForMap;
            for (auto& colExpr : exprArgCol) {
                for (auto& mapColExpr : allColExprForMap) {
                    z3::expr_vector to(*context);
                    to.push_back(*colExpr);
                    auto newColExpr = std::make_shared<z3::expr>(mapColExpr->substitute(from, to));
                    updatedColExprForMap.push_back(newColExpr);
                }
            }
            allColExprForMap = updatedColExprForMap;
        }
    }
    std::string fieldName = mapOperator->getMapExpression()->getField()->getFieldName();
    cols[fieldName] = allColExprForMap;

    //Fetch child cond
    auto conds = childrenQuerySignatures[0]->getConds();

    return QuerySignature::create(conds, cols);
}

QuerySignaturePtr OperatorToQuerySignatureUtil::createForFilter(z3::ContextPtr context,
                                                                std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                                FilterLogicalOperatorNodePtr filterOperator) {

    auto operatorCond = ExpressionToZ3ExprUtil::createForExpression(filterOperator->getPredicate(), context);
    auto fieldMap = operatorCond->getFieldMap();
    auto z3Expr = operatorCond->getExpr();

    z3::expr_vector allConds(*context);
    allConds.push_back(*z3Expr);

    NES_TRACE("OperatorToQueryPlanSignatureUtil: Replace Z3 Expression for the filed with corresponding column values from "
              "children signatures");
    //As filter can't have more than 1 children so fetch the only child signature
    auto subQueryCols = childrenQuerySignatures[0]->getCols();

    //Prepare a vector of conditions by substituting the field expressions in the predicate with col values from children signature
    for (auto constPair : fieldMap) {
        if (subQueryCols.find(constPair.first) != subQueryCols.end()) {
            std::vector<z3::ExprPtr> vectorOfExprs = subQueryCols[constPair.first];
            z3::expr_vector from(*context);
            from.push_back(*constPair.second);
            z3::expr_vector updatedConds(*context);
            for (auto expr : vectorOfExprs) {
                z3::expr_vector to(*context);
                to.push_back(*expr);
                for (auto cond : allConds) {
                    auto updatedCond = cond.substitute(from, to);
                    updatedConds.push_back(updatedCond);
                }
            }
            allConds = updatedConds;
        }
    }
    //Compute a DNF condition for all different conditions identified by substituting the col values
    auto filterCond = z3::mk_or(allConds);

    //Compute a CNF condition using the children and filter conds
    auto childrenConds = childrenQuerySignatures[0]->getConds();
    Z3_ast array[] = {filterCond, *childrenConds};
    auto conds = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_and(*context, 2, array)));

    return QuerySignature::create(conds, subQueryCols);
}

}// namespace NES::Optimizer
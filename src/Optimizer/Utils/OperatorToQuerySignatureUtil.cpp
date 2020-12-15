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
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
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
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
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
    } else if(operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()){
        //FIXME: as part of the issue 1351
        return childrenQuerySignatures[0];
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for operator: " + operatorNode->toString());
}

QuerySignaturePtr
OperatorToQuerySignatureUtil::buildFromChildrenSignatures(z3::ContextPtr context,
                                                          std::vector<QuerySignaturePtr> childrenQuerySignatures) {

    NES_DEBUG("OperatorToQueryPlanSignatureUtil: Computing Signature from children signatures");
    z3::expr_vector allConditions(*context);
    std::map<std::string, std::vector<z3::ExprPtr>> allColumns;

    for (auto& subQuerySignature : childrenQuerySignatures) {
        //Merge the columns together from different children signatures
        if (allColumns.empty()) {
            allColumns = subQuerySignature->getColumns();
        } else {
            for (auto [columnName, columnExprs] : subQuerySignature->getColumns()) {
                if (allColumns.find(columnName) != allColumns.end()) {
                    std::vector<z3::ExprPtr> existingColExprs = allColumns[columnName];
                    existingColExprs.insert(existingColExprs.end(), columnExprs.begin(), columnExprs.end());
                    allColumns[columnName] = existingColExprs;
                } else {
                    allColumns[columnName] = columnExprs;
                }
            }
        }
        //Add condition to the array
        allConditions.push_back(*subQuerySignature->getConditions());
    }

    //Create a CNF using all conditions from children signatures
    z3::ExprPtr conditions = std::make_shared<z3::expr>(z3::mk_and(allConditions));
    return QuerySignature::create(conditions, allColumns);
}

QuerySignaturePtr OperatorToQuerySignatureUtil::createForWindow(z3::ContextPtr,
                                                                std::vector<QuerySignaturePtr>,
                                                                WindowLogicalOperatorNodePtr windowOperator) {

    auto windowDefinition = windowOperator->getWindowDefinition();
    if (windowDefinition->isKeyed()) {
        FieldAccessExpressionNodePtr key = windowDefinition->getOnKey();
    }

    auto windowType = windowDefinition->getWindowType();
    auto timeCharacteristic = windowType->getTimeCharacteristic();
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::EventTime) {
        timeCharacteristic->getField()->name;
    } else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::IngestionTime) {

    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Characteristic");
    }
    timeCharacteristic->getTimeUnit().getMultiplier();

    if (windowType->isTumblingWindow()) {
        auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType);
        tumblingWindow->getSize().getTime();
    } else if (windowType->isSlidingWindow()) {
        auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType);
        slidingWindow->getSize().getTime();
        slidingWindow->getSlide().getTime();
    } else {
        NES_ERROR("OperatorSerializationUtil: Cant serialize window Time Type");
    }

    // serialize aggregation
    windowDefinition->getWindowAggregation()->as();
    windowDefinition->getWindowAggregation()->on();

    switch (windowDefinition->getWindowAggregation()->getType()) {
        case Windowing::WindowAggregationDescriptor::Count:
            break;
        case Windowing::WindowAggregationDescriptor::Max:
            break;
        case Windowing::WindowAggregationDescriptor::Min:
            break;
        case Windowing::WindowAggregationDescriptor::Sum:
            break;
        default: NES_FATAL_ERROR("OperatorSerializationUtil: could not cast aggregation type");
    }

    return QuerySignature::create(nullptr, {});
}

QuerySignaturePtr OperatorToQuerySignatureUtil::createForMap(z3::ContextPtr context,
                                                             std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                             MapLogicalOperatorNodePtr mapOperator) {
    z3::ExprPtr expr = ExpressionToZ3ExprUtil::createForExpression(mapOperator->getMapExpression(), context)->getExpr();

    //Fetch the signature of only children and get the column values
    auto columns = childrenQuerySignatures[0]->getColumns();

    //Prepare all combinations of col expression for the given map by substituting the previous col values in the assignment expression
    std::vector<z3::ExprPtr> allColExprForMap{expr};
    for (unsigned i = 0; i < expr->num_args(); i++) {
        const auto& exprArg = expr->arg(i);
        auto exprArgString = exprArg.to_string();
        if (columns.find(exprArgString) != columns.end()) {
            auto exprArgCol = columns[exprArgString];
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
    columns[fieldName] = allColExprForMap;

    //Fetch child cond
    auto conditions = childrenQuerySignatures[0]->getConditions();
    return QuerySignature::create(conditions, columns);
}

QuerySignaturePtr OperatorToQuerySignatureUtil::createForFilter(z3::ContextPtr context,
                                                                std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                                FilterLogicalOperatorNodePtr filterOperator) {

    auto operatorCond = ExpressionToZ3ExprUtil::createForExpression(filterOperator->getPredicate(), context);
    auto fieldMap = operatorCond->getFieldMap();
    auto z3Expr = operatorCond->getExpr();

    z3::expr_vector allConditions(*context);
    allConditions.push_back(*z3Expr);

    NES_TRACE("OperatorToQueryPlanSignatureUtil: Replace Z3 Expression for the filed with corresponding column values from "
              "children signatures");
    //As filter can't have more than 1 children so fetch the only child signature
    auto subQueryCols = childrenQuerySignatures[0]->getColumns();

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
                for (auto cond : allConditions) {
                    auto updatedCond = cond.substitute(from, to);
                    updatedConds.push_back(updatedCond);
                }
            }
            allConditions = updatedConds;
        }
    }
    //Compute a DNF condition for all different conditions identified by substituting the col values
    auto filterConditions = z3::mk_or(allConditions);

    //Compute a CNF condition using the children and filter conds
    auto childrenConditions = childrenQuerySignatures[0]->getConditions();
    Z3_ast array[] = {filterConditions, *childrenConditions};
    auto conditions = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_and(*context, 2, array)));
    return QuerySignature::create(conditions, subQueryCols);
}

}// namespace NES::Optimizer
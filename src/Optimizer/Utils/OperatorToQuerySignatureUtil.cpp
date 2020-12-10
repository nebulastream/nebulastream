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
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        //TODO: Will be done in issue #1272
        NES_NOT_IMPLEMENTED();
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        //We do not expect a broadcast operator to have no or more than 1 children
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Broadcast operator can't have empty or more than one children : " + operatorNode->toString());
        }
        return buildFromChildrenSignatures(context, childrenQuerySignatures);
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        //We do not expect a Map operator to have no or more than 1 children
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Map operator can't have empty or more than one children : " + operatorNode->toString());
        }

        // compute the expression for only the right side of the assignment operator
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        z3::ExprPtr expr = ExpressionToZ3ExprUtil::createForExpression(mapOperator->getMapExpression(), context)->getExpr();

        std::string fieldName = mapOperator->getMapExpression()->getField()->getFieldName();

        //Fetch the signature of only children and get the column values
        auto cols = childrenQuerySignatures[0]->getCols();

//        const z3::expr_vector& vector = expr->contains();
//        for(auto ele : vector){
//            NES_INFO(ele);
//        }
        
        if (cols.find(fieldName) != cols.end()) {
            //Substitute the field expressions with the children column values
            auto colExprs = cols[fieldName];

            //Compute the filed expression
            DataTypePtr fieldType = mapOperator->getMapExpression()->getField()->getStamp();
            auto filedExpr = DataTypeToZ3ExprUtil::createForField(fieldName, fieldType, context);

            z3::expr_vector from(*context);
            from.push_back(*filedExpr->getExpr());

            std::vector<z3::ExprPtr> updatedColExprs;
            for (auto& colExpr : colExprs) {
                z3::expr_vector to(*context);
                to.push_back(*colExpr);
                auto newColExpr = std::make_shared<z3::expr>(expr->substitute(from, to));
                updatedColExprs.push_back(newColExpr);
            }

            cols[fieldName] = updatedColExprs;
        } else {
            cols[fieldName] = {expr};
        }

        //Fetch child cond
        auto conds = childrenQuerySignatures[0]->getConds();

        return QuerySignature::create(conds, cols);
    } else if (operatorNode->instanceOf<WindowOperatorNode>()) {
        //TODO: Will be done in issue #1138
        NES_NOT_IMPLEMENTED();
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
        //Merge the columns
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
    z3::ExprPtr conds = std::make_shared<z3::expr>(z3::mk_and(allConds));
    return QuerySignature::create(conds, allCols);
}

}// namespace NES::Optimizer
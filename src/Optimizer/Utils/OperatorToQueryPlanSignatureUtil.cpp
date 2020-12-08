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
#include <Optimizer/Utils/OperatorToQueryPlanSignatureUtil.hpp>
#include <Optimizer/Utils/ReturnValue.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <z3++.h>

namespace NES::Optimizer {

QuerySignaturePtr OperatorToQueryPlanSignatureUtil::createForOperator(OperatorNodePtr operatorNode,
                                                                      std::vector<QuerySignaturePtr> subQuerySignatures,
                                                                      z3::ContextPtr context) {
    NES_TRACE("Creating Z3 expression for operator " << operatorNode->toString());
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        if (!subQuerySignatures.empty()) {
            NES_THROW_RUNTIME_ERROR("Source operator can't have children : " + operatorNode->toString());
        }
        SourceLogicalOperatorNodePtr sourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        auto var = context->constant(context->str_symbol("streamName"), context->string_sort());
        auto val = context->string_val(sourceOperator->getSourceDescriptor()->getStreamName());
        auto conds = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_eq(*context, var, val)));
        return QuerySignature::create(conds, {});
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        if (subQuerySignatures.empty()) {
            NES_THROW_RUNTIME_ERROR("Sink operator can't have empty children set : " + operatorNode->toString());
        }
        return buildFromSubQuerySignatures(context, subQuerySignatures);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        //FIXME: add comment explaining
        if (subQuerySignatures.empty() || subQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Filter operator can't have empty or more than one children : " + operatorNode->toString());
        }

        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        auto subQueryCols = subQuerySignatures[0]->getCols();

        auto operatorCond = ExpressionToZ3ExprUtil::createForExpression(filterOperator->getPredicate(), context);

        auto constMap = operatorCond->getConstMap();
        auto optrExpr = operatorCond->getExpr();

        for (auto constPair : constMap) {
            if (subQueryCols.find(constPair.first) != subQueryCols.end()) {
                std::vector<z3::ExprPtr> vectorOfExprs = subQueryCols[constPair.first];
                for (auto expr : vectorOfExprs) {
                    z3::expr_vector from(*context);
                    from.push_back(*constPair.second);
                    z3::expr_vector to(*context);
                    to.push_back(*expr);
                    optrExpr = std::make_shared<z3::expr>(optrExpr->substitute(from, to));
                }
            }
        }

        auto subQueryConds = subQuerySignatures[0]->getConds();
        Z3_ast array[] = {*optrExpr, *subQueryConds};
        auto conds = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_and(*context, 2, array)));
        return QuerySignature::create(conds, subQueryCols);
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        //TODO: Will be done in issue #1272
        NES_NOT_IMPLEMENTED();
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        return buildFromSubQuerySignatures(context, subQuerySignatures);
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        if (subQuerySignatures.empty() || subQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Map operator can't have empty or more than one children : " + operatorNode->toString());
        }
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        auto conds = subQuerySignatures[0]->getConds();
        auto cols = subQuerySignatures[0]->getCols();
        z3::ExprPtr expr = ExpressionToZ3ExprUtil::createForExpression(mapOperator->getMapExpression(), context)->getExpr();

        std::string fieldName = mapOperator->getMapExpression()->getField()->getFieldName();

        if (cols.find(fieldName) != cols.end()) {
            auto colExprs = cols[fieldName];
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
        return QuerySignature::create(conds, cols);
    } else if (operatorNode->instanceOf<WindowOperatorNode>()) {
        //TODO: Will be done in issue #1138
        NES_NOT_IMPLEMENTED();
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for operator: " + operatorNode->toString());
}

QuerySignaturePtr
OperatorToQueryPlanSignatureUtil::buildFromSubQuerySignatures(z3::ContextPtr context,
                                                              std::vector<QuerySignaturePtr> subQuerySignatures) {
    z3::expr_vector cnfConds(*context);
    std::map<std::string, std::vector<z3::ExprPtr>> allCols;

    for (auto& subQuerySignature : subQuerySignatures) {
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
        cnfConds.push_back(*subQuerySignature->getConds());
    }
    z3::ExprPtr conds = std::make_shared<z3::expr>(z3::mk_and(cnfConds));
    return QuerySignature::create(conds, allCols);
}

}// namespace NES::Optimizer
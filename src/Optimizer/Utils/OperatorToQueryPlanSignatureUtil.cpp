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
#include <Optimizer/QueryMerger/Signature/QueryPlanSignature.hpp>
#include <Optimizer/Utils/DataTypeToZ3ExprUtil.hpp>
#include <Optimizer/Utils/ExpressionToZ3ExprUtil.hpp>
#include <Optimizer/Utils/OperatorToQueryPlanSignatureUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <z3++.h>

namespace NES::Optimizer {

QueryPlanSignaturePtr OperatorToQueryPlanSignatureUtil::createForOperator(OperatorNodePtr operatorNode,
                                                                          std::vector<QueryPlanSignaturePtr> subQuerySignatures,
                                                                          z3::context& context) {
    NES_TRACE("Creating Z3 expression for operator " << operatorNode->toString());
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        if (!subQuerySignatures.empty()) {
            NES_THROW_RUNTIME_ERROR("Source operator can't have children : " + operatorNode->toString());
        }
        SourceLogicalOperatorNodePtr sourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        auto var = context.constant(context.str_symbol("streamName"), context.string_sort());
        auto val = context.string_val(sourceOperator->getSourceDescriptor()->getStreamName());
        auto conds = std::make_shared<z3::expr>(to_expr(context, Z3_mk_eq(context, var, val)));
        return QueryPlanSignature::create(conds, {});
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        if (subQuerySignatures.empty() || subQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Sink operator can't have empty or more than one children : " + operatorNode->toString());
        }
        return subQuerySignatures[0];
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        if (subQuerySignatures.empty() || subQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Filter operator can't have empty or more than one children : " + operatorNode->toString());
        }

        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        auto subQueryCols = subQuerySignatures[0]->getCols();
        auto subQueryConds = subQuerySignatures[0]->getConds();
        auto operatorCond = ExpressionToZ3ExprUtil::createForExpression(filterOperator->getPredicate(), subQueryCols, context);
        Z3_ast array[] = {*operatorCond, *subQueryConds};
        auto conds = std::make_shared<z3::expr>(to_expr(context, Z3_mk_and(context, 2, array)));


        return QueryPlanSignature::create(conds, subQueryCols);
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        //        auto var = context.bool_const("merge");
        //        auto val = context.bool_val(true);
        //        return to_expr(context, Z3_mk_eq(context, var, val));
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        //        auto var = context.bool_const("broadcast");
        //        auto val = context.bool_val(true);
        //        return to_expr(context, Z3_mk_eq(context, var, val));
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        if (subQuerySignatures.empty() || subQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("Map operator can't have empty or more than one children : " + operatorNode->toString());
        }
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        auto conds = subQuerySignatures[0]->getConds();
        auto cols = subQuerySignatures[0]->getCols();
        z3::ExprPtr expr = ExpressionToZ3ExprUtil::createForExpression(mapOperator->getMapExpression(), cols, context);

        std::string fieldName = mapOperator->getMapExpression()->getField()->getFieldName();

        if (cols.find(fieldName) != cols.end()) {
            auto colExprs = cols[fieldName];
            DataTypePtr fieldType = mapOperator->getMapExpression()->getField()->getStamp();
            auto filedExpr = DataTypeToZ3ExprUtil::createForField(fieldName, fieldType, context);
            z3::expr_vector from(context);
            from.push_back(*filedExpr);

            std::vector<z3::ExprPtr> updatedColExprs;
            for (auto& colExpr : colExprs) {
                z3::expr_vector to(context);
                to.push_back(*colExpr);
                auto newColExpr = std::make_shared<z3::expr>(expr->substitute(from, to));
                updatedColExprs.push_back(newColExpr);
            }

            cols[fieldName] = updatedColExprs;
        } else {
            cols[fieldName] = {expr};
        }
        NES_INFO("Condition: " << conds->to_string());
        return QueryPlanSignature::create(conds, cols);
    } else if (operatorNode->instanceOf<WindowOperatorNode>()) {
        //TODO: will be done in another issue
        NES_NOT_IMPLEMENTED();
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for operator: " + operatorNode->toString());
}

}// namespace NES::Optimizer
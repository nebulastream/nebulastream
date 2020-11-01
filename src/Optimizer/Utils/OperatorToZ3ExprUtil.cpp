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
#include <Optimizer/Utils/ExpressionToZ3ExprUtil.hpp>
#include <Optimizer/Utils/OperatorToZ3ExprUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <z3++.h>

namespace NES {

z3::expr OperatorToZ3ExprUtil::createForOperator(OperatorNodePtr operatorNode, z3::context& context) {
    NES_TRACE("Creating Z3 expression for operator " << operatorNode->toString());
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        SourceLogicalOperatorNodePtr sourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        auto var = context.constant(context.str_symbol("streamName"), context.string_sort());
        auto val = context.string_val(sourceOperator->getSourceDescriptor()->getStreamName());
        return to_expr(context, Z3_mk_eq(context, var, val));
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        auto var = context.bool_const("Sink");
        auto val = context.bool_val(true);
        return to_expr(context, Z3_mk_eq(context, var, val));
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        return ExpressionToZ3ExprUtil::createForExpression(filterOperator->getPredicate(), context);
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        auto var = context.bool_const("merge");
        auto val = context.bool_val(true);
        return to_expr(context, Z3_mk_eq(context, var, val));
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        auto var = context.bool_const("broadcast");
        auto val = context.bool_val(true);
        return to_expr(context, Z3_mk_eq(context, var, val));
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        return ExpressionToZ3ExprUtil::createForExpression(mapOperator->getMapExpression(), context);
    } else if (operatorNode->instanceOf<WindowOperatorNode>()) {
        //TODO: will be done in another issue
        NES_NOT_IMPLEMENTED();
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for operator: " + operatorNode->toString());
}

}// namespace NES
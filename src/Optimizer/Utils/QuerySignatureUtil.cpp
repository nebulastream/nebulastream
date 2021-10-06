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
#include <API/AttributeField.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/Utils/DataTypeToZ3ExprUtil.hpp>
#include <Optimizer/Utils/ExpressionToZ3ExprUtil.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Optimizer/Utils/Z3ExprAndFieldMap.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <z3++.h>

namespace NES::Optimizer {

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForOperator(const z3::ContextPtr& context,
                                                                      const OperatorNodePtr& operatorNode) {

    NES_DEBUG("QuerySignatureUtil: Creating query signature for operator " << operatorNode->toString());

    auto children = operatorNode->getChildren();
    if (operatorNode->isUnaryOperator()) {
        if (operatorNode->instanceOf<SourceLogicalOperatorNode>() && !children.empty()) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Source can't have children : " + operatorNode->toString());
        } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>() && children.empty()) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Source can't have empty children set : " + operatorNode->toString());
        } else if (!(operatorNode->instanceOf<SourceLogicalOperatorNode>() || operatorNode->instanceOf<SinkLogicalOperatorNode>())
                   && children.size() != 1) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Unary operator can have only one children : " + operatorNode->toString()
                                    + " found : " + std::to_string(children.size()));
        }
    } else if (operatorNode->isBinaryOperator() && children.size() != 2) {
        NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Binary operator can't have empty or only one children : "
                                + operatorNode->toString());
    }

    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for Source operator");
        SourceLogicalOperatorNodePtr sourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        return createQuerySignatureForSource(context, sourceOperator);
    }
    if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for Sink operator");
        NES_ASSERT(!children.empty(), "Sink operator should have atleast one children.");
        return children[0]->as<LogicalOperatorNode>()->getZ3Signature();
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for filter operator");
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        return createQuerySignatureForFilter(context, filterOperator);
    } else if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for Merge operator");
        auto unionOperator = operatorNode->as<UnionLogicalOperatorNode>();
        return createQuerySignatureForUnion(context, unionOperator);
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for Map operator");
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        return createQuerySignatureForMap(context, mapOperator);
    } else if (operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for window operator");
        auto windowOperator = operatorNode->as<WindowLogicalOperatorNode>();
        return createQuerySignatureForWindow(context, windowOperator);
    } else if (operatorNode->instanceOf<ProjectionLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for Project operator");
        auto projectOperator = operatorNode->as<ProjectionLogicalOperatorNode>();
        return createQuerySignatureForProject(projectOperator);
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for watermark operator");
        auto watermarkAssignerOperator = operatorNode->as<WatermarkAssignerLogicalOperatorNode>();
        return createQuerySignatureForWatermark(context, watermarkAssignerOperator);
    } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for join operator");
        auto joinOperator = operatorNode->as<JoinLogicalOperatorNode>();
        return createQuerySignatureForJoin(context, joinOperator);
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for operator: " + operatorNode->toString());
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForSource(const z3::ContextPtr& context,
                                                                    const SourceLogicalOperatorNodePtr& sourceOperator) {

    //Compute the column expressions for the source
    std::vector<std::string> columns;
    std::map<std::string, z3::ExprPtr> fieldToZ3ExprMap;
    SchemaPtr outputSchema = sourceOperator->getOutputSchema();
    for (auto& field : outputSchema->fields) {
        auto fieldName = field->getName();
        columns.emplace_back(fieldName);
        auto fieldExpr = DataTypeToZ3ExprUtil::createForField(fieldName, field->getDataType(), context)->getExpr();
        fieldToZ3ExprMap[fieldName] = fieldExpr;
    }
    auto updatedSchemaFieldToExprMaps = {fieldToZ3ExprMap};

    //Create an equality expression for example: <logical stream name>.streamName == "<logical stream name>"
    std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
    auto streamNameVarName = streamName + ".streamName";
    auto streamNameVar = context->constant(context->str_symbol(streamNameVarName.c_str()), context->string_sort());
    auto streamNameVal = context->string_val(streamName);
    //Construct Z3 expression using stream variable name and stream variable value
    auto conditions = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_eq(*context, streamNameVar, streamNameVal)));

    //Compute signature
    return QuerySignature::create(std::move(conditions), std::move(columns), updatedSchemaFieldToExprMaps, {});
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForProject(const ProjectionLogicalOperatorNodePtr& projectOperator) {

    //Get all children operators
    auto children = projectOperator->getChildren();
    NES_ASSERT(children.size() == 1, "Project operator should only have one and non null children.");
    auto childQuerySignature = children[0]->as<LogicalOperatorNode>()->getZ3Signature();
    auto columns = childQuerySignature->getColumns();

    //Extract projected columns
    std::vector<std::string> updatedColumns;
    std::vector<std::map<std::string, z3::ExprPtr>> updatedSchemaFieldToExprMaps;
    auto outputSchema = projectOperator->getOutputSchema();
    auto expressions = projectOperator->getExpressions();

    //Iterate over schema field to expression maps of the upstream child and create new schema map based on
    // projected attributes listed in the projection operator
    auto schemaFieldToExprMaps = childQuerySignature->getSchemaFieldToExprMaps();
    for (auto& schemaFieldToExprMap : schemaFieldToExprMaps) {
        std::map<std::string, z3::ExprPtr> updatedSchemaMap;

        //Iterate over projection expression and select the column name and expression from the schemaField to expression map of
        //upstream operator
        for (auto& expression : expressions) {

            //Identify the new field name and the old field name in the upstream operator
            std::string newFieldName;
            std::string fieldName;
            if (expression->instanceOf<FieldRenameExpressionNode>()) {
                auto fieldRename = expression->as<FieldRenameExpressionNode>();
                newFieldName = fieldRename->getNewFieldName();
                fieldName = fieldRename->getOriginalField()->getFieldName();
            } else {
                auto fieldAccess = expression->as<FieldAccessExpressionNode>();
                newFieldName = fieldAccess->getFieldName();
                fieldName = newFieldName;
            }

            auto found = schemaFieldToExprMap.find(fieldName);
            if (found == schemaFieldToExprMap.end()) {
                NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Unable to find projected field " + fieldName
                                        + " in children column set.");
            }
            //add the Field expression map
            updatedSchemaMap[newFieldName] = found->second;
        }
        //Insert the updated expressions
        updatedSchemaFieldToExprMaps.emplace_back(updatedSchemaMap);
    }

    for (auto& field : outputSchema->fields) {
        auto fieldName = field->getName();
        updatedColumns.emplace_back(fieldName);
    }

    auto conditions = childQuerySignature->getConditions();
    auto windowExpressions = childQuerySignature->getWindowsExpressions();
    return QuerySignature::create(std::move(conditions),
                                  std::move(updatedColumns),
                                  std::move(updatedSchemaFieldToExprMaps),
                                  std::move(windowExpressions));
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForMap(const z3::ContextPtr& context,
                                                                 const MapLogicalOperatorNodePtr& mapOperator) {

    //Fetch query signature of the child operator
    std::vector<NodePtr> children = mapOperator->getChildren();
    NES_ASSERT(children.size() == 1, "Map operator should only have one non null children.");
    auto child = children[0];
    auto childQuerySignature = child->as<LogicalOperatorNode>()->getZ3Signature();

    auto exprAndFieldMap = ExpressionToZ3ExprUtil::createForExpression(mapOperator->getMapExpression(), context);
    auto mapExpr = exprAndFieldMap->getExpr();
    auto rhsOperandFieldMap = exprAndFieldMap->getFieldMap();

    //Fetch the signature of only children and get the column values
    auto columns = childQuerySignature->getColumns();
    auto schemaFieldToExprMaps = childQuerySignature->getSchemaFieldToExprMaps();
    std::string fieldName = mapOperator->getMapExpression()->getField()->getFieldName();

    //Substitute rhs operands with actual values computed previously
    std::vector<std::map<std::string, z3::ExprPtr>> updatedSchemaFieldToExprMaps;
    for (auto& schemaFieldToExprMap : schemaFieldToExprMaps) {
        z3::ExprPtr updatedMapExpr = mapExpr;
        for (auto& [operandExprName, operandExpr] : rhsOperandFieldMap) {
            auto found = schemaFieldToExprMap.find(operandExprName);
            if (found == schemaFieldToExprMap.end()) {
                NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: " + operandExprName + " doesn't exists");
            }

            //Change from
            z3::expr_vector from(*context);
            from.push_back(*operandExpr);

            //Change to
            //Fetch the modified operand expression to be substituted
            auto derivedOperandExpr = found->second;
            z3::expr_vector to(*context);
            to.push_back(*derivedOperandExpr);

            //Perform replacement
            updatedMapExpr = std::make_shared<z3::expr>(updatedMapExpr->substitute(from, to));
        }
        schemaFieldToExprMap[fieldName] = updatedMapExpr;
        updatedSchemaFieldToExprMaps.emplace_back(schemaFieldToExprMap);
    }

    //Add field to the column list
    auto found = std::find(columns.begin(), columns.end(), fieldName);
    if (found == columns.end()) {
        columns.emplace_back(fieldName);
    }

    auto conditions = childQuerySignature->getConditions();
    auto windowsExpressions = childQuerySignature->getWindowsExpressions();
    return QuerySignature::create(std::move(conditions),
                                  std::move(columns),
                                  std::move(updatedSchemaFieldToExprMaps),
                                  std::move(windowsExpressions));
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForFilter(const z3::ContextPtr& context,
                                                                    const FilterLogicalOperatorNodePtr& filterOperator) {

    //Fetch query signature of the child operator
    std::vector<NodePtr> children = filterOperator->getChildren();
    NES_ASSERT(children.size() == 1, "Map operator should only have one non null children.");
    auto child = children[0];
    auto childQuerySignature = child->as<LogicalOperatorNode>()->getZ3Signature();

    auto filterExprAndFieldMap = ExpressionToZ3ExprUtil::createForExpression(filterOperator->getPredicate(), context);
    auto filterFieldMap = filterExprAndFieldMap->getFieldMap();
    auto filterExpr = filterExprAndFieldMap->getExpr();

    NES_TRACE("QuerySignatureUtil: Replace Z3 Expression for the filed with corresponding column values from "
              "children signatures");
    //Fetch the signature of only children and get the column values
    auto schemaFieldToExprMaps = childQuerySignature->getSchemaFieldToExprMaps();

    //Substitute rhs operands with actual values computed previously
    z3::expr_vector filterExpressions(*context);
    for (auto& schemaFieldToExprMap : schemaFieldToExprMaps) {
        z3::ExprPtr updatedExpr = filterExpr;
        for (auto& [operandExprName, operandExpr] : filterFieldMap) {
            auto found = schemaFieldToExprMap.find(operandExprName);
            if (found == schemaFieldToExprMap.end()) {
                NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: " + operandExprName + " doesn't exists");
            }

            //Change from
            z3::expr_vector from(*context);
            from.push_back(*operandExpr);

            //Change to
            //Fetch the modified operand expression to be substituted
            auto derivedOperandExpr = found->second;
            z3::expr_vector to(*context);
            to.push_back(*derivedOperandExpr);

            //Perform replacement
            updatedExpr = std::make_shared<z3::expr>(updatedExpr->substitute(from, to));
        }
        filterExpressions.push_back(*updatedExpr);
    }

    auto filterConditions = z3::mk_or(filterExpressions);

    //Compute a CNF condition using the children and filter conditions
    auto childConditions = childQuerySignature->getConditions();
    Z3_ast array[] = {filterConditions, *childConditions};
    auto conditions = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_and(*context, 2, array)));

    auto windowExpressions = childQuerySignature->getWindowsExpressions();
    auto columns = childQuerySignature->getColumns();

    return QuerySignature::create(std::move(conditions),
                                  std::move(columns),
                                  std::move(schemaFieldToExprMaps),
                                  std::move(windowExpressions));
}

QuerySignaturePtr
QuerySignatureUtil::createQuerySignatureForWatermark(const z3::ContextPtr& context,
                                                     const WatermarkAssignerLogicalOperatorNodePtr& watermarkAssignerOperator) {

    //Fetch query signature of the child operator
    std::vector<NodePtr> children = watermarkAssignerOperator->getChildren();
    NES_ASSERT(children.size() == 1, "Map operator should only have one non null children.");
    auto child = children[0];
    auto childQuerySignature = child->as<LogicalOperatorNode>()->getZ3Signature();

    auto conditions = childQuerySignature->getConditions();

    auto watermarkDescriptor = watermarkAssignerOperator->getWatermarkStrategyDescriptor();

    //Compute conditions based on watermark descriptor
    z3::expr watermarkDescriptorConditions(*context);
    if (watermarkDescriptor->instanceOf<Windowing::EventTimeWatermarkStrategyDescriptor>()) {
        auto eventTimeWatermarkStrategy = watermarkDescriptor->as<Windowing::EventTimeWatermarkStrategyDescriptor>();

        //Compute equal condition for allowed lateness
        auto allowedLatenessVar = context->int_const("allowedLateness");
        auto allowedLateness = eventTimeWatermarkStrategy->getAllowedLateness().getTime();
        auto allowedLatenessVal = context->int_val(allowedLateness);
        auto allowedLatenessExpr = to_expr(*context, Z3_mk_eq(*context, allowedLatenessVar, allowedLatenessVal));

        //Compute equality conditions for event time field
        auto eventTimeFieldVar = context->constant(context->str_symbol("eventTimeField"), context->string_sort());
        auto eventTimeFieldName =
            eventTimeWatermarkStrategy->getOnField()->as<FieldAccessExpressionNode>()->getFieldName();
        auto eventTimeFieldVal = context->string_val(eventTimeFieldName);
        auto eventTimeFieldExpr = to_expr(*context, Z3_mk_eq(*context, eventTimeFieldVar, eventTimeFieldVal));

        //CNF both conditions together to compute the descriptors condition
        Z3_ast andConditions[] = {allowedLatenessExpr, eventTimeFieldExpr};
        watermarkDescriptorConditions = to_expr(*context, Z3_mk_and(*context, 2, andConditions));
    } else if (watermarkDescriptor->instanceOf<Windowing::IngestionTimeWatermarkStrategyDescriptor>()) {
        //Create an equality expression <source>.watermarkAssignerType == "IngestionTime"
        auto var = context->constant(context->str_symbol("watermarkAssignerType"), context->string_sort());
        auto val = context->constant(context->str_symbol("IngestionTime"), context->string_sort());
        watermarkDescriptorConditions = to_expr(*context, Z3_mk_eq(*context, var, val));
    } else {
        NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Unrecognized watermark descriptor found.");
    }

    //CNF the watermark conditions to the original condition
    Z3_ast andConditions[] = {*conditions, watermarkDescriptorConditions};
    conditions = std::make_shared<z3::expr>(z3::to_expr(*context, Z3_mk_and(*context, 2, andConditions)));

    //Extract remaining signature attributes from child query signature
    auto windowExpressions = childQuerySignature->getWindowsExpressions();
    auto columns = childQuerySignature->getColumns();
    auto schemaFieldToExprMaps = childQuerySignature->getSchemaFieldToExprMaps();

    return QuerySignature::create(std::move(conditions),
                                  std::move(columns),
                                  std::move(schemaFieldToExprMaps),
                                  std::move(windowExpressions));
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForUnion(const z3::ContextPtr& context,
                                                                   const UnionLogicalOperatorNodePtr& unionOperator) {

    NES_DEBUG("QuerySignatureUtil: Computing Signature from children signatures");
    auto children = unionOperator->getChildren();
    auto leftSchema = unionOperator->getLeftInputSchema();

    //Identify the left and right schema
    QuerySignaturePtr leftSignature = children[0]->as<LogicalOperatorNode>()->getZ3Signature();
    QuerySignaturePtr rightSignature = children[1]->as<LogicalOperatorNode>()->getZ3Signature();

    //Compute a vector of different tuple schemas expected at this operator
    std::vector<std::map<std::string, z3::ExprPtr>> updatedSchemaFieldToExprMaps;
    //Fetch the Operator schemas and column names from left and right child
    std::vector<std::string> leftColumns = leftSignature->getColumns();
    std::vector<std::string> rightColumns = rightSignature->getColumns();
    auto leftSchemaFieldToExprMaps = leftSignature->getSchemaFieldToExprMaps();
    auto rightSchemaFieldToExprMaps = rightSignature->getSchemaFieldToExprMaps();

    //Compute Operator Tuple Schema Map
    //Copy all schemas from left child
    updatedSchemaFieldToExprMaps.insert(updatedSchemaFieldToExprMaps.end(),
                                        leftSchemaFieldToExprMaps.begin(),
                                        leftSchemaFieldToExprMaps.end());
    //Iterate over right children schemas and
    for (auto& rightSchemaFieldToExprMap : rightSchemaFieldToExprMaps) {
        std::map<std::string, z3::ExprPtr> updatedSchemaFieldToExprMap;
        for (uint32_t i = 0; i < leftColumns.size(); i++) {
            auto rightFieldName = rightColumns[i];
            auto rightExpr = rightSchemaFieldToExprMap[rightFieldName];
            updatedSchemaFieldToExprMap[leftColumns[i]] = rightExpr;
        }
        updatedSchemaFieldToExprMaps.emplace_back(updatedSchemaFieldToExprMap);
    }

    //Merge the window definitions together
    std::map<std::string, z3::ExprPtr> windowExpressions = leftSignature->getWindowsExpressions();
    for (const auto& [windowKey, windowExpression] : rightSignature->getWindowsExpressions()) {
        if (windowExpressions.find(windowKey) != windowExpressions.end()) {
            //FIXME: when we receive more than one window expressions for same window in issue #1272
            NES_NOT_IMPLEMENTED();
        } else {
            windowExpressions[windowKey] = windowExpression;
        }
    }

    //Add condition to the array
    z3::expr_vector allConditions(*context);
    allConditions.push_back(*leftSignature->getConditions());
    allConditions.push_back(*rightSignature->getConditions());

    //Create a CNF using all conditions from children signatures
    z3::ExprPtr conditions = std::make_shared<z3::expr>(z3::mk_and(allConditions));
    return QuerySignature::create(std::move(conditions),
                                  std::move(leftColumns),
                                  std::move(updatedSchemaFieldToExprMaps),
                                  std::move(windowExpressions));
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForJoin(const z3::ContextPtr& context,
                                                                  const JoinLogicalOperatorNodePtr& joinOperator) {

    //Compute intermediate signature by performing CNFs of all child signatures
    std::vector<NodePtr> children = joinOperator->getChildren();
    if (children.size() != 2) {
        NES_THROW_RUNTIME_ERROR("Join operator can have only 2 children. Found " + std::to_string(children.size()));
    }
    auto leftSchema = joinOperator->getLeftInputSchema();

    //Identify the left and right schema
    QuerySignaturePtr leftSignature;
    QuerySignaturePtr rightSignature;
    for (auto& child : children) {
        auto childOperator = child->as<LogicalOperatorNode>();
        if (childOperator->getOutputSchema()->equals(leftSchema)) {
            leftSignature = childOperator->getZ3Signature();
        } else {
            rightSignature = childOperator->getZ3Signature();
        }
    }

    //Find the left and right join key
    auto joinDefinition = joinOperator->getJoinDefinition();
    auto leftJoinKey = joinDefinition->getLeftJoinKey();
    auto rightJoinKey = joinDefinition->getRightJoinKey();
    auto leftKeyName = leftJoinKey->getFieldName();
    auto rightKeyName = rightJoinKey->getFieldName();

    //merge columns from both children
    std::vector<std::string> columns = leftSignature->getColumns();
    std::vector<std::string> rightColumns = rightSignature->getColumns();
    columns.insert(columns.end(), rightColumns.begin(), rightColumns.end());

    //Merge the Operator Tuple Schemas
    std::vector<std::map<std::string, z3::ExprPtr>> updatedSchemaFieldToExprMaps;
    auto leftSchemaFieldToExprMaps = leftSignature->getSchemaFieldToExprMaps();
    auto rightSchemaFieldToExprMaps = rightSignature->getSchemaFieldToExprMaps();

    //Iterate over all left and right schemas and compute new join predicates and schemas
    z3::expr_vector joinPredicates(*context);
    for (auto& leftSchemaMap : leftSchemaFieldToExprMaps) {
        //Iterate over all schemas from right children
        for (auto& rightSchemaMap : rightSchemaFieldToExprMaps) {
            //Compute the new field to z3 expression map by inserting all fields from left and right children
            std::map<std::string, z3::ExprPtr> updatedFieldToZ3ExprMap;
            updatedFieldToZ3ExprMap.insert(leftSchemaMap.begin(), leftSchemaMap.end());
            updatedFieldToZ3ExprMap.insert(rightSchemaMap.begin(), rightSchemaMap.end());
            //
            auto leftPredicate = updatedFieldToZ3ExprMap[leftKeyName];
            auto rightPredicate = updatedFieldToZ3ExprMap[rightKeyName];
            auto joinPredicate = z3::to_expr(*context, Z3_mk_eq(*context, *leftPredicate, *rightPredicate));
            joinPredicates.push_back(joinPredicate);
            updatedSchemaFieldToExprMaps.emplace_back(updatedFieldToZ3ExprMap);
        }
    }

    auto joinCondition = z3::mk_or(joinPredicates);

    //CNF join predicates and conditions from both children
    Z3_ast andConditions[] = {*leftSignature->getConditions(), *rightSignature->getConditions(), joinCondition};
    auto conditions =
        std::make_shared<z3::expr>(z3::to_expr(*context, z3::to_expr(*context, Z3_mk_and(*context, 3, andConditions))));

    //Compute the expression for window time key
    auto windowType = joinDefinition->getWindowType();
    auto timeCharacteristic = windowType->getTimeCharacteristic();
    //FIXME: problem is that only one time key is defined during the join definition
    // fix it as part of #1592
    //    z3::expr windowTimeKeyVal(*context);
    //    Windowing::TimeCharacteristic::Type type = timeCharacteristic->getType();
    //    if (type == Windowing::TimeCharacteristic::EventTime) {
    //        windowTimeKeyVal = context->string_val(timeCharacteristic->getField()->getName());
    //    } else if (type == Windowing::TimeCharacteristic::IngestionTime) {
    //        windowTimeKeyVal = context->string_val(timeCharacteristic->getField()->getName());
    //    } else {
    //        NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Unknown window Time Characteristic");
    //    }
    //    auto windowTimeKeyVar = context->constant(context->str_symbol("time-key"), context->string_sort());
    //    auto windowTimeKeyExpression = to_expr(*context, Z3_mk_eq(*context, windowTimeKeyVar, windowTimeKeyVal));

    //Compute the expression for window size and slide
    auto multiplier = timeCharacteristic->getTimeUnit().getMultiplier();
    uint64_t length = 0;
    uint64_t slide = 0;
    if (windowType->isTumblingWindow()) {
        auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType);
        length = tumblingWindow->getSize().getTime() * multiplier;
        slide = length;
    } else if (windowType->isSlidingWindow()) {
        auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType);
        length = slidingWindow->getSize().getTime() * multiplier;
        slide = slidingWindow->getSlide().getTime() * multiplier;
    } else {
        NES_ERROR("QuerySignatureUtil: Cant serialize window Time Type");
    }
    auto windowTimeSizeVar = context->int_const("window-time-size");
    z3::expr windowTimeSizeVal = context->int_val(length);
    auto windowTimeSlideVar = context->int_const("window-time-slide");
    z3::expr windowTimeSlideVal = context->int_val(slide);
    auto windowTimeSizeExpression = to_expr(*context, Z3_mk_eq(*context, windowTimeSizeVar, windowTimeSizeVal));
    auto windowTimeSlideExpression = to_expr(*context, Z3_mk_eq(*context, windowTimeSlideVar, windowTimeSlideVal));

    //Compute join window key expression
    auto windowKeyVar = context->constant(context->str_symbol("window-key"), context->string_sort());
    std::string windowKey = "JoinWindow";
    z3::expr windowKeyVal = context->string_val(windowKey);
    auto windowKeyExpression = to_expr(*context, Z3_mk_eq(*context, windowKeyVar, windowKeyVal));

    //Compute the CNF based on the window-key, window-size, and window-slide
    Z3_ast expressionArray[] = {windowKeyExpression, windowTimeSlideExpression, windowTimeSizeExpression};

    std::map<std::string, z3::ExprPtr> windowExpressions = leftSignature->getWindowsExpressions();

    for (const auto& [rightWindowKey, rightWindowExpr] : rightSignature->getWindowsExpressions()) {
        if (windowExpressions.find(rightWindowKey) == windowExpressions.end()) {
            windowExpressions[rightWindowKey] = rightWindowExpr;
        } else {
            //TODO: as part of #1377
            NES_NOT_IMPLEMENTED();
        }
    }

    if (windowExpressions.find(windowKey) == windowExpressions.end()) {
        windowExpressions[windowKey] = std::make_shared<z3::expr>(z3::to_expr(*context, Z3_mk_and(*context, 3, expressionArray)));
    } else {
        //TODO: as part of #1377
        NES_NOT_IMPLEMENTED();
    }

    return QuerySignature::create(std::move(conditions),
                                  std::move(columns),
                                  std::move(updatedSchemaFieldToExprMaps),
                                  std::move(windowExpressions));
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForWindow(const z3::ContextPtr& context,
                                                                    const WindowLogicalOperatorNodePtr& windowOperator) {

    //Fetch query signature of the child operator
    std::vector<NodePtr> children = windowOperator->getChildren();
    NES_ASSERT(children.size() == 1 && children[0], "Window operator should only have one non null children.");
    auto child = children[0];
    auto childQuerySignature = child->as<LogicalOperatorNode>()->getZ3Signature();

    NES_DEBUG("QuerySignatureUtil: compute signature for window operator");
    z3::expr_vector windowConditions(*context);

    auto windowDefinition = windowOperator->getWindowDefinition();

    //Compute the expression for window key
    std::string windowKey;
    if (windowDefinition->isKeyed()) {
        FieldAccessExpressionNodePtr key = windowDefinition->getOnKey();
        windowKey = key->getFieldName();
    } else {
        windowKey = "non-keyed";
    }
    auto windowKeyVar = context->constant(context->str_symbol("window-key"), context->string_sort());
    z3::expr windowKeyVal = context->string_val(windowKey);
    auto windowKeyExpression = to_expr(*context, Z3_mk_eq(*context, windowKeyVar, windowKeyVal));

    //Compute the expression for window time key
    auto windowType = windowDefinition->getWindowType();
    auto timeCharacteristic = windowType->getTimeCharacteristic();
    z3::expr windowTimeKeyVal(*context);
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::EventTime) {
        windowTimeKeyVal = context->string_val(timeCharacteristic->getField()->getName());
    } else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        windowTimeKeyVal = context->string_val(timeCharacteristic->getField()->getName());
    } else {
        NES_ERROR("QuerySignatureUtil: Cant serialize window Time Characteristic");
    }
    auto windowTimeKeyVar = context->constant(context->str_symbol("time-key"), context->string_sort());
    auto windowTimeKeyExpression = to_expr(*context, Z3_mk_eq(*context, windowTimeKeyVar, windowTimeKeyVal));

    //Compute the expression for window size and slide
    auto multiplier = timeCharacteristic->getTimeUnit().getMultiplier();
    uint64_t length = 0;
    uint64_t slide = 0;
    if (windowType->isTumblingWindow()) {
        auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType);
        length = tumblingWindow->getSize().getTime() * multiplier;
        slide = length;
    } else if (windowType->isSlidingWindow()) {
        auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType);
        length = slidingWindow->getSize().getTime() * multiplier;
        slide = slidingWindow->getSlide().getTime() * multiplier;
    } else {
        NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Unknown window Time Characteristic");
    }
    auto windowTimeSizeVar = context->int_const("window-time-size");
    z3::expr windowTimeSizeVal = context->int_val(length);
    auto windowTimeSlideVar = context->int_const("window-time-slide");
    z3::expr windowTimeSlideVal = context->int_val(slide);
    auto windowTimeSizeExpression = to_expr(*context, Z3_mk_eq(*context, windowTimeSizeVar, windowTimeSizeVal));
    auto windowTimeSlideExpression = to_expr(*context, Z3_mk_eq(*context, windowTimeSlideVar, windowTimeSlideVal));

    //FIXME: when count based window is implemented #1383
    //    auto windowCountSizeVar = context->int_const("window-count-size");

    //Compute the CNF based on the window-key, window-time-key, window-size, and window-slide
    Z3_ast expressionArray[] = {windowKeyExpression,
                                windowTimeKeyExpression,
                                windowTimeSlideExpression,
                                windowTimeSizeExpression};
    auto windowExpressions = childQuerySignature->getWindowsExpressions();
    if (windowExpressions.find(windowKey) == windowExpressions.end()) {
        windowExpressions[windowKey] = std::make_shared<z3::expr>(z3::to_expr(*context, Z3_mk_and(*context, 4, expressionArray)));
    } else {
        //TODO: as part of #1377
        NES_NOT_IMPLEMENTED();
    }

    //FIXME: change the logic here as part of #1377
    //Compute expression for aggregation method
    z3::func_decl aggregate(*context);
    z3::sort sort = context->int_sort();
    auto windowAggregation = windowDefinition->getWindowAggregation();
    switch (windowAggregation->getType()) {
        case Windowing::WindowAggregationDescriptor::Count: {
            aggregate = z3::function("Count", sort, sort);
            break;
        }
        case Windowing::WindowAggregationDescriptor::Max: {
            aggregate = z3::function("Max", sort, sort);
            break;
        }
        case Windowing::WindowAggregationDescriptor::Min: {
            aggregate = z3::function("Min", sort, sort);
            break;
        }
        case Windowing::WindowAggregationDescriptor::Sum: {
            aggregate = z3::function("Sum", sort, sort);
            break;
        }
        case Windowing::WindowAggregationDescriptor::Avg: {
            aggregate = z3::function("Avg", sort, sort);
            break;
        }
        default: NES_FATAL_ERROR("QuerySignatureUtil: could not cast aggregation type");
    }

    // Get the expression for on field and update the column values
    auto onField = windowAggregation->on();
    auto onFieldName = onField->as<FieldAccessExpressionNode>()->getFieldName();
    auto asField = windowAggregation->as();
    auto asFieldName = asField->as<FieldAccessExpressionNode>()->getFieldName();

    auto schemaFieldToExprMaps = childQuerySignature->getSchemaFieldToExprMaps();
    auto outputSchema = windowOperator->getOutputSchema();

    //Compute new schemas for this operator
    std::vector<std::map<std::string, z3::ExprPtr>> updatedSchemaFieldToExprMaps;
    //Iterate over all child schemas
    for (auto& schemaFieldToExprMap : schemaFieldToExprMaps) {
        std::map<std::string, z3::ExprPtr> updatedSchemaMap;
        for (auto& outputField : outputSchema->fields) {
            auto originalAttributeName = outputField->getName();
            if (originalAttributeName.find("start") != std::string ::npos
                || originalAttributeName.find("end") != std::string::npos
                || originalAttributeName.find("cnt") != std::string::npos) {
                updatedSchemaMap[originalAttributeName] =
                    DataTypeToZ3ExprUtil::createForField(originalAttributeName, outputField->getDataType(), context)->getExpr();
            } else if (originalAttributeName == asFieldName) {
                auto fieldExpr = schemaFieldToExprMap[onFieldName];
                auto updatedFieldExpr = std::make_shared<z3::expr>(z3::to_expr(*context, aggregate(*fieldExpr)));
                updatedSchemaMap[originalAttributeName] = updatedFieldExpr;
            } else {
                updatedSchemaMap[originalAttributeName] = schemaFieldToExprMap[originalAttributeName];
            }
        }
        updatedSchemaFieldToExprMaps.emplace_back(updatedSchemaMap);
    }

    std::vector<std::string> columns;
    for (auto& outputField : outputSchema->fields) {
        columns.emplace_back(outputField->getName());
    }
    auto conditions = childQuerySignature->getConditions();
    return QuerySignature::create(std::move(conditions),
                                  std::move(columns),
                                  std::move(updatedSchemaFieldToExprMaps),
                                  std::move(windowExpressions));
}
}// namespace NES::Optimizer
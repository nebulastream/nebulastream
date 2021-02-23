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
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/RenameStreamOperatorNode.hpp>
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
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <z3++.h>

namespace NES::Optimizer {

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForOperator(z3::ContextPtr context, OperatorNodePtr operatorNode) {

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

    SchemaPtr outputSchema = operatorNode->getOutputSchema();

    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for Source operator");
        SourceLogicalOperatorNodePtr sourceOperator = operatorNode->as<SourceLogicalOperatorNode>();

        //Compute the column expressions for the source
        std::map<std::string, std::vector<z3::ExprPtr>> columns;
        for (auto& field : outputSchema->fields) {
            auto attributeName = field->getName();
            auto column = DataTypeToZ3ExprUtil::createForField(attributeName, field->getDataType(), context)->getExpr();
            columns[attributeName] = {column};
        }

        //Create an equality expression for example: <logical stream name>.streamName == "<logical stream name>"
        std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();
        auto streamNameVarName = streamName + ".streamName";
        auto streamNameVar = context->constant(context->str_symbol(streamNameVarName.c_str()), context->string_sort());
        auto streamNameVal = context->string_val(streamName);
        auto conditions = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_eq(*context, streamNameVar, streamNameVal)));
        return QuerySignature::create(conditions, columns, {});
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for Sink operator");
        return buildQuerySignatureForChildren(context, children);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for filter operator");
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        return createQuerySignatureForFilter(context, filterOperator);
    } else if (operatorNode->instanceOf<UnionLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for Merge operator");
        auto unionOperator = operatorNode->as<UnionLogicalOperatorNode>();
        return createQuerySignatureForUnion(context, unionOperator);
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for Broadcast operator");
        return buildQuerySignatureForChildren(context, children);
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

        NES_ASSERT(children.size() == 1 && children[0], "Project operator should only have one non null children.");
        auto childQuerySignature = children[0]->as<LogicalOperatorNode>()->getSignature();

        //Extract projected columns
        auto columns = childQuerySignature->getColumns();
        std::map<std::string, std::vector<z3::ExprPtr>> updatedColumns;
        for (auto& field : outputSchema->fields) {
            auto attributeName = field->getName();
            if (columns.find(attributeName) == columns.end()) {
                NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Unable to find projected attribute " + attributeName
                                        + " in children column set.");
            }
            updatedColumns[attributeName] = columns[attributeName];
        }

        auto conditions = childQuerySignature->getConditions();
        auto windowExpressions = childQuerySignature->getWindowsExpressions();
        return QuerySignature::create(conditions, updatedColumns, windowExpressions);
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for watermark operator");
        auto watermarkAssignerOperator = operatorNode->as<WatermarkAssignerLogicalOperatorNode>();
        return createQuerySignatureForWatermark(context, watermarkAssignerOperator);
    } else if (operatorNode->instanceOf<JoinLogicalOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for join operator");
        auto joinOperator = operatorNode->as<JoinLogicalOperatorNode>();
        return createQuerySignatureForJoin(context, joinOperator);
    } else if (operatorNode->instanceOf<RenameStreamOperatorNode>()) {
        NES_TRACE("QuerySignatureUtil: Computing Signature for rename stream operator");
        auto renameStreamOperator = operatorNode->as<RenameStreamOperatorNode>();
        return createQuerySignatureForRenameStream(context, renameStreamOperator);
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for operator: " + operatorNode->toString());
}

QuerySignaturePtr QuerySignatureUtil::buildQuerySignatureForChildren(z3::ContextPtr context, std::vector<NodePtr> children) {

    NES_DEBUG("QuerySignatureUtil: Computing Signature from children signatures");
    z3::expr_vector allConditions(*context);
    std::map<std::string, std::vector<z3::ExprPtr>> columns;
    std::map<std::string, z3::ExprPtr> windowExpressions;

    //Iterate over all children query signatures for computing the column values
    for (auto& child : children) {

        //Fetch signature of the child operator
        auto childSignature = child->as<LogicalOperatorNode>()->getSignature();

        /* if (sources.empty()) {
            sources = childSignature->getSources();
        } else {
            auto childSources = childSignature->getSources();

            //Check if sources and sources from child signature have common stream source name
            // This is done to prevent from creating conflicting attribute names
            std::vector<std::string> commonSources;
            std::sort(sources.begin(), sources.end());
            std::sort(childSources.begin(), childSources.end());
            //We use std intersection api to compute intersection between two vectors
            std::set_intersection(sources.begin(), sources.end(), childSources.begin(), childSources.end(),
                                  back_inserter(commonSources));

            if (!commonSources.empty()) {
                NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Can not compute signature for query with children upstreams based "
                                        "on source with same logical name");
            }
            sources.insert(sources.end(), childSources.begin(), childSources.end());
        }*/

        //Merge the attribute map together
        /* if (attributeMap.empty()) {
            attributeMap = childSignature->getAttributeMap();
        } else {
            for (auto [originalAttributeName, derivedAttributeNames] : childSignature->getAttributeMap()) {
                if (attributeMap.find(originalAttributeName) == attributeMap.end()) {
                    attributeMap[originalAttributeName] = derivedAttributeNames;
                } else {
                    auto existingDerivedAttributes = attributeMap[originalAttributeName];
                    existingDerivedAttributes.insert(existingDerivedAttributes.end(), derivedAttributeNames.begin(),
                                                     derivedAttributeNames.end());
                    attributeMap[originalAttributeName] = existingDerivedAttributes;
                }
            }
        }*/

        //Merge the columns from different children signatures together
        if (columns.empty()) {
            columns = childSignature->getColumns();
        } else {
            columns.merge(childSignature->getColumns());
        }

        //Merge the window definitions together
        if (windowExpressions.empty()) {
            windowExpressions = childSignature->getWindowsExpressions();
        } else {
            for (auto [windowKey, windowExpression] : childSignature->getWindowsExpressions()) {
                if (windowExpressions.find(windowKey) != windowExpressions.end()) {
                    //FIXME: when we receive more than one window expressions for same window in issue #1272
                    NES_NOT_IMPLEMENTED();
                } else {
                    windowExpressions[windowKey] = windowExpression;
                }
            }
        }

        //Add condition to the array
        allConditions.push_back(*childSignature->getConditions());
    }

    //Create a CNF using all conditions from children signatures
    z3::ExprPtr conditions = std::make_shared<z3::expr>(z3::mk_and(allConditions));
    return QuerySignature::create(conditions, columns, windowExpressions);
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForWindow(z3::ContextPtr context,
                                                                    WindowLogicalOperatorNodePtr windowOperator) {

    //Fetch query signature of the child operator
    std::vector<NodePtr> children = windowOperator->getChildren();
    NES_ASSERT(children.size() == 1 && children[0], "Map operator should only have one non null children.");
    auto child = children[0];
    auto childQuerySignature = child->as<LogicalOperatorNode>()->getSignature();

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
    uint64_t length;
    uint64_t slide;
    if (windowType->isTumblingWindow()) {
        auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType);
        length = tumblingWindow->getSize().getTime() * multiplier;
        slide = tumblingWindow->getSize().getTime() * multiplier;
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

    //FIXME: when count based window is implemented
    //    auto windowCountSizeVar = context->int_const("window-count-size");

    //Compute the CNF based on the window-key, window-time-key, window-size, and window-slide
    Z3_ast expressionArray[] = {windowKeyExpression, windowTimeKeyExpression, windowTimeSlideExpression,
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

    auto columns = childQuerySignature->getColumns();

    if (columns.find(onFieldName) == columns.end()) {
        NES_THROW_RUNTIME_ERROR("Can find attribute " + onFieldName);
    }

    auto onFieldExpression = columns[onFieldName];
    columns[asFieldName] = {std::make_shared<z3::expr>(z3::to_expr(*context, aggregate(*onFieldExpression)))};

    std::map<std::string, std::vector<z3::ExprPtr>> updatedColumns;
    auto outputSchema = windowOperator->getOutputSchema();
    for (auto& field : outputSchema->fields) {
        auto originalAttributeName = field->getName();
        if (columns.find(originalAttributeName) == columns.end()) {
            if (originalAttributeName == "start" || originalAttributeName == "end") {
                updatedColumns[originalAttributeName] = {
                    DataTypeToZ3ExprUtil::createForField(originalAttributeName, field->getDataType(), context)->getExpr()};
            }
        } else {
            updatedColumns[originalAttributeName] = columns[originalAttributeName];
        }
    }

    return QuerySignature::create(childQuerySignature->getConditions(), updatedColumns, windowExpressions);
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForMap(z3::ContextPtr context, MapLogicalOperatorNodePtr mapOperator) {

    //Fetch query signature of the child operator
    std::vector<NodePtr> children = mapOperator->getChildren();
    NES_ASSERT(children.size() == 1 && children[0], "Map operator should only have one non null children.");
    auto child = children[0];
    auto childQuerySignature = child->as<LogicalOperatorNode>()->getSignature();

    auto exprAndFieldMap = ExpressionToZ3ExprUtil::createForExpression(mapOperator->getMapExpression(), context);
    auto expr = exprAndFieldMap->getExpr();
    auto rhsOperandFieldMap = exprAndFieldMap->getFieldMap();

    //Fetch the signature of only children and get the column values
    auto columns = childQuerySignature->getColumns();

    std::string fieldName = mapOperator->getMapExpression()->getField()->getFieldName();

    //Find if the LHS operand is a new attribute or not
    bool isNewAttribute = true;
    auto inputSchema = mapOperator->getInputSchema();
    for (auto inputField : inputSchema->fields) {
        if (inputField->getName() == fieldName) {
            isNewAttribute = false;
            break;
        }
    }

    //Substitute rhs operands with actual values computed previously
    //    for (auto source : sources) {
    //
    //        //Compute the derived attribute name
    //        auto derivedAttributeName = fieldName;
    //
    //        //Add the derived attribute to the attribute map
    //        if (isNewAttribute) {
    //            //Add the newly derived attribute to the attribute map
    //            if (attributeMap.find(fieldName) == attributeMap.end()) {
    //                attributeMap[fieldName] = {derivedAttributeName};
    //            } else {
    //                //Add the newly derived attribute to attribute map
    //                auto derivedAttributes = attributeMap[fieldName];
    //                derivedAttributes.push_back(derivedAttributeName);
    //                attributeMap[fieldName] = derivedAttributes;
    //            }
    //        } else if (!isNewAttribute && columns.find(derivedAttributeName) == columns.end()) {
    //            // this attribute doesn't exists in the stream source and hence should not be created
    //            continue;
    //        }

    z3::ExprPtr updatedExpr = substituteIntoInputExpression(context, expr, rhsOperandFieldMap, columns);
    columns[fieldName] = {updatedExpr};
    //    }

    //Prepare all combinations of col expression for the given map by substituting the previous col values in the assignment expression
    std::vector<z3::ExprPtr> allColExprForMap{expr};

    return QuerySignature::create(childQuerySignature->getConditions(), columns, childQuerySignature->getWindowsExpressions());
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForFilter(z3::ContextPtr context,
                                                                    FilterLogicalOperatorNodePtr filterOperator) {

    //Fetch query signature of the child operator
    std::vector<NodePtr> children = filterOperator->getChildren();
    NES_ASSERT(children.size() == 1 && children[0], "Map operator should only have one non null children.");
    auto child = children[0];
    auto childQuerySignature = child->as<LogicalOperatorNode>()->getSignature();

    auto filterExprAndFieldMap = ExpressionToZ3ExprUtil::createForExpression(filterOperator->getPredicate(), context);
    auto filterFieldMap = filterExprAndFieldMap->getFieldMap();
    auto filterExpr = filterExprAndFieldMap->getExpr();

    NES_TRACE("QuerySignatureUtil: Replace Z3 Expression for the filed with corresponding column values from "
              "children signatures");
    //As filter can't have more than 1 children so fetch the only child signature
    auto columns = childQuerySignature->getColumns();
    //    auto sources = childQuerySignature->getSources();

    z3::expr_vector filterExpressions(*context);
    //    for (auto source : sources) {
    auto updatedExpression = substituteIntoInputExpression(context, filterExpr, filterFieldMap, columns);
    filterExpressions.push_back(*updatedExpression);
    //    }

    //Compute a DNF condition for all different conditions identified by substituting the col values
    auto filterConditions = z3::mk_or(filterExpressions);

    //Compute a CNF condition using the children and filter conditions
    auto childConditions = childQuerySignature->getConditions();
    Z3_ast array[] = {filterConditions, *childConditions};
    auto conditions = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_and(*context, 2, array)));
    return QuerySignature::create(conditions, columns, childQuerySignature->getWindowsExpressions());
}

QuerySignaturePtr
QuerySignatureUtil::createQuerySignatureForWatermark(z3::ContextPtr context,
                                                     WatermarkAssignerLogicalOperatorNodePtr watermarkOperator) {

    //Fetch query signature of the child operator
    std::vector<NodePtr> children = watermarkOperator->getChildren();
    NES_ASSERT(children.size() == 1 && children[0], "Map operator should only have one non null children.");
    auto child = children[0];
    auto childQuerySignature = child->as<LogicalOperatorNode>()->getSignature();

    auto conditions = childQuerySignature->getConditions();

    auto watermarkDescriptor = watermarkOperator->getWatermarkStrategyDescriptor();

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
            eventTimeWatermarkStrategy->getOnField().getExpressionNode()->as<FieldAccessExpressionNode>()->getFieldName();
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
    //    auto attributeMap = childQuerySignature->getAttributeMap();
    auto windowExpressions = childQuerySignature->getWindowsExpressions();
    auto columns = childQuerySignature->getColumns();

    return QuerySignature::create(conditions, columns, windowExpressions);
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForJoin(z3::ContextPtr context,
                                                                  JoinLogicalOperatorNodePtr joinOperator) {

    //Compute intermediate signature by performing CNFs of all child signatures
    std::vector<NodePtr> children = joinOperator->getChildren();
    if (children.size() != 2) {
        NES_THROW_RUNTIME_ERROR("Join operator can have only 2 children. Found " + std::to_string(children.size()));
    }
    auto intermediateQuerySignature = buildQuerySignatureForChildren(context, children);

    auto conditions = intermediateQuerySignature->getConditions();
    //    auto attributeMap = intermediateQuerySignature->getAttributeMap();
    auto columns = intermediateQuerySignature->getColumns();

    //Find the left and right join key
    auto joinDefinition = joinOperator->getJoinDefinition();
    auto leftJoinKey = joinDefinition->getLeftJoinKey();
    auto rightJoinKey = joinDefinition->getRightJoinKey();

    //Find the left key expression
    auto leftChild = children[0]->as<LogicalOperatorNode>();
    auto leftKeyName = leftJoinKey->getFieldName();

    z3::ExprPtr leftKeyExpr;
    //Iterate over left source names and try to find the expression for left join key

    if (columns.find(leftKeyName) == columns.end()) {
        NES_THROW_RUNTIME_ERROR("Unexpected behaviour! Left join key " + leftKeyName + " does not exists in the attribute map.");
    }

    if (columns.find(leftKeyName) != columns.end()) {
        leftKeyExpr = columns[leftKeyName];
    }

    //Find the right key expression
    auto rightChild = children[1]->as<LogicalOperatorNode>();
    auto rightKeyName = rightJoinKey->getFieldName();

    z3::ExprPtr rightKeyExpr;
    if (columns.find(rightKeyName) == columns.end()) {
        NES_THROW_RUNTIME_ERROR("Unexpected behaviour! Right join key " + rightKeyName
                                + " does not exists in the attribute map.");
    }

    //Find the expression for the right join key expected attribute name
    if (columns.find(rightKeyName) != columns.end()) {
        rightKeyExpr = columns[rightKeyName];
    }

    NES_ASSERT(leftKeyExpr && rightKeyExpr, "Unexpected behaviour! Unable to find right or left join key ");

    //Compute the equi join condition
    auto joinCondition = z3::to_expr(*context, Z3_mk_eq(*context, *leftKeyExpr, *rightKeyExpr));

    //CNF the watermark conditions to the original condition
    Z3_ast andConditions[] = {*conditions, joinCondition};
    conditions = std::make_shared<z3::expr>(z3::to_expr(*context, Z3_mk_and(*context, 2, andConditions)));

    //Compute the expression for window time key
    auto windowType = joinDefinition->getWindowType();
    auto timeCharacteristic = windowType->getTimeCharacteristic();
    z3::expr windowTimeKeyVal(*context);
    Windowing::TimeCharacteristic::Type type = timeCharacteristic->getType();
    if (type == Windowing::TimeCharacteristic::EventTime) {
        windowTimeKeyVal = context->string_val(timeCharacteristic->getField()->getName());
    } else if (type == Windowing::TimeCharacteristic::IngestionTime) {
        windowTimeKeyVal = context->string_val(timeCharacteristic->getField()->getName());
    } else {
        NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Unknown window Time Characteristic");
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
        NES_ERROR("QuerySignatureUtil: Cant serialize window Time Type");
    }
    auto windowTimeSizeVar = context->int_const("window-time-size");
    z3::expr windowTimeSizeVal = context->int_val(length);
    auto windowTimeSlideVar = context->int_const("window-time-slide");
    z3::expr windowTimeSlideVal = context->int_val(slide);
    auto windowTimeSizeExpression = to_expr(*context, Z3_mk_eq(*context, windowTimeSizeVar, windowTimeSizeVal));
    auto windowTimeSlideExpression = to_expr(*context, Z3_mk_eq(*context, windowTimeSlideVar, windowTimeSlideVal));
    auto windowExpressions = intermediateQuerySignature->getWindowsExpressions();

    //Compute join window key expression
    auto windowKeyVar = context->constant(context->str_symbol("window-key"), context->string_sort());
    std::string windowKey = "JoinWindow";
    z3::expr windowKeyVal = context->string_val(windowKey);
    auto windowKeyExpression = to_expr(*context, Z3_mk_eq(*context, windowKeyVar, windowKeyVal));

    //Compute the CNF based on the window-key, window-time-key, window-size, and window-slide
    Z3_ast expressionArray[] = {windowKeyExpression, windowTimeKeyExpression, windowTimeSlideExpression,
                                windowTimeSizeExpression};

    if (windowExpressions.find(windowKey) == windowExpressions.end()) {
        windowExpressions[windowKey] = std::make_shared<z3::expr>(z3::to_expr(*context, Z3_mk_and(*context, 4, expressionArray)));
    } else {
        //TODO: as part of #1377
        NES_NOT_IMPLEMENTED();
    }
    //    auto sources = intermediateQuerySignature->getSources();
    return QuerySignature::create(conditions, columns, windowExpressions);
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForUnion(z3::ContextPtr context,
                                                                   UnionLogicalOperatorNodePtr unionOperator) {

    NES_DEBUG("QuerySignatureUtil: Computing Signature from children signatures");

    auto children = unionOperator->getChildren();
    auto leftSchema = unionOperator->getLeftInputSchema();
    auto rightSchema = unionOperator->getRightInputSchema();

    QuerySignaturePtr leftSignature;
    QuerySignaturePtr rightSignature;
    for (auto& child : children) {
        auto childOperator = child->as<LogicalOperatorNode>();
        if (childOperator->getOutputSchema()->equals(leftSchema)) {
            leftSignature = childOperator->getSignature();
        } else {
            rightSignature = childOperator->getSignature();
        }
    }

    std::map<std::string, std::vector<z3::ExprPtr>> columns = leftSignature->getColumns();
    std::map<std::string, z3::ExprPtr> windowExpressions = leftSignature->getWindowsExpressions();
    //    std::map<std::string, std::vector<std::string>> attributeMap = leftSignature->getAttributeMap();
    //    std::vector<std::string> sources = leftSignature->getSources();

    //Fetch signature of the child operator
    //    auto childSignature = child->as<LogicalOperatorNode>()->getSignature();
    //
    //    if (sources.empty()) {
    //        sources = childSignature->getSources();
    //    } else {
    //        auto childSources = childSignature->getSources();
    //
    //        //Check if sources and sources from child signature have common stream source name
    //        // This is done to prevent from creating conflicting attribute names
    //        std::vector<std::string> commonSources;
    //        std::sort(sources.begin(), sources.end());
    //        std::sort(childSources.begin(), childSources.end());
    //        //We use std intersection api to compute intersection between two vectors
    //        std::set_intersection(sources.begin(), sources.end(), childSources.begin(), childSources.end(),
    //                              back_inserter(commonSources));
    //
    //        if (!commonSources.empty()) {
    //            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Can not compute signature for query with children upstreams based "
    //                                    "on source with same logical name");
    //        }
    //        sources.insert(sources.end(), childSources.begin(), childSources.end());
    //    }

    //    auto leftAttributeMap = leftSignature->getAttributeMap();
    //    auto rightAttributeMap = rightSignature->getAttributeMap();
    //
    //    for (uint32_t i = 0; i < leftSchema->getSize(); i++) {
    //        auto leftAttribute = leftSchema->get(i)->getName();
    //        auto rightAttribute = rightSchema->get(i)->getName();
    //        z3::expr_vector allConditions(*context);
    //         leftAttributeMap[leftAttribute];
    //    }

    //    for (auto [originalAttributeName, derivedAttributeNames] : childSignature->getAttributeMap()) {
    //        if (attributeMap.find(originalAttributeName) == attributeMap.end()) {
    //            attributeMap[originalAttributeName] = derivedAttributeNames;
    //        } else {
    //            auto existingDerivedAttributes = attributeMap[originalAttributeName];
    //            existingDerivedAttributes.insert(existingDerivedAttributes.end(), derivedAttributeNames.begin(),
    //                                             derivedAttributeNames.end());
    //            attributeMap[originalAttributeName] = existingDerivedAttributes;
    //        }
    //    }
    //
    //    //Merge the columns from different children signatures together
    //    if (columns.empty()) {
    //        columns = childSignature->getColumns();
    //    } else {
    //        columns.merge(childSignature->getColumns());
    //    }

    //Merge the window definitions together
    for (auto [windowKey, windowExpression] : rightSignature->getWindowsExpressions()) {
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
    return QuerySignature::create(conditions, columns, windowExpressions);
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForRenameStream(z3::ContextPtr, RenameStreamOperatorNodePtr) {

    /*    //Fetch query signature of the child operator
    std::vector<NodePtr> children = renameStreamOperator->getChildren();
    NES_ASSERT(children.size() == 1 && children[0], "RenameStream operator should only have one non null children.");
    auto child = children[0];
    auto childQuerySignature = child->as<LogicalOperatorNode>()->getSignature();

    auto conditions = childQuerySignature->getConditions();

    //Find the source name
    auto sources = childQuerySignature->getSources();
    NES_ASSERT(sources.size() == 1 && !sources[0].empty(),
               "QuerySignatureUtil: Watermark assigner operator can have only 1 non empty source");
    auto source = sources[0];
    auto newStreamName = renameStreamOperator->getNewStreamName();

    //Compute conditions based on watermark descriptor
    z3::expr watermarkDescriptorConditions(*context);
    if (watermarkDescriptor->instanceOf<Windowing::EventTimeWatermarkStrategyDescriptor>()) {
        auto eventTimeWatermarkStrategy = watermarkDescriptor->as<Windowing::EventTimeWatermarkStrategyDescriptor>();

        //Compute equal condition for allowed lateness
        auto allowedLatenessVarName = source + ".allowedLateness";
        auto allowedLatenessVar = context->int_const(allowedLatenessVarName.c_str());
        auto allowedLateness = eventTimeWatermarkStrategy->getAllowedLateness().getTime();
        auto allowedLatenessVal = context->int_val(allowedLateness);
        auto allowedLatenessExpr = to_expr(*context, Z3_mk_eq(*context, allowedLatenessVar, allowedLatenessVal));

        //Compute equality conditions for event time field
        auto eventTimeFieldVarName = source + ".eventTimeField";
        auto eventTimeFieldVar = context->constant(context->str_symbol(eventTimeFieldVarName.c_str()), context->string_sort());
        auto eventTimeFieldName = source + "."
            + eventTimeWatermarkStrategy->getOnField().getExpressionNode()->as<FieldAccessExpressionNode>()->getFieldName();
        auto eventTimeFieldVal = context->string_val(eventTimeFieldName);
        auto eventTimeFieldExpr = to_expr(*context, Z3_mk_eq(*context, eventTimeFieldVar, eventTimeFieldVal));

        //CNF both conditions together to compute the descriptors condition
        Z3_ast andConditions[] = {allowedLatenessExpr, eventTimeFieldExpr};
        watermarkDescriptorConditions = to_expr(*context, Z3_mk_and(*context, 2, andConditions));
    } else if (watermarkDescriptor->instanceOf<Windowing::IngestionTimeWatermarkStrategyDescriptor>()) {
        //Create an equality expression <source>.watermarkAssignerType == "IngestionTime"
        auto varName = source + ".watermarkAssignerType";
        auto var = context->constant(context->str_symbol(varName.c_str()), context->string_sort());
        auto val = context->constant(context->str_symbol("IngestionTime"), context->string_sort());
        watermarkDescriptorConditions = to_expr(*context, Z3_mk_eq(*context, var, val));
    } else {
        NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Unrecognized watermark descriptor found.");
    }

    //CNF the watermark conditions to the original condition
    Z3_ast andConditions[] = {*conditions, watermarkDescriptorConditions};
    conditions = std::make_shared<z3::expr>(z3::to_expr(*context, Z3_mk_and(*context, 2, andConditions)));

    //Extract remaining signature attributes from child query signature
    auto attributeMap = childQuerySignature->getAttributeMap();
    auto windowExpressions = childQuerySignature->getWindowsExpressions();
    auto columns = childQuerySignature->getColumns();

    return QuerySignature::create(conditions, columns, windowExpressions, attributeMap, sources);*/
    return nullptr;
}

z3::ExprPtr QuerySignatureUtil::substituteIntoInputExpression(const z3::ContextPtr context, const z3::ExprPtr inputExpr,
                                                              std::map<std::string, z3::ExprPtr>& operandFieldMap,
                                                              std::map<std::string, std::vector<z3::ExprPtr>>& columns) {
    z3::ExprPtr updatedExpr = inputExpr;
    //Loop over the Operand Fields contained in the input expression
    for (auto [operandExprName, operandExpr] : operandFieldMap) {

        if (columns.find(operandExprName) == columns.end()) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: We can't assign attribute " + operandExprName + " that doesn't exists");
        }

        //Change from
        z3::expr_vector from(*context);
        from.push_back(*operandExpr);

        //Change to
        //Fetch the modified operand expression to be substituted
        auto derivedOperandExpr = columns[operandExprName];
        z3::expr_vector to(*context);
        to.push_back(*derivedOperandExpr);

        //Perform replacement
        updatedExpr = std::make_shared<z3::expr>(updatedExpr->substitute(from, to));
    }
    return updatedExpr;
}
}// namespace NES::Optimizer
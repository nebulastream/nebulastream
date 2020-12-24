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
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/Utils/DataTypeToZ3ExprUtil.hpp>
#include <Optimizer/Utils/ExpressionToZ3ExprUtil.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Optimizer/Utils/Z3ExprAndFieldMap.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <z3++.h>

namespace NES::Optimizer {

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForOperator(OperatorNodePtr operatorNode,
                                                                      std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                                      z3::ContextPtr context) {
    NES_DEBUG("QuerySignatureUtil: Creating query signature for operator " << operatorNode->toString());
    if (operatorNode->instanceOf<SourceLogicalOperatorNode>()) {
        if (!childrenQuerySignatures.empty()) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Source operator can't have children : " + operatorNode->toString());
        }
        //Will return a Z3 expression equivalent to: streamName = <logical stream name>
        SourceLogicalOperatorNodePtr sourceOperator = operatorNode->as<SourceLogicalOperatorNode>();
        std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();

        //Compute attribute mapping and columns based on output schema for source operator
        std::map<std::string, std::vector<std::string>> attributeMap;
        std::map<std::string, z3::ExprPtr> columns;
        const SchemaPtr outputSchema = operatorNode->getOutputSchema();
        for (auto& field : outputSchema->fields) {
            auto attributeName = field->name;
            auto derivedAttributeName = streamName + "." + attributeName;
            attributeMap[attributeName] = {derivedAttributeName};
            auto column = DataTypeToZ3ExprUtil::createForField(derivedAttributeName, field->getDataType(), context)->getExpr();
            columns[derivedAttributeName] = column;
        }

        //Create an equality expression for example: streamName == "<logical stream name>"
        auto var = context->constant(context->str_symbol("streamName"), context->string_sort());
        auto val = context->string_val(streamName);
        auto conditions = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_eq(*context, var, val)));

        //Compute signature
        return QuerySignature::create(conditions, columns, {}, attributeMap, {streamName});
    } else if (operatorNode->instanceOf<SinkLogicalOperatorNode>()) {
        if (childrenQuerySignatures.empty()) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Sink operator can't have empty children set : "
                                    + operatorNode->toString());
        }
        //Will return a z3 expression computed by CNF of children signatures
        return buildFromChildrenSignatures(context, childrenQuerySignatures);
    } else if (operatorNode->instanceOf<FilterLogicalOperatorNode>()) {
        //We do not expect a filter to have no or more than 1 children
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Filter operator can't have empty or more than one children : "
                                    + operatorNode->toString());
        }

        NES_TRACE("QuerySignatureUtil: Computing Z3Expression and filed map for filter predicate");
        auto filterOperator = operatorNode->as<FilterLogicalOperatorNode>();
        return createQuerySignatureForFilter(context, childrenQuerySignatures[0], filterOperator);
    } else if (operatorNode->instanceOf<MergeLogicalOperatorNode>()) {
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() == 1) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Merge operator can't have empty or only one children : "
                                    + operatorNode->toString());
        }

        //Will return a z3 expression computed by CNF of children signatures
        return buildFromChildrenSignatures(context, childrenQuerySignatures);
    } else if (operatorNode->instanceOf<BroadcastLogicalOperatorNode>()) {
        //We do not expect a broadcast operator to have no or more than 1 children
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Broadcast operator can't have empty or more than one children : "
                                    + operatorNode->toString());
        }
        return buildFromChildrenSignatures(context, childrenQuerySignatures);
    } else if (operatorNode->instanceOf<MapLogicalOperatorNode>()) {
        //We do not expect a Map operator to have no or more than 1 children
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Map operator can't have empty or more than one children : "
                                    + operatorNode->toString());
        }

        // compute the expression for only the right side of the assignment operator
        auto mapOperator = operatorNode->as<MapLogicalOperatorNode>();
        return createQuerySignatureForMap(context, childrenQuerySignatures[0], mapOperator);
    } else if (operatorNode->instanceOf<WindowLogicalOperatorNode>()) {
        //We do not expect a window operator to have no or more than 1 children
        if (childrenQuerySignatures.empty() || childrenQuerySignatures.size() > 1) {
            NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Window operator can't have empty or more than one children : "
                                    + operatorNode->toString());
        }
        NES_TRACE("QuerySignatureUtil: Computing Signature for window operator");
        auto windowOperator = operatorNode->as<WindowLogicalOperatorNode>();
        return createQuerySignatureForWindow(context, childrenQuerySignatures[0], windowOperator);
    } else if (operatorNode->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        //FIXME: as part of the issue 1351
        return childrenQuerySignatures[0];
    }
    NES_THROW_RUNTIME_ERROR("No conversion to Z3 expression possible for operator: " + operatorNode->toString());
}

QuerySignaturePtr QuerySignatureUtil::buildFromChildrenSignatures(z3::ContextPtr context,
                                                                  std::vector<QuerySignaturePtr> childrenQuerySignatures) {

    NES_DEBUG("QuerySignatureUtil: Computing Signature from children signatures");
    z3::expr_vector allConditions(*context);
    std::map<std::string, z3::ExprPtr> columns;
    std::map<std::string, z3::ExprPtr> windowExpressions;
    std::map<std::string, std::vector<std::string>> attributeMap;
    std::vector<std::string> sources;

    //Iterate over all children query signatures for computing the column values
    for (auto& childSignature : childrenQuerySignatures) {

        if (sources.empty()) {
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
        }

        //Merge the attribute map together
        if (attributeMap.empty()) {
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
        }

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
                    NES_THROW_RUNTIME_ERROR("Should not occur");
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
    return QuerySignature::create(conditions, columns, windowExpressions, attributeMap, sources);
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForWindow(z3::ContextPtr context, QuerySignaturePtr childQuerySignature,
                                                                    WindowLogicalOperatorNodePtr windowOperator) {

    NES_DEBUG("QuerySignatureUtil: compute signature for window operator");
    z3::expr_vector windowConditions(*context);

    auto windowDefinition = windowOperator->getWindowDefinition();

    //Compute the expression for window key
    z3::expr windowKeyVal(*context);
    std::string windowKey;
    if (windowDefinition->isKeyed()) {
        FieldAccessExpressionNodePtr key = windowDefinition->getOnKey();
        windowKey = key->getFieldName();
    } else {
        windowKey = "non-keyed";
    }
    auto windowKeyVar = context->constant(context->str_symbol("window-key"), context->string_sort());
    windowKeyVal = context->string_val(windowKey);
    auto windowKeyExpression = to_expr(*context, Z3_mk_eq(*context, windowKeyVar, windowKeyVal));

    //Compute the expression for window time key
    auto windowType = windowDefinition->getWindowType();
    auto timeCharacteristic = windowType->getTimeCharacteristic();
    z3::expr windowTimeKeyVal(*context);
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::EventTime) {
        windowTimeKeyVal = context->string_val(timeCharacteristic->getField()->name);
    } else if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::IngestionTime) {
        windowTimeKeyVal = context->string_val(timeCharacteristic->getField()->name);
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
        slide = tumblingWindow->getSize().getTime() * multiplier;
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

    auto sources = childQuerySignature->getSources();
    auto columns = childQuerySignature->getColumns();
    for (auto source : sources) {
        auto derivedOnFieldName = source + "." + onFieldName;
        auto derivedAsFieldName = source + "." + asFieldName;
        if (columns.find(derivedOnFieldName) == columns.end()) {
            NES_THROW_RUNTIME_ERROR("Can find derived attribute " + derivedOnFieldName + " for the source " + source);
        }

        auto onFieldExpression = columns[derivedOnFieldName];
        columns[derivedAsFieldName] = {std::make_shared<z3::expr>(z3::to_expr(*context, aggregate(*onFieldExpression)))};
    }

    std::map<std::string, z3::ExprPtr> updatedColumns;
    auto outputSchema = windowOperator->getOutputSchema();
    for (auto& field : outputSchema->fields) {
        auto originalAttributeName = field->name;

        for (auto source : sources) {
            auto derivedAttributeName = source + "." + originalAttributeName;

            if (columns.find(derivedAttributeName) == columns.end()) {
                if (originalAttributeName == "start" || originalAttributeName == "end") {
                    updatedColumns[originalAttributeName] =
                        DataTypeToZ3ExprUtil::createForField(originalAttributeName, field->getDataType(), context)->getExpr();
                }
            } else {
                updatedColumns[derivedAttributeName] = columns[derivedAttributeName];
            }
        }
    }

    return QuerySignature::create(childQuerySignature->getConditions(), updatedColumns, windowExpressions,
                                  childQuerySignature->getAttributeMap(), sources);
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForMap(z3::ContextPtr context, QuerySignaturePtr childQuerySignature,
                                                                 MapLogicalOperatorNodePtr mapOperator) {

    z3::ExprPtr expr = ExpressionToZ3ExprUtil::createForExpression(mapOperator->getMapExpression(), context)->getExpr();

    //Fetch the signature of only children and get the column values
    auto columns = childQuerySignature->getColumns();
    auto attributeMap = childQuerySignature->getAttributeMap();
    auto sources = childQuerySignature->getSources();
    std::string fieldName = mapOperator->getMapExpression()->getField()->getFieldName();

    //Find if the LHS operand is a new attribute or not
    bool isNewAttribute = true;
    auto inputSchema = mapOperator->getInputSchema();
    for (auto inputField : inputSchema->fields) {
        if (inputField->name == fieldName) {
            isNewAttribute = false;
            break;
        }
    }

    //Substitute rhs operands with actual values computed previously
    for (auto source : sources) {
        NES_INFO(expr->to_string());
        auto derivedAttributeName = source + "." + fieldName;

        if (isNewAttribute) {
            //Add the newly derived attribute to the attribute map
            if (attributeMap.find(fieldName) == attributeMap.end()) {
                attributeMap[fieldName] = {derivedAttributeName};
            } else {
                //Add the newly derived attribute to attribute map
                auto derivedAttributes = attributeMap[fieldName];
                derivedAttributes.push_back(derivedAttributeName);
                attributeMap[fieldName] = derivedAttributes;
            }
        } else if (!isNewAttribute && columns.find(derivedAttributeName) == columns.end()) {
            // this attribute doesn't exists in the stream source and hence should not be created
            continue;
        }
        z3::ExprPtr exprCopy = expr;

        for (unsigned i = 0; i < exprCopy->num_args(); i++) {
            const auto& rhsOperandExpr = exprCopy->arg(i);
            auto rhsOperandExprName = rhsOperandExpr.to_string();
            auto derivedRHSOperandName = source + "." + rhsOperandExprName;

            if (columns.find(derivedRHSOperandName) == columns.end()) {
                //TODO: open issue for this
                NES_THROW_RUNTIME_ERROR("We can't assign attribute " + derivedRHSOperandName
                                        + " that doesn't exists in the source " + source);
            }

            auto derivedRHSOperandExpr = columns[derivedRHSOperandName];

            //Change from
            z3::expr_vector from(*context);
            from.push_back(rhsOperandExpr);

            //Change to
            z3::expr_vector to(*context);
            to.push_back(*derivedRHSOperandExpr);

            //Perform replacement
            exprCopy = std::make_shared<z3::expr>(exprCopy->substitute(from, to));
        }

        columns[derivedAttributeName] = exprCopy;
    }

    //Prepare all combinations of col expression for the given map by substituting the previous col values in the assignment expression
    std::vector<z3::ExprPtr> allColExprForMap{expr};

    return QuerySignature::create(childQuerySignature->getConditions(), columns, childQuerySignature->getWindowsExpressions(),
                                  attributeMap, sources);
}

QuerySignaturePtr QuerySignatureUtil::createQuerySignatureForFilter(z3::ContextPtr context, QuerySignaturePtr childQuerySignature,
                                                                    FilterLogicalOperatorNodePtr filterOperator) {

    auto filterExprAndFieldMap = ExpressionToZ3ExprUtil::createForExpression(filterOperator->getPredicate(), context);
    auto filterFieldMap = filterExprAndFieldMap->getFieldMap();
    auto filterExpr = filterExprAndFieldMap->getExpr();

    NES_TRACE("QuerySignatureUtil: Replace Z3 Expression for the filed with corresponding column values from "
              "children signatures");
    //As filter can't have more than 1 children so fetch the only child signature
    auto columns = childQuerySignature->getColumns();
    auto sources = childQuerySignature->getSources();

    z3::expr_vector filterExpressions(*context);
    for (auto source : sources) {

        auto exprCopy = filterExpr;

        for (auto [operandName, operandExpr] : filterFieldMap) {
            auto derivedOperandName = source + "." + operandName;
            if (columns.find(derivedOperandName) == columns.end()) {
                NES_THROW_RUNTIME_ERROR("Can find derived attribute " + derivedOperandName + " for the source " + source);
            }
            auto derivedOperandExpression = columns[derivedOperandName];

            //Change from
            z3::expr_vector from(*context);
            from.push_back(*operandExpr);

            //Change to
            z3::expr_vector to(*context);
            to.push_back(*derivedOperandExpression);

            //Perform replacement
            exprCopy = std::make_shared<z3::expr>(exprCopy->substitute(from, to));
        }

        filterExpressions.push_back(*exprCopy);
    }

    //Compute a DNF condition for all different conditions identified by substituting the col values
    auto filterConditions = z3::mk_or(filterExpressions);

    //Compute a CNF condition using the children and filter conditions
    auto childConditions = childQuerySignature->getConditions();
    Z3_ast array[] = {filterConditions, *childConditions};
    auto conditions = std::make_shared<z3::expr>(to_expr(*context, Z3_mk_and(*context, 2, array)));
    return QuerySignature::create(conditions, columns, childQuerySignature->getWindowsExpressions(),
                                  childQuerySignature->getAttributeMap(), sources);
}

std::map<std::string, std::vector<z3::ExprPtr>>
QuerySignatureUtil::updateQuerySignatureColumns(z3::ContextPtr context, SchemaPtr outputSchema,
                                                std::map<std::string, std::vector<z3::ExprPtr>> oldColumnMap) {

    NES_DEBUG("QuerySignatureUtil: update columns based on output schema");
    std::map<std::string, std::vector<z3::ExprPtr>> updatedColumnMap;

    //Iterate over all output fields of the schema and check if the expression for the field exists in oldColumnMap
    // if doesn't exists then create a new one else take the one from old column map
    auto outputFields = outputSchema->fields;
    for (auto& outputField : outputFields) {
        auto fieldName = outputField->name;

        if (oldColumnMap.find(fieldName) != oldColumnMap.end()) {
            updatedColumnMap[fieldName] = oldColumnMap[fieldName];
        } else {
            auto expr = DataTypeToZ3ExprUtil::createForField(fieldName, outputField->getDataType(), context)->getExpr();
            updatedColumnMap[fieldName] = {expr};
        }
    }
    return updatedColumnMap;
}

}// namespace NES::Optimizer
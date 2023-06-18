/*
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
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>

namespace NES::Optimizer {

SignatureContainmentUtilPtr SignatureContainmentUtil::create(const z3::ContextPtr& context) {
    return std::make_shared<SignatureContainmentUtil>(context);
}

SignatureContainmentUtil::SignatureContainmentUtil(const z3::ContextPtr& context) {
    this->context = context;
    this->solver = std::make_unique<z3::solver>(*this->context);
}

ContainmentType SignatureContainmentUtil::checkContainmentForBottomUpMerging(const QuerySignaturePtr& leftSignature,
                                                           const QuerySignaturePtr& rightSignature) {
    NES_TRACE("Checking for containment.");
    ContainmentType containmentRelationship = ContainmentType::NO_CONTAINMENT;
    auto otherConditions = rightSignature->getConditions();
    auto conditions = leftSignature->getConditions();
    NES_TRACE("Left signature: {}", conditions->to_string());
    NES_TRACE("Right signature: {}", otherConditions->to_string());
    if (!conditions || !otherConditions) {
        NES_WARNING("Can't obtain containment relationships for null signatures");
        return ContainmentType::NO_CONTAINMENT;
    }
    try {
        // In the following, we
        // First check for WindowContainment
        // In case of window equality, we continue to check for projection containment
        // In case of projection equality, we finally check for filter containment
        containmentRelationship = get<1>(checkWindowContainment(leftSignature, rightSignature));
        NES_TRACE("Check window containment returned: {}", magic_enum::enum_name(containmentRelationship));
        if (containmentRelationship == ContainmentType::EQUALITY) {
            containmentRelationship = checkProjectionContainment(leftSignature, rightSignature);
            NES_TRACE("Check projection containment returned: {}", magic_enum::enum_name(containmentRelationship));
            if (containmentRelationship == ContainmentType::EQUALITY) {
                containmentRelationship = checkFilterContainment(leftSignature, rightSignature);
                if (containmentRelationship == ContainmentType::LEFT_SIG_CONTAINED
                    || containmentRelationship == ContainmentType::RIGHT_SIG_CONTAINED) {
                    if (!checkFilterContainmentPossible(rightSignature, leftSignature)) {
                        containmentRelationship = ContainmentType::NO_CONTAINMENT;
                    }
                }
                NES_TRACE("Check filter containment returned: {}", magic_enum::enum_name(containmentRelationship));
            }
        }
    } catch (...) {
        auto exception = std::current_exception();
        try {
            std::rethrow_exception(exception);
        } catch (const std::exception& e) {
            NES_ERROR("SignatureContainmentUtil: Exception occurred while performing containment check among "
                      "queryIdAndCatalogEntryMapping {}",
                      e.what());
        }
    }
    return containmentRelationship;
}

std::tuple<ContainmentType, std::vector<LogicalOperatorNodePtr>>
SignatureContainmentUtil::checkContainmentRelationshipTopDown(const LogicalOperatorNodePtr& leftOperator,
                                                              const LogicalOperatorNodePtr& rightOperator) {
    NES_TRACE("Checking for containment.");
    ContainmentType containmentRelationship = ContainmentType::NO_CONTAINMENT;
    std::vector<LogicalOperatorNodePtr> containmentOperators = {};
    try {
        auto otherConditions = leftOperator->getZ3Signature()->getConditions();
        NES_TRACE("Left signature: {}", otherConditions->to_string());
        auto conditions = rightOperator->getZ3Signature()->getConditions();
        NES_TRACE("Right signature: {}", conditions->to_string());
        if (!conditions || !otherConditions) {
            NES_WARNING("Can't obtain containment relationships for null signatures");
            return {};
        }
        // In the following, we
        // First check for WindowContainment
        // In case of window equality, we continue to check for projection containment
        // In case of projection equality, we finally check for filter containment
        // If we detect a containment relationship at any point in the algorithm, we stop and extract the contained upstream operators
        auto windowContainment = checkWindowContainment(leftOperator->getZ3Signature(), rightOperator->getZ3Signature());
        containmentRelationship = get<1>(windowContainment);
        NES_TRACE("Check window containment returned: {}", magic_enum::enum_name(containmentRelationship));
        if (containmentRelationship == ContainmentType::EQUALITY) {
            containmentRelationship = checkProjectionContainment(leftOperator->getZ3Signature(), rightOperator->getZ3Signature());
            NES_TRACE("Check projection containment returned: {}", magic_enum::enum_name(containmentRelationship));
            if (containmentRelationship == ContainmentType::EQUALITY) {
                containmentRelationship = checkFilterContainment(leftOperator->getZ3Signature(), rightOperator->getZ3Signature());
                NES_TRACE("Check filter containment returned: {}", magic_enum::enum_name(containmentRelationship));
                if (containmentRelationship == ContainmentType::EQUALITY) {
                    return {ContainmentType::EQUALITY, {}};
                } else if (containmentRelationship == ContainmentType::RIGHT_SIG_CONTAINED) {
                    containmentOperators = createFilterOperators(leftOperator, rightOperator);
                    if (containmentOperators.empty()) {
                        containmentRelationship = ContainmentType::NO_CONTAINMENT;
                    }
                } else if (containmentRelationship == ContainmentType::LEFT_SIG_CONTAINED) {
                    containmentOperators = createFilterOperators(rightOperator, leftOperator);
                    if (containmentOperators.empty()) {
                        containmentRelationship = ContainmentType::NO_CONTAINMENT;
                    }
                }
            } else if (containmentRelationship == ContainmentType::RIGHT_SIG_CONTAINED) {
                auto projectionOperator = createProjectionOperator(rightOperator);
                if (projectionOperator == nullptr) {
                    containmentRelationship = ContainmentType::NO_CONTAINMENT;
                } else {
                    containmentOperators.push_back(projectionOperator);
                }
            } else if (containmentRelationship == ContainmentType::LEFT_SIG_CONTAINED) {
                auto projectionOperator = createProjectionOperator(leftOperator);
                if (projectionOperator == nullptr) {
                    containmentRelationship = ContainmentType::NO_CONTAINMENT;
                } else {
                    containmentOperators.push_back(projectionOperator);
                }
            }
        } else if (containmentRelationship == ContainmentType::RIGHT_SIG_CONTAINED) {
            containmentOperators = createContainedWindowOperator(rightOperator, get<0>(windowContainment));
            if (containmentOperators.empty()) {
                containmentRelationship = ContainmentType::NO_CONTAINMENT;
            }
        } else if (containmentRelationship == ContainmentType::LEFT_SIG_CONTAINED) {
            containmentOperators = createContainedWindowOperator(leftOperator, get<0>(windowContainment));
            if (containmentOperators.empty()) {
                containmentRelationship = ContainmentType::NO_CONTAINMENT;
            }
        }
    } catch (...) {
        auto exception = std::current_exception();
        try {
            std::rethrow_exception(exception);
        } catch (const std::exception& e) {
            NES_ERROR("SignatureContainmentUtil: Exception occurred while performing containment check among "
                       "queryIdAndCatalogEntryMapping {}",
                       e.what());
        }
    }
    return std::tuple(containmentRelationship, containmentOperators);
}

ContainmentType SignatureContainmentUtil::checkProjectionContainment(const QuerySignaturePtr& leftSignature,
                                                                     const QuerySignaturePtr& rightSignature) {
    z3::expr_vector leftQueryProjectionFOL(*context);
    z3::expr_vector rightQueryProjectionFOL(*context);
    //create the projection conditions for each signature
    createProjectionFOL(leftSignature, leftQueryProjectionFOL);
    createProjectionFOL(rightSignature, rightQueryProjectionFOL);
    // We first check if the left query signature's projection first order logic (FOL) is contained by the right signature's projections FOL,
    // if yes, we check for equal transformations
    // then we move on to check for equality and attribute order equality
    // if those checks passed, we return equality
    // else if that check failed, we check for filter equality and return left signature contained if we have equal filters
    // Otherwise, we check the other containment relationship in the same manner but exclude the equality check
    // if (!rightProjectionFOL && leftProjectionFOL == unsat, aka rightProjectionFOL ⊆ leftProjectionFOL
    //      && rightTransformationFOL != leftTransformationFOL == unsat, aka rightTransformationFOL == leftTransformationFOL
    //        if (rightProjectionFOL && !leftProjectionFOL == unsat, aka rightProjectionFOL ⊆ leftProjectionFOL)
    //            true: return Equality
    //        if (checkFilterContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY)
    //            true: return Right sig contained
    // else if (rightProjectionFOL && !leftProjectionFOL == unsat, aka leftProjectionFOL ⊆ rightProjectionFOL)
    //      && rightTransformationFOL != leftTransformationFOL == unsat, aka rightTransformationFOL == leftTransformationFOL
    //      && filters are equal
    //          true: return Left sig contained
    // else: No_Containment
    if (checkContainmentConditionsUnsatisfied(rightQueryProjectionFOL, leftQueryProjectionFOL)
        && checkForEqualTransformations(leftSignature, rightSignature)) {
        if (checkContainmentConditionsUnsatisfied(leftQueryProjectionFOL, rightQueryProjectionFOL)
            && checkAttributeOrder(leftSignature, rightSignature)) {
            NES_TRACE("Equal projection.");
            return ContainmentType::EQUALITY;
        } else if (checkFilterContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY) {
            return ContainmentType::RIGHT_SIG_CONTAINED;
        }
    } else if (checkContainmentConditionsUnsatisfied(leftQueryProjectionFOL, rightQueryProjectionFOL)
               && checkForEqualTransformations(leftSignature, rightSignature)
               && checkFilterContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY) {
        return ContainmentType::LEFT_SIG_CONTAINED;
    }
    return ContainmentType::NO_CONTAINMENT;
}

std::tuple<uint8_t, ContainmentType> SignatureContainmentUtil::checkWindowContainment(const QuerySignaturePtr& leftSignature,
                                                                                      const QuerySignaturePtr& rightSignature) {
    // if no window signature is present, return equality
    if (leftSignature->getWindowsExpressions().empty() && rightSignature->getWindowsExpressions().empty()) {
        //0 indicates that there are no window operations in the queries
        return std::tuple(NO_WINDOW_PRESENT, ContainmentType::EQUALITY);
    }
    // obtain the number of window operations. Use the number of window operations from the signature that has less window operations
    size_t numberOfWindows = rightSignature->getWindowsExpressions().size();
    if (numberOfWindows > leftSignature->getWindowsExpressions().size()) {
        numberOfWindows = leftSignature->getWindowsExpressions().size();
    }
    NES_TRACE("Number of windows: {}", numberOfWindows);
    // each vector entry in the windowExpressions vector represents the signature of one window
    // we assume a bottom up approach for our containment algorithm, hence a window operation can only be partially shared if
    // the previous operations are completely sharable. As soon as there is no equality in window operations, we return the
    // obtained relationship
    ContainmentType containmentRelationship = ContainmentType::NO_CONTAINMENT;
    uint8_t currentWindow = 0;
    for (size_t i = 0; i < numberOfWindows; ++i) {
        // obtain each window signature in bottom up fashion
        const auto& leftWindow = leftSignature->getWindowsExpressions()[i];
        const auto& rightWindow = rightSignature->getWindowsExpressions()[i];
        NES_TRACE("Starting with left window: {}", leftWindow.at("z3-window-expressions")->to_string());
        NES_TRACE("Starting with right window: {}", rightWindow.at("z3-window-expressions")->to_string());
        // checks if the window ids are equal, operator sharing can only happen for equal window ids
        // the window id consists of the involved window-keys, and the time stamp attribute
        //extract the z3-window-expressions
        z3::expr_vector leftQueryWindowConditions(*context);
        z3::expr_vector rightQueryWindowConditions(*context);
        leftQueryWindowConditions.push_back(to_expr(*context, *leftWindow.at("z3-window-expressions")));
        rightQueryWindowConditions.push_back(to_expr(*context, *rightWindow.at("z3-window-expressions")));
        NES_TRACE("Created window FOL.");
        //checks if the number of aggregates is equal, for equal number of aggregates we
        // 1. check for complete equality
        // 2. check if a containment relationship exists,
        // e.g. z3 checks leftWindow-size <= rightWindow-size && leftWindow-slide <= rightWindow-slide
        NES_TRACE("Number of Aggregates left window: {}", leftWindow.at("number-of-aggregates")->to_string());
        NES_TRACE("Number of Aggregates right window: {}", rightWindow.at("number-of-aggregates")->to_string());
        if (leftWindow.at("number-of-aggregates")->get_numeral_int()
            == rightWindow.at("number-of-aggregates")->get_numeral_int()) {
            NES_TRACE("Same number of aggregates.");
            if (checkContainmentConditionsUnsatisfied(rightQueryWindowConditions, leftQueryWindowConditions)) {
                if (checkContainmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)) {
                    NES_TRACE("Equal windows.");
                    containmentRelationship = ContainmentType::EQUALITY;
                }
                // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
                // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
                // additionally, we also check for projection equality
                else if (checkWindowContainmentPossible(leftWindow, rightWindow, leftSignature, rightSignature)
                         && (checkProjectionContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY)) {
                    NES_TRACE("Right window contained.");
                    containmentRelationship = ContainmentType::RIGHT_SIG_CONTAINED;
                } else {
                    containmentRelationship = ContainmentType::NO_CONTAINMENT;
                }
                // first, we check that there is a window containment relationship then
                // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
                // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
                // additionally, we also check for projection equality
            } else if (checkContainmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)
                       && checkWindowContainmentPossible(rightWindow, leftWindow, leftSignature, rightSignature)
                       && (checkProjectionContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY)) {
                NES_TRACE("Left window contained.");
                containmentRelationship = ContainmentType::LEFT_SIG_CONTAINED;
            } else {
                containmentRelationship = ContainmentType::NO_CONTAINMENT;
            }
            // checks if the number of aggregates for the left signature is smaller than the number of aggregates for the right
            // signature
        } else if (leftWindow.at("number-of-aggregates")->get_numeral_int()
                   < rightWindow.at("number-of-aggregates")->get_numeral_int()) {
            NES_TRACE("Right Window has more Aggregates than left Window.");
            combineWindowAndProjectionFOL(leftSignature, rightSignature, leftQueryWindowConditions, rightQueryWindowConditions);
            // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
            // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
            // then check if the left window is contained
            if (checkWindowContainmentPossible(rightWindow, leftWindow, leftSignature, rightSignature)
                && checkContainmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)) {
                if (checkContainmentConditionsUnsatisfied(rightQueryWindowConditions, leftQueryWindowConditions)) {
                    NES_TRACE("Equal windows.");
                    containmentRelationship = ContainmentType::EQUALITY;
                } else {
                    NES_TRACE("Left window contained.");
                    containmentRelationship = ContainmentType::LEFT_SIG_CONTAINED;
                }
            } else {
                containmentRelationship = ContainmentType::NO_CONTAINMENT;
            }
            // checks if the number of aggregates for the left signature is larger than the number of aggregates for the right
            // signature
        } else if (leftWindow.at("number-of-aggregates")->get_numeral_int()
                   > rightWindow.at("number-of-aggregates")->get_numeral_int()) {
            NES_TRACE("Left Window has more Aggregates than right Window.");
            // combines window and projection FOL to find out containment relationships
            combineWindowAndProjectionFOL(leftSignature, rightSignature, leftQueryWindowConditions, rightQueryWindowConditions);
            // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
            // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
            // then check if the right window is contained
            if (checkWindowContainmentPossible(leftWindow, rightWindow, leftSignature, rightSignature)
                && checkContainmentConditionsUnsatisfied(rightQueryWindowConditions, leftQueryWindowConditions)) {
                NES_TRACE("Right window contained.");
                containmentRelationship = ContainmentType::RIGHT_SIG_CONTAINED;
            } else {
                containmentRelationship = ContainmentType::NO_CONTAINMENT;
            }
        }
        // stop the loop as soon as there is no equality relationship
        if (containmentRelationship != ContainmentType::EQUALITY) {
            return std::tuple(i + 1, containmentRelationship);
        }
        currentWindow = i + 1;
    }
    return std::tuple(currentWindow, containmentRelationship);
}

ContainmentType SignatureContainmentUtil::checkFilterContainment(const QuerySignaturePtr& leftSignature,
                                                                 const QuerySignaturePtr& rightSignature) {
    NES_TRACE("Create new condition vectors.");
    z3::expr_vector leftQueryFilterConditions(*context);
    z3::expr_vector rightQueryFilterConditions(*context);
    NES_TRACE("Add filter conditions.");
    leftQueryFilterConditions.push_back(to_expr(*context, *leftSignature->getConditions()));
    rightQueryFilterConditions.push_back(to_expr(*context, *rightSignature->getConditions()));
    NES_TRACE("content of left sig expression vectors: {}", leftQueryFilterConditions.to_string());
    NES_TRACE("content of right sig expression vectors: {}", rightQueryFilterConditions.to_string());
    //The rest of the method checks for filter containment as follows:
    //check if right sig ⊆ left sig for filters, i.e. if ((right cond && !left condition) == unsat) <=> right sig ⊆ left sig,
    //since we're checking for projection containment, the negation is on the side of the contained condition,
    //e.g. right sig ⊆ left sig <=> (((attr<5 && attr2==6) && !(attr1<=10 && attr2==45)) == unsat)
    //      check if right sig ⊆ left sig for filters
    //           true: check if left sig ⊆ right sig
    //               true: return EQUALITY
    //               false: return RIGHT_SIG_CONTAINED
    //           false: check if left sig ⊆ right sig
    //               true: return LEFT_SIG_CONTAINED
    //           false: return NO_CONTAINMENT
    if (checkContainmentConditionsUnsatisfied(leftQueryFilterConditions, rightQueryFilterConditions)) {
        if (checkContainmentConditionsUnsatisfied(rightQueryFilterConditions, leftQueryFilterConditions)) {
            NES_TRACE("Equal filters.");
            return ContainmentType::EQUALITY;
        }
        NES_TRACE("left sig contains right sig for filters.");
        return ContainmentType::RIGHT_SIG_CONTAINED;
    } else if (checkContainmentConditionsUnsatisfied(rightQueryFilterConditions, leftQueryFilterConditions)) {
        NES_TRACE("right sig contains left sig for filters.");
        return ContainmentType::LEFT_SIG_CONTAINED;
    }
    return ContainmentType::NO_CONTAINMENT;
}

std::vector<LogicalOperatorNodePtr>
SignatureContainmentUtil::createContainedWindowOperator(const LogicalOperatorNodePtr& containedOperator,
                                                        const uint8_t containedWindowIndex) {
    std::vector<LogicalOperatorNodePtr> containmentOperators = {};
    auto windowOperators = containedOperator->getNodesByType<WindowLogicalOperatorNode>();
    //get the correct window operator
    if (containedWindowIndex > 0 && containedWindowIndex <= windowOperators.size()) {
        LogicalOperatorNodePtr windowOperator = windowOperators[windowOperators.size() - containedWindowIndex];
        auto windowDefinition = windowOperator->as<WindowLogicalOperatorNode>()->getWindowDefinition();
        //check that containee is a time based window, else return false
        if (windowDefinition->getWindowType()->isTimeBasedWindowType()) {
            auto timeBasedWindow = windowDefinition->getWindowType()->asTimeBasedWindowType(windowDefinition->getWindowType());
            //we need to set the time characteristic field to start because the previous timestamp will not exist anymore
            auto field = windowOperator->getOutputSchema()->hasFieldName("start");
            //return false if this is not possible
            if (field == nullptr) {
                return {};
            }
            timeBasedWindow->getTimeCharacteristic()->setField(field);
            containmentOperators.push_back(windowOperator);
            //obtain the watermark operator
            auto watermarkOperator = windowOperator->getChildren()[0]->as<LogicalOperatorNode>();
            containmentOperators.push_back(watermarkOperator);
            NES_TRACE("Window containment possible.");
        }
    }
    if (!containmentOperators.empty()
        && !checkDownstreamOperatorChainForSingleParent(containedOperator, containmentOperators.back())) {
        return {};
    }
    return containmentOperators;
}

LogicalOperatorNodePtr SignatureContainmentUtil::createProjectionOperator(const LogicalOperatorNodePtr& containedOperator) {
    auto projectionOperators = containedOperator->getNodesByType<ProjectionLogicalOperatorNode>();
    //get the most downstream projection operator
    if (!projectionOperators.empty()) {
        if (!checkDownstreamOperatorChainForSingleParent(containedOperator, projectionOperators.at(0))) {
            return {};
        }
        return projectionOperators.at(0);
    }
    return nullptr;
}

std::vector<LogicalOperatorNodePtr> SignatureContainmentUtil::createFilterOperators(const LogicalOperatorNodePtr& container,
                                                                                    const LogicalOperatorNodePtr& containee) {
    NES_DEBUG("Check if filter containment is possible for container {}, containee {}.", container->toString(), containee->toString());
    //if all checks pass, we extract the filter operators
    std::vector<LogicalOperatorNodePtr> containmentOperators = {};
    std::vector<FilterLogicalOperatorNodePtr> upstreamFilterOperators = containee->getNodesByType<FilterLogicalOperatorNode>();
    if (upstreamFilterOperators.empty()) {
        return {};
    } else if (upstreamFilterOperators.size() == 1) {
        containmentOperators.push_back(upstreamFilterOperators.front());
        std::vector<std::string> mapAttributeNames = {};
        if (!checkDownstreamOperatorChainForSingleParent(containee, upstreamFilterOperators.front(), mapAttributeNames)) {
            return {};
        }
        if (!mapAttributeNames.empty()
            && !filterPredicateStillApplicable(upstreamFilterOperators.front(),
                                               mapAttributeNames,
                                               container->getOutputSchema())) {
            return {};
        }
        return containmentOperators;
    } else {
        std::vector<std::string> mapAttributeNames = {};
        NES_DEBUG2("Filter predicate: {}", upstreamFilterOperators.back()->toString());
        if (checkDownstreamOperatorChainForSingleParent(containee, upstreamFilterOperators.back(), mapAttributeNames)) {
            NES_DEBUG2("Filter predicate: {}", upstreamFilterOperators.back()->toString());
            auto predicate = upstreamFilterOperators.front()->getPredicate();
            if (!mapAttributeNames.empty()
                && !filterPredicateStillApplicable(upstreamFilterOperators.front(),
                                                   mapAttributeNames,
                                                   container->getOutputSchema())) {
                return {};
            }
            for (size_t i = 1; i < upstreamFilterOperators.size(); ++i) {
                if (!mapAttributeNames.empty()
                    && !filterPredicateStillApplicable(upstreamFilterOperators.at(i),
                                                       mapAttributeNames,
                                                       container->getOutputSchema())) {
                    return {};
                }
                predicate = AndExpressionNode::create(predicate, upstreamFilterOperators.at(i)->getPredicate());
            }
            NES_DEBUG2("Filter predicate: {}", predicate->toString());
            upstreamFilterOperators.front()->setPredicate(predicate);
            return {upstreamFilterOperators.front()};
        } else {
            return {};
        }
    }
    return containmentOperators;
}

bool SignatureContainmentUtil::filterPredicateStillApplicable(FilterLogicalOperatorNodePtr const& filterOperator,
                                                              const std::vector<std::string>& fieldNames,
                                                              const SchemaPtr& containerOutputSchema) {

    NES_TRACE2("Create an iterator for traversing the filter predicates");
    const ExpressionNodePtr filterPredicate = filterOperator->getPredicate();
    DepthFirstNodeIterator depthFirstNodeIterator(filterPredicate);
    for (auto itr = depthFirstNodeIterator.begin(); itr != NES::DepthFirstNodeIterator::end(); ++itr) {
        NES_TRACE2("Iterate and find the predicate with FieldAccessExpression Node");
        if ((*itr)->instanceOf<FieldAccessExpressionNode>()) {
            const FieldAccessExpressionNodePtr accessExpressionNode = (*itr)->as<FieldAccessExpressionNode>();
            NES_TRACE2("Check if the input field name is same as the FieldAccessExpression field name");
            for (const auto& fieldName : fieldNames) {
                if (accessExpressionNode->getFieldName() == fieldName
                    && !containerOutputSchema->contains(accessExpressionNode->getFieldName())) {
                    return false;
                }
            }
        }
    }
    return true;
}

std::string SignatureContainmentUtil::getFieldNameUsedByMapOperator(const NodePtr& node) {
    NES_TRACE2("Find the field name used in map operator");
    MapLogicalOperatorNodePtr mapLogicalOperatorNodePtr = node->as<MapLogicalOperatorNode>();
    const FieldAssignmentExpressionNodePtr mapExpression = mapLogicalOperatorNodePtr->getMapExpression();
    const FieldAccessExpressionNodePtr field = mapExpression->getField();
    return field->getFieldName();
}

void SignatureContainmentUtil::createProjectionFOL(const QuerySignaturePtr& signature, z3::expr_vector& projectionFOL) {
    //create projection FOL
    //We create a boolean value that we set to true for each attribute present in the schemaFieldToExprMaps
    //We create an and expression for those values
    //in case there is a union present, we create an or expression for each schemaFieldToExprMap in the vector
    z3::expr_vector orFOlForUnion(*context);
    NES_TRACE("Length of schemaFieldToExprMaps: {}", signature->getSchemaFieldToExprMaps().size());
    for (auto schemaFieldToExpressionMap : signature->getSchemaFieldToExprMaps()) {
        z3::expr_vector createSourceFOL(*context);
        for (auto [attributeName, z3Expression] : schemaFieldToExpressionMap) {
            NES_TRACE("SignatureContainmentUtil::createProjectionFOL: strings: {}", attributeName);
            NES_TRACE("SignatureContainmentUtil::createProjectionFOL: z3 expressions: {}", z3Expression->to_string());
            z3::ExprPtr expr = std::make_shared<z3::expr>(context->bool_const(attributeName.c_str()));
            z3::ExprPtr columnIsUsed = std::make_shared<z3::expr>(context->bool_val(true));
            createSourceFOL.push_back(to_expr(*context, Z3_mk_eq(*context, *expr, *columnIsUsed)));
        }
        orFOlForUnion.push_back(to_expr(*context, mk_and(createSourceFOL)));
    }
    if (signature->getSchemaFieldToExprMaps().size() > 1) {
        projectionFOL.push_back(to_expr(*context, mk_or(orFOlForUnion)));
    } else {
        projectionFOL = orFOlForUnion;
    }
    NES_TRACE("Projection FOL: {}", projectionFOL.to_string());
}

bool SignatureContainmentUtil::checkForEqualTransformations(const QuerySignaturePtr& leftSignature,
                                                            const QuerySignaturePtr& rightSignature) {
    auto schemaFieldToExprMaps = leftSignature->getSchemaFieldToExprMaps();
    auto otherSchemaFieldToExprMaps = rightSignature->getSchemaFieldToExprMaps();
    auto columns = leftSignature->getColumns();
    auto otherColumns = rightSignature->getColumns();
    auto containedAttributes = columns;
    if (containedAttributes.size() > otherColumns.size()) {
        containedAttributes = otherColumns;
    }
    for (auto schemaFieldToExprMap : schemaFieldToExprMaps) {
        bool schemaMatched = false;
        for (auto otherSchemaMapItr = otherSchemaFieldToExprMaps.begin(); otherSchemaMapItr != otherSchemaFieldToExprMaps.end();
             otherSchemaMapItr++) {
            z3::expr_vector colChecks(*context);
            for (uint64_t index = 0; index < containedAttributes.size(); index++) {
                auto colExpr = schemaFieldToExprMap[containedAttributes[index]];
                auto otherColExpr = (*otherSchemaMapItr)[containedAttributes[index]];
                auto equivalenceCheck = to_expr(*context, Z3_mk_eq(*context, *colExpr, *otherColExpr));
                NES_TRACE("Equivalence check for transformations: {} on attribute {}",
                           equivalenceCheck.to_string(),
                           containedAttributes[index]);
                colChecks.push_back(equivalenceCheck);
            }

            solver->push();
            solver->add(!z3::mk_and(colChecks).simplify());
            NES_TRACE("Content of equivalence solver: {}", solver->to_smt2());
            schemaMatched = solver->check() == z3::unsat;
            solver->pop();
            counter++;
            if (counter >= RESET_SOLVER_THRESHOLD) {
                resetSolver();
            }

            //If the transformations are equal then remove the other schema from the list to avoid duplicate matching
            if (schemaMatched) {
                otherSchemaFieldToExprMaps.erase(otherSchemaMapItr);
                break;
            }
        }

        //If the transformations are not equal two signatures are different and cannot be contained or equal
        if (!schemaMatched) {
            NES_TRACE("Both signatures have different transformations.");
            return false;
        }
    }
    return true;
}

bool SignatureContainmentUtil::checkContainmentConditionsUnsatisfied(z3::expr_vector& negatedCondition,
                                                                     z3::expr_vector& condition) {
    solver->push();
    solver->add(!mk_and(negatedCondition).simplify());
    solver->push();
    solver->add(mk_and(condition).simplify());
    NES_TRACE("Check unsat: {}", solver->check());
    NES_TRACE("Content of solver: {}", solver->to_smt2());
    bool conditionUnsatisfied = false;
    if (solver->check() == z3::unsat) {
        conditionUnsatisfied = true;
    }
    solver->pop(NUMBER_OF_CONDITIONS_TO_POP_FROM_SOLVER);
    counter++;
    if (counter >= RESET_SOLVER_THRESHOLD) {
        resetSolver();
    }
    return conditionUnsatisfied;
}

bool SignatureContainmentUtil::checkAttributeOrder(const QuerySignaturePtr& leftSignature,
                                                   const QuerySignaturePtr& rightSignature) const {
    for (size_t j = 0; j < rightSignature->getColumns().size(); ++j) {
        NES_TRACE("Containment order check for {}", rightSignature->getColumns()[j]);
        NES_TRACE(" and {}", leftSignature->getColumns()[j]);
        if (leftSignature->getColumns()[j] != rightSignature->getColumns()[j]) {
            return false;
        }
    }
    return true;
}

void SignatureContainmentUtil::combineWindowAndProjectionFOL(const QuerySignaturePtr& leftSignature,
                                                             const QuerySignaturePtr& rightSignature,
                                                             z3::expr_vector& leftFOL,
                                                             z3::expr_vector& rightFOL) {
    z3::expr_vector leftQueryFOL(*context);
    z3::expr_vector rightQueryFOL(*context);
    //create conditions for projection containment
    createProjectionFOL(leftSignature, leftQueryFOL);
    createProjectionFOL(rightSignature, rightQueryFOL);
    for (const auto& fol : leftQueryFOL) {
        leftFOL.push_back(fol);
    }
    for (const auto& fol : rightQueryFOL) {
        rightFOL.push_back(fol);
    }
}

bool SignatureContainmentUtil::checkWindowContainmentPossible(const std::map<std::string, z3::ExprPtr>& containerWindow,
                                                              const std::map<std::string, z3::ExprPtr>& containedWindow,
                                                              const QuerySignaturePtr& leftSignature,
                                                              const QuerySignaturePtr& rightSignature) {
    const auto& median = Windowing::WindowAggregationDescriptor::Type::Median;
    const auto& avg = Windowing::WindowAggregationDescriptor::Type::Avg;
    NES_TRACE("Current window-id: {}", containerWindow.at("window-id")->to_string());
    NES_TRACE("Current window-id != JoinWindow: {}", containerWindow.at("window-id")->to_string() != "\"JoinWindow\"");
    //first, check that the window is not a join window and that it does not contain a median or avg aggregation function
    bool windowContainmentPossible = containerWindow.at("window-id")->to_string() != "\"JoinWindow\""
        && containedWindow.at("window-id")->to_string() != "\"JoinWindow\""
        && std::find(containerWindow.at("aggregate-types")->to_string().begin(),
                     containerWindow.at("aggregate-types")->to_string().end(),
                     magic_enum::enum_integer(median))
            != containerWindow.at("aggregate-types")->to_string().end()
        && std::find(containerWindow.at("aggregate-types")->to_string().begin(),
                     containerWindow.at("aggregate-types")->to_string().end(),
                     magic_enum::enum_integer(avg))
            != containerWindow.at("aggregate-types")->to_string().end();
    NES_TRACE("Window containment possible after aggregation and join test: {}", windowContainmentPossible);
    if (windowContainmentPossible) {
        //if first check successful, check that the contained window size and slide are multiples of the container window size and slide
        windowContainmentPossible =
            containedWindow.at("window-time-size")->get_numeral_int() % containerWindow.at("window-time-size")->get_numeral_int()
                == 0
            && containedWindow.at("window-time-slide")->get_numeral_int()
                    % containerWindow.at("window-time-slide")->get_numeral_int()
                == 0;
        NES_TRACE("Window containment possible after modulo check: {}", windowContainmentPossible);
        if (windowContainmentPossible) {
            //if that is the chase, check that the contained window either has the same slide as the container window or that the slide and size of both windows are equal
            windowContainmentPossible = containedWindow.at("window-time-slide")->get_numeral_int()
                    == containerWindow.at("window-time-slide")->get_numeral_int()
                || (containerWindow.at("window-time-slide")->get_numeral_int()
                        == containerWindow.at("window-time-size")->get_numeral_int()
                    && containedWindow.at("window-time-slide")->get_numeral_int()
                        == containedWindow.at("window-time-size")->get_numeral_int());
            NES_TRACE("Window containment possible after slide check: {}",
                      containedWindow.at("window-time-slide")->to_string()
                          == containerWindow.at("window-time-slide")->to_string());
            NES_TRACE(
                "Window containment possible after slide and size check: {}",
                (containerWindow.at("window-time-slide")->to_string() == containerWindow.at("window-time-size")->to_string()
                 && containedWindow.at("window-time-slide")->to_string() == containedWindow.at("window-time-size")->to_string()));
            if (windowContainmentPossible) {
                //if all other checks passed, check for filter equality
                windowContainmentPossible = (checkFilterContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY);
                NES_TRACE("Window containment possible after filter check: {}", windowContainmentPossible);
            }
        }
    }
    return windowContainmentPossible;
}

bool SignatureContainmentUtil::checkFilterContainmentPossible(const QuerySignaturePtr& container,
                                                              const QuerySignaturePtr& containee) {
    if (container->getSchemaFieldToExprMaps().size() > 1) {
        for (const auto& [containerSourceName, containerConditions] : container->getUnionExpressions()) {
            NES_TRACE("Container source name: {}", containerSourceName);
            NES_TRACE("Container conditions: {}", containerConditions->to_string());
            // Check if the key exists in map2
            auto containeeUnionExpressions = containee->getUnionExpressions().find(containerSourceName);
            if (containeeUnionExpressions == containee->getUnionExpressions().end()) {
                return false;
            }

            // Check if the values are equal
            z3::ExprPtr containeeConditions = containeeUnionExpressions->second;
            NES_TRACE("Containee conditions: {}", containeeConditions->to_string());
            if (containerConditions->to_string() != containeeConditions->to_string()) {
                return false;
            }
        }
    }
    return true;
}

bool SignatureContainmentUtil::checkDownstreamOperatorChainForSingleParent(
    const LogicalOperatorNodePtr& containedOperator,
    const LogicalOperatorNodePtr& extractedContainedOperator) {
    NES_INFO2("What's up with the extracted contained operator: {}", extractedContainedOperator->toString());
    if (extractedContainedOperator->getParents().size() != 1) {
        return false;
    }
    auto parent = extractedContainedOperator->getParents()[0];
    while (!parent->equal(containedOperator)) {
        if (parent->getParents().size() != 1) {
            return false;
        } else {
            parent = parent->getParents()[0];
        }
    }
    return true;
}

bool SignatureContainmentUtil::checkDownstreamOperatorChainForSingleParent(
    const LogicalOperatorNodePtr& containedOperator,
    const LogicalOperatorNodePtr& extractedContainedOperator,
    std::vector<std::string>& mapAttributeNames) {
    NES_DEBUG2("extractedContainedOperator parents size: {}", extractedContainedOperator->getParents().size());
    if (extractedContainedOperator->getParents().size() != 1) {
        return false;
    }
    auto parent = extractedContainedOperator->getParents()[0];
    NES_DEBUG2("Parent: {}", parent->toString());
    if (parent->instanceOf<UnionLogicalOperatorNode>()) {
        return false;
    }
    while (!parent->equal(containedOperator)) {
        NES_DEBUG2("Parent: {}", parent->toString());
        if (parent->getParents().size() != 1 || parent->instanceOf<WindowLogicalOperatorNode>()
            || parent->instanceOf<UnionLogicalOperatorNode>()) {
            return false;
        } else {
            if (parent->instanceOf<MapLogicalOperatorNode>()) {
                auto mapOperator = parent->as<MapLogicalOperatorNode>();
                mapAttributeNames.push_back(getFieldNameUsedByMapOperator(mapOperator));
            }
            parent = parent->getParents()[0];
        }
    }
    return true;
}

void SignatureContainmentUtil::resetSolver() {
    solver->reset();
    counter = 0;
}
}// namespace NES::Optimizer

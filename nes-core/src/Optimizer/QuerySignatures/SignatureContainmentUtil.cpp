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
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
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
SignatureContainmentUtil::checkContainmentForTopDownMerging(const LogicalOperatorNodePtr& leftOperator,
                                                            const LogicalOperatorNodePtr& rightOperator) {
    NES_TRACE("Checking for containment.");
    ContainmentType containmentRelationship = ContainmentType::NO_CONTAINMENT;
    std::vector<LogicalOperatorNodePtr> containmentOperators = {};
    auto otherConditions = leftOperator->getZ3Signature()->getConditions();
    auto conditions = rightOperator->getZ3Signature()->getConditions();
    NES_TRACE("Left signature: {}", conditions->to_string());
    NES_TRACE("Right signature: {}", otherConditions->to_string());
    if (!conditions || !otherConditions) {
        NES_WARNING("Can't obtain containment relationships for null signatures");
        return {};
    }
    try {
        // In the following, we
        // First check for WindowContainment
        // In case of window equality, we continue to check for projection containment
        // In case of projection equality, we finally check for filter containment
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
    // We first check if the first order logic (FOL) is equal for projections, if not we move on to check for containment relationships.
    // We added heuristic checks to prevent unnecessary calls to the SMT solver
    // if (!rightFOL && leftFOL == unsat, aka leftFOL ⊆ rightFOL
    //      && filters are equal)
    //        if (rightFOL && !leftFOL == unsat, aka rightFOL ⊆ leftFOL)
    //            true: return Equality
    //      true: return Right sig contained
    // else if (rightFOL && !leftFOL == unsat, aka rightFOL ⊆ leftFOL)
    //      && filters are equal
    //      true: return Left sig contained
    // else: No_Containment
    if (checkContainmentConditionsUnsatisfied(rightQueryProjectionFOL, leftQueryProjectionFOL)) {
        if (checkContainmentConditionsUnsatisfied(leftQueryProjectionFOL, rightQueryProjectionFOL)
            && checkAttributeOrder(leftSignature, rightSignature)) {
            NES_TRACE("Equal projection.");
            return ContainmentType::EQUALITY;
        } else if (checkFilterContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY) {
            return ContainmentType::RIGHT_SIG_CONTAINED;
        }
    } else if (checkContainmentConditionsUnsatisfied(leftQueryProjectionFOL, rightQueryProjectionFOL)
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
    NES_TRACE2("Number of windows: {}", numberOfWindows);
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
                if (checkContainmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)) {
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
        } else if (checkFilterContainmentPossible(leftSignature, rightSignature)) {
            NES_TRACE("left sig contains right sig for filters.");
            return ContainmentType::RIGHT_SIG_CONTAINED;
        }
    } else if (checkContainmentConditionsUnsatisfied(rightQueryFilterConditions, leftQueryFilterConditions)
               && checkFilterContainmentPossible(rightSignature, leftSignature)) {
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
    return containmentOperators;
}

LogicalOperatorNodePtr SignatureContainmentUtil::createProjectionOperator(const LogicalOperatorNodePtr& containedOperator) {
    auto projectionOperators = containedOperator->getNodesByType<ProjectionLogicalOperatorNode>();
    //get the correct projection operator
    if (!projectionOperators.empty()) {
        return projectionOperators.at(0);
    }
    return nullptr;
}

std::vector<LogicalOperatorNodePtr> SignatureContainmentUtil::createFilterOperators(const LogicalOperatorNodePtr& container,
                                                                                    const LogicalOperatorNodePtr& containee) {
    std::vector<LogicalOperatorNodePtr> containmentOperators = {};
    for (const auto& [attributeName, isMapFunctionApplied] :
         containee->getZ3Signature()->getFilterAttributesAndIsMapFunctionApplied()) {
        NES_TRACE2("Check if a map function {} was applied to {}.", isMapFunctionApplied, attributeName);
        if (!isMapFunctionApplied) {
            auto attributeStillPresent = std::binary_search(container->getZ3Signature()->getColumns().begin(),
                                                            container->getZ3Signature()->getColumns().end(),
                                                            attributeName);
            if (attributeStillPresent) {
                for (const auto& item : containee->getNodesByType<FilterLogicalOperatorNode>()) {
                    containmentOperators.push_back(item);
                }
                return containmentOperators;
            }
        }
    }
    return containmentOperators;
}

void SignatureContainmentUtil::createProjectionFOL(const QuerySignaturePtr& signature, z3::expr_vector& projectionFOL) {
    //check projection containment
    // if we are given a map value for the attribute, we create a FOL as attributeStringName == mapCondition, e.g. age == 25
    // else we indicate that the attribute is involved in the projection as attributeStingName == true
    // all FOL are added to the projectionCondition vector
    z3::expr_vector createSourceFOL(*context);
    NES_TRACE("Length of schemaFieldToExprMaps: {}", signature->getSchemaFieldToExprMaps().size());
    for (auto schemaFieldToExpressionMap : signature->getSchemaFieldToExprMaps()) {
        for (auto [attributeName, z3Expression] : schemaFieldToExpressionMap) {
            NES_TRACE("SignatureContainmentUtil::createProjectionFOL: strings: {}", attributeName);
            NES_TRACE("SignatureContainmentUtil::createProjectionFOL: z3 expressions: {}", z3Expression->to_string());
            z3::ExprPtr expr = std::make_shared<z3::expr>(context->bool_const((attributeName.c_str())));
            if (z3Expression->to_string() != attributeName) {
                if (z3Expression->is_int()) {
                    expr = std::make_shared<z3::expr>(context->int_const(attributeName.c_str()));
                } else if (z3Expression->is_fpa()) {
                    expr = std::make_shared<z3::expr>(context->fpa_const<64>(attributeName.c_str()));
                } else if (z3Expression->is_string_value()) {
                    expr = std::make_shared<z3::expr>(context->string_const(attributeName.c_str()));
                }
                createSourceFOL.push_back(to_expr(*context, Z3_mk_eq(*context, *expr, *z3Expression)));
            } else {
                z3::ExprPtr columnIsUsed = std::make_shared<z3::expr>(context->bool_val(true));
                createSourceFOL.push_back(to_expr(*context, Z3_mk_eq(*context, *expr, *columnIsUsed)));
            }
        }
    }
    projectionFOL.push_back(to_expr(*context, mk_and(createSourceFOL)));
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

void SignatureContainmentUtil::resetSolver() {
    solver->reset();
    counter = 0;
}
}// namespace NES::Optimizer

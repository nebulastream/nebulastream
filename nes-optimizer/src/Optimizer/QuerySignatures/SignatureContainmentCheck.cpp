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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Optimizer/QuerySignatures/ContainedOperatorsUtil.hpp>
#include <Optimizer/QuerySignatures/ContainmentRelationshipAndOperatorChain.hpp>
#include <Util/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentCheck.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>

namespace NES::Optimizer {

SignatureContainmentCheckPtr SignatureContainmentCheck::create(const z3::ContextPtr& context,
                                                               bool allowExhaustiveContainmentCheck) {
    return std::make_shared<SignatureContainmentCheck>(context, allowExhaustiveContainmentCheck);
}

SignatureContainmentCheck::SignatureContainmentCheck(const z3::ContextPtr& context, bool allowExhaustiveContainmentCheck) {
    this->context = context;
    this->solver = std::make_unique<z3::solver>(*this->context);
    this->allowExhaustiveContainmentCheck = allowExhaustiveContainmentCheck;
}

ContainmentRelationshipAndOperatorChainPtr
SignatureContainmentCheck::checkContainmentForBottomUpMerging(const LogicalOperatorPtr& leftOperator,
                                                              const LogicalOperatorPtr& rightOperator) {
    NES_TRACE("Checking for containment.");
    ContainmentRelationship containmentRelationship = ContainmentRelationship::NO_CONTAINMENT;
    auto otherConditions = rightOperator->getZ3Signature()->getConditions();
    auto conditions = leftOperator->getZ3Signature()->getConditions();
    NES_TRACE("Left signature: {}", conditions->to_string());
    NES_TRACE("Right signature: {}", otherConditions->to_string());
    if (!conditions || !otherConditions) {
        NES_WARNING("Can't obtain containment relationships for null signatures");
        return ContainmentRelationshipAndOperatorChain::create(containmentRelationship, {});
    }
    auto leftSignature = leftOperator->getZ3Signature();
    auto rightSignature = rightOperator->getZ3Signature();
    try {
        // In the following, we
        // First check for WindowContainment
        // In case of window equality, we continue to check for projection containment
        // In case of projection equality, we finally check for filter containment
        containmentRelationship = checkWindowContainment(leftSignature, rightSignature);
        NES_TRACE("Check window containment returned: {}", magic_enum::enum_name(containmentRelationship));
        if (containmentRelationship == ContainmentRelationship::EQUALITY) {
            containmentRelationship = checkProjectionContainment(leftSignature, rightSignature);
            NES_TRACE("Check projection containment returned: {}", magic_enum::enum_name(containmentRelationship));
            if (containmentRelationship == ContainmentRelationship::EQUALITY) {
                containmentRelationship = checkFilterContainment(leftSignature, rightSignature);
                if (containmentRelationship == ContainmentRelationship::LEFT_SIG_CONTAINED
                    || containmentRelationship == ContainmentRelationship::RIGHT_SIG_CONTAINED) {
                    if (!checkFilterContainmentPossible(rightSignature, leftSignature)) {
                        containmentRelationship = ContainmentRelationship::NO_CONTAINMENT;
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
    return ContainmentRelationshipAndOperatorChain::create(containmentRelationship, {});
}

ContainmentRelationshipAndOperatorChainPtr
SignatureContainmentCheck::checkContainmentRelationshipForTopDownMerging(const LogicalOperatorPtr& leftOperator,
                                                                         const LogicalOperatorPtr& rightOperator) {
    NES_TRACE("Checking for containment.");
    ContainmentRelationship containmentRelationship = ContainmentRelationship::NO_CONTAINMENT;
    if (leftOperator->instanceOf<WatermarkAssignerLogicalOperator>()
        || rightOperator->instanceOf<WatermarkAssignerLogicalOperator>()) {
        NES_TRACE("Watermark assigner detected. Skipping containment check.");
        return ContainmentRelationshipAndOperatorChain::create(containmentRelationship, {});
    }
    std::vector<LogicalOperatorPtr> containmentOperators = {};
    try {
        auto otherConditions = leftOperator->getZ3Signature()->getConditions();
        NES_TRACE("Left signature: {}", otherConditions->to_string());
        auto conditions = rightOperator->getZ3Signature()->getConditions();
        NES_TRACE("Right signature: {}", conditions->to_string());
        if (!conditions || !otherConditions) {
            NES_WARNING("Can't obtain containment relationships for null signatures");
            return ContainmentRelationshipAndOperatorChain::create(containmentRelationship, {});
        }
        // In the following, we
        // First check for WindowContainment
        // In case of window equality, we continue to check for projection containment
        // In case of projection equality, we finally check for filter containment
        // If we detect a containment relationship at any point in the algorithm, we stop and extract the contained upstream operators
        // Additionally, if we detect a containment relationship, we also check that the other relationships allow the containment relationship
        // i.e.: 1. Window containment requires equal filters and either equal projections or a containment relationship in the same direction
        // 2. Projection containment also checks for equal filters (window containment check already returned equality)
        // 3. Before we can detect a filter containment relationship, we need to know that the window and projection containment checks returned equality
        containmentRelationship = checkWindowContainment(leftOperator->getZ3Signature(), rightOperator->getZ3Signature());
        NES_TRACE("Check window containment returned: {}", magic_enum::enum_name(containmentRelationship));
        if (containmentRelationship == ContainmentRelationship::EQUALITY) {
            containmentRelationship = checkProjectionContainment(leftOperator->getZ3Signature(), rightOperator->getZ3Signature());
            NES_TRACE("Check projection containment returned: {}", magic_enum::enum_name(containmentRelationship));
            LogicalOperatorPtr containedOperator = nullptr;
            if (containmentRelationship == ContainmentRelationship::EQUALITY) {
                containmentRelationship = checkFilterContainment(leftOperator->getZ3Signature(), rightOperator->getZ3Signature());
                NES_TRACE("Check filter containment returned: {}", magic_enum::enum_name(containmentRelationship));
                if (containmentRelationship == ContainmentRelationship::EQUALITY) {
                    return ContainmentRelationshipAndOperatorChain::create(ContainmentRelationship::EQUALITY, {});
                    // Create the contained operator chain, i.e., extract all filter operators from the containee (if possible, more details on that inside ContainedOperatorUtil),
                    // and concatenate the filter predicates so that we return one filter operator with all contained filter operations
                } else if (containmentRelationship == ContainmentRelationship::RIGHT_SIG_CONTAINED) {
                    containedOperator = ContainedOperatorsUtil::createContainedFilterOperators(leftOperator, rightOperator);
                } else if (containmentRelationship == ContainmentRelationship::LEFT_SIG_CONTAINED) {
                    containedOperator = ContainedOperatorsUtil::createContainedFilterOperators(rightOperator, leftOperator);
                }
                // Create the contained operator chain, i.e., extract the most downstream projection operator from the containee
                // (if possible, more details on that inside ContainedOperatorUtil)
            } else if (containmentRelationship == ContainmentRelationship::RIGHT_SIG_CONTAINED) {
                containedOperator = ContainedOperatorsUtil::createContainedProjectionOperator(rightOperator);
            } else if (containmentRelationship == ContainmentRelationship::LEFT_SIG_CONTAINED) {
                containedOperator = ContainedOperatorsUtil::createContainedProjectionOperator(leftOperator);
            }
            if (containedOperator) {
                containmentOperators.push_back(containedOperator);
            }
            // Create the contained operator chain, i.e., extract the most downstream windwo operator and its associated watermark assigner from the containee
        } else if (containmentRelationship == ContainmentRelationship::RIGHT_SIG_CONTAINED) {
            containmentOperators = ContainedOperatorsUtil::createContainedWindowOperator(rightOperator, leftOperator);
        } else if (containmentRelationship == ContainmentRelationship::LEFT_SIG_CONTAINED) {
            containmentOperators = ContainedOperatorsUtil::createContainedWindowOperator(leftOperator, rightOperator);
        }
        if (containmentOperators.empty()) {
            containmentRelationship = ContainmentRelationship::NO_CONTAINMENT;
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
    return ContainmentRelationshipAndOperatorChain::create(containmentRelationship, containmentOperators);
}

ContainmentRelationship SignatureContainmentCheck::checkProjectionContainment(const QuerySignaturePtr& leftSignature,
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
            return ContainmentRelationship::EQUALITY;
        } else if (checkFilterContainment(leftSignature, rightSignature) == ContainmentRelationship::EQUALITY) {
            return ContainmentRelationship::RIGHT_SIG_CONTAINED;
        }
    } else if (allowExhaustiveContainmentCheck
               && checkContainmentConditionsUnsatisfied(leftQueryProjectionFOL, rightQueryProjectionFOL)
               && checkForEqualTransformations(leftSignature, rightSignature)
               && checkFilterContainment(leftSignature, rightSignature) == ContainmentRelationship::EQUALITY) {
        return ContainmentRelationship::LEFT_SIG_CONTAINED;
    }
    return ContainmentRelationship::NO_CONTAINMENT;
}

ContainmentRelationship SignatureContainmentCheck::checkWindowContainment(const QuerySignaturePtr& leftSignature,
                                                                          const QuerySignaturePtr& rightSignature) {
    // if no window signature is present, return equality
    if (leftSignature->getWindowsExpressions().empty() && rightSignature->getWindowsExpressions().empty()) {
        //0 indicates that there are no window operations in the queries
        return ContainmentRelationship::EQUALITY;
    }
    // if the number of window operations is not the same, we cannot detect containment relationships for these queries
    if (rightSignature->getWindowsExpressions().size() != leftSignature->getWindowsExpressions().size()) {
        return ContainmentRelationship::NO_CONTAINMENT;
    }
    // each vector entry in the windowExpressions vector represents the signature of one window
    // we assume a bottom up approach for our containment algorithm, hence a window operation can only be partially shared if
    // the previous operations are completely sharable. As soon as there is no equality in window operations, we return the
    // obtained relationship
    ContainmentRelationship containmentRelationship = ContainmentRelationship::NO_CONTAINMENT;
    for (size_t i = 0; i < rightSignature->getWindowsExpressions().size(); ++i) {
        // obtain each window signature in bottom up fashion
        const auto& leftWindow = leftSignature->getWindowsExpressions()[i];
        const auto& rightWindow = rightSignature->getWindowsExpressions()[i];
        NES_TRACE("Starting with left window: {}", leftWindow.at("z3-window-expressions")->to_string());
        NES_TRACE("Starting with right window: {}", rightWindow.at("z3-window-expressions")->to_string());
        // checks if the window ids are equal, operator sharing can only happen for equal window ids
        // the window id consists of the involved window-keys, and the time stamp attribute
        // extract the z3-window-expressions
        z3::expr_vector leftQueryWindowConditions(*context);
        z3::expr_vector rightQueryWindowConditions(*context);
        leftQueryWindowConditions.push_back(to_expr(*context, *leftWindow.at("z3-window-expressions")));
        rightQueryWindowConditions.push_back(to_expr(*context, *rightWindow.at("z3-window-expressions")));
        NES_TRACE("Created window FOL.");
        // checks if the number of aggregates is equal, for equal number of aggregates we
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
                    containmentRelationship = ContainmentRelationship::EQUALITY;
                }
                // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
                // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
                // additionally, we also check for projection equality
                else if (i + 1 == rightSignature->getWindowsExpressions().size()
                         && checkWindowContainmentPossible(leftWindow, rightWindow, leftSignature, rightSignature)
                         && (checkProjectionContainment(leftSignature, rightSignature) == ContainmentRelationship::EQUALITY)) {
                    NES_TRACE("Right window contained.");
                    containmentRelationship = ContainmentRelationship::RIGHT_SIG_CONTAINED;
                } else {
                    containmentRelationship = ContainmentRelationship::NO_CONTAINMENT;
                }
                // first, we check that there is a window containment relationship then
                // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
                // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
                // additionally, we also check for projection equality
            } else if (allowExhaustiveContainmentCheck && i + 1 == rightSignature->getWindowsExpressions().size()
                       && checkContainmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)
                       && checkWindowContainmentPossible(rightWindow, leftWindow, leftSignature, rightSignature)
                       && (checkProjectionContainment(leftSignature, rightSignature) == ContainmentRelationship::EQUALITY)) {
                NES_TRACE("Left window contained.");
                containmentRelationship = ContainmentRelationship::LEFT_SIG_CONTAINED;
            } else {
                containmentRelationship = ContainmentRelationship::NO_CONTAINMENT;
            }
            // checks if the number of aggregates for the left signature is smaller than the number of aggregates for the right
            // signature
        } else if (i + 1 == rightSignature->getWindowsExpressions().size()
                   && leftWindow.at("number-of-aggregates")->get_numeral_int()
                       < rightWindow.at("number-of-aggregates")->get_numeral_int()) {
            NES_TRACE("Right Window has more Aggregates than left Window.");
            combineWindowAndProjectionFOL(leftSignature, rightSignature, leftQueryWindowConditions, rightQueryWindowConditions);
            // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
            // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
            // then check if the left window is contained
            if (allowExhaustiveContainmentCheck
                && checkWindowContainmentPossible(rightWindow, leftWindow, leftSignature, rightSignature)
                && checkContainmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)) {
                NES_TRACE("Left window contained.");
                containmentRelationship = ContainmentRelationship::LEFT_SIG_CONTAINED;
            } else {
                containmentRelationship = ContainmentRelationship::NO_CONTAINMENT;
            }
        }
        // checks if the number of aggregates for the left signature is larger than the number of aggregates for the right
        // signature
        else if (i + 1 == rightSignature->getWindowsExpressions().size()
                 && leftWindow.at("number-of-aggregates")->get_numeral_int()
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
                containmentRelationship = ContainmentRelationship::RIGHT_SIG_CONTAINED;
            } else {
                containmentRelationship = ContainmentRelationship::NO_CONTAINMENT;
            }
        }
        // stop the loop as soon as there is no equality relationship
        if (containmentRelationship != ContainmentRelationship::EQUALITY) {
            break;
        }
    }
    return containmentRelationship;
}

ContainmentRelationship SignatureContainmentCheck::checkFilterContainment(const QuerySignaturePtr& leftSignature,
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
            return ContainmentRelationship::EQUALITY;
        }
        NES_TRACE("left sig contains right sig for filters.");
        return ContainmentRelationship::RIGHT_SIG_CONTAINED;
    } else if (allowExhaustiveContainmentCheck
               && checkContainmentConditionsUnsatisfied(rightQueryFilterConditions, leftQueryFilterConditions)) {
        NES_TRACE("right sig contains left sig for filters.");
        return ContainmentRelationship::LEFT_SIG_CONTAINED;
    }
    return ContainmentRelationship::NO_CONTAINMENT;
}

void SignatureContainmentCheck::createProjectionFOL(const QuerySignaturePtr& signature, z3::expr_vector& projectionFOL) {
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

bool SignatureContainmentCheck::checkForEqualTransformations(const QuerySignaturePtr& leftSignature,
                                                             const QuerySignaturePtr& rightSignature) {
    auto schemaFieldToExprMaps = leftSignature->getSchemaFieldToExprMaps();
    auto otherSchemaFieldToExprMaps = rightSignature->getSchemaFieldToExprMaps();
    auto otherColumns = rightSignature->getColumns();
    auto containedAttributes = leftSignature->getColumns();
    //Extract the smaller attribute list, we only want to know it the transformations are equal for the smaller list
    if (containedAttributes.size() > otherColumns.size()) {
        containedAttributes = otherColumns;
    }
    //iterate over all entries in the vector schemaFieldToExprMaps
    for (auto schemaFieldToExprMap : schemaFieldToExprMaps) {
        bool schemaMatched = false;
        //iterate over all entries in the other schemaFieldToExprMaps vector
        for (auto otherSchemaMapItr = otherSchemaFieldToExprMaps.begin(); otherSchemaMapItr != otherSchemaFieldToExprMaps.end();
             otherSchemaMapItr++) {
            z3::expr_vector colChecks(*context);
            for (uint64_t index = 0; index < containedAttributes.size(); index++) {
                //extract the transformations for the given attributes for both queries from the schemaFieldToExprMaps
                auto colExpr = schemaFieldToExprMap[containedAttributes[index]];
                auto otherColExpr = (*otherSchemaMapItr)[containedAttributes[index]];
                auto equivalenceCheck = to_expr(*context, Z3_mk_eq(*context, *colExpr, *otherColExpr));
                NES_TRACE("Equivalence check for transformations: {} on attribute {}",
                          equivalenceCheck.to_string(),
                          containedAttributes[index]);
                colChecks.push_back(equivalenceCheck);
            }

            //conjunct the transformations and then negate the expression
            solver->push();
            solver->add(!z3::mk_and(colChecks).simplify());
            NES_TRACE("Content of equivalence solver: {}", solver->to_smt2());
            //check if the transformations are equal
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

bool SignatureContainmentCheck::checkContainmentConditionsUnsatisfied(z3::expr_vector& negatedCondition,
                                                                      z3::expr_vector& condition) {
    solver->push();
    solver->add(!mk_and(negatedCondition).simplify());
    solver->push();
    solver->add(mk_and(condition).simplify());
    NES_TRACE("Check unsat: {}; Content of solver: {}", solver->check(), solver->to_smt2());
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

bool SignatureContainmentCheck::checkAttributeOrder(const QuerySignaturePtr& leftSignature,
                                                    const QuerySignaturePtr& rightSignature) const {
    for (size_t j = 0; j < rightSignature->getColumns().size(); ++j) {
        NES_TRACE("Containment order check for {} and {}", rightSignature->getColumns()[j], leftSignature->getColumns()[j]);
        if (leftSignature->getColumns()[j] != rightSignature->getColumns()[j]) {
            return false;
        }
    }
    return true;
}

void SignatureContainmentCheck::combineWindowAndProjectionFOL(const QuerySignaturePtr& leftSignature,
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

bool SignatureContainmentCheck::checkWindowContainmentPossible(const std::map<std::string, z3::ExprPtr>& containerWindow,
                                                               const std::map<std::string, z3::ExprPtr>& containedWindow,
                                                               const QuerySignaturePtr& leftSignature,
                                                               const QuerySignaturePtr& rightSignature) {
    const auto& median = Windowing::WindowAggregationDescriptor::Type::Median;
    const auto& avg = Windowing::WindowAggregationDescriptor::Type::Avg;
    NES_TRACE("Current window-id: {}, Current window-id != JoinWindow: {}",
              containerWindow.at("window-id")->to_string(),
              containerWindow.at("window-id")->to_string() != "\"JoinWindow\"");

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

            NES_TRACE(
                "Window containment possible after slide check: {}; Window containment possible after slide and size check: {}",
                containedWindow.at("window-time-slide")->to_string() == containerWindow.at("window-time-slide")->to_string(),
                (containerWindow.at("window-time-slide")->to_string() == containerWindow.at("window-time-size")->to_string()
                 && containedWindow.at("window-time-slide")->to_string() == containedWindow.at("window-time-size")->to_string()));

            if (windowContainmentPossible) {
                //if all other checks passed, check for filter equality
                windowContainmentPossible =
                    (checkFilterContainment(leftSignature, rightSignature) == ContainmentRelationship::EQUALITY);
                NES_TRACE("Window containment possible after filter check: {}", windowContainmentPossible);
            }
        }
    }
    return windowContainmentPossible;
}

bool SignatureContainmentCheck::checkFilterContainmentPossible(const QuerySignaturePtr& container,
                                                               const QuerySignaturePtr& containee) {
    if (container->getSchemaFieldToExprMaps().size() > 1) {
        for (const auto& [containerSourceName, containerConditions] : container->getUnionExpressions()) {
            NES_TRACE("Container source name: {}; Container conditions: {}",
                      containerSourceName,
                      containerConditions->to_string());
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

void SignatureContainmentCheck::resetSolver() {
    solver->reset();
    counter = 0;
}
}// namespace NES::Optimizer

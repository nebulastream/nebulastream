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

#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>

namespace NES::Optimizer {

SignatureContainmentUtilPtr SignatureContainmentUtil::create(const z3::ContextPtr& context) {
    return std::make_shared<SignatureContainmentUtil>(context);
}

SignatureContainmentUtil::SignatureContainmentUtil(const z3::ContextPtr& context) {
    this->context = context;
    this->solver = std::make_unique<z3::solver>(*this->context);
}

ContainmentType SignatureContainmentUtil::checkContainment(const QuerySignaturePtr& leftSignature,
                                                           const QuerySignaturePtr& rightSignature) {
    NES_TRACE2("Checking for containment.");
    ContainmentType containmentRelationship = ContainmentType::NO_CONTAINMENT;
    auto otherConditions = rightSignature->getConditions();
    auto conditions = leftSignature->getConditions();
    NES_TRACE2("Left signature: {}", conditions->to_string());
    NES_TRACE2("Right signature: {}", otherConditions->to_string());
    if (!conditions || !otherConditions) {
        NES_WARNING("Can't obtain containment relationships for null signatures");
        return ContainmentType::NO_CONTAINMENT;
    }
    try {
        // In the following, we
        // First check for WindowContainment
        // In case of window equality, we continue to check for projection containment
        // In case of projection equality, we finally check for filter containment
        containmentRelationship = checkWindowContainment(leftSignature, rightSignature);
        NES_TRACE2("Check window containment returned: {}", magic_enum::enum_name(containmentRelationship));
        if (containmentRelationship == ContainmentType::EQUALITY) {
            containmentRelationship = checkProjectionContainment(leftSignature, rightSignature);
            NES_TRACE2("Check projection containment returned: {}", magic_enum::enum_name(containmentRelationship));
            if (containmentRelationship == ContainmentType::EQUALITY) {
                containmentRelationship = checkFilterContainment(leftSignature, rightSignature);
                NES_TRACE2("Check filter containment returned: {}", magic_enum::enum_name(containmentRelationship));
            }
        }
    } catch (...) {
        auto exception = std::current_exception();
        try {
            std::rethrow_exception(exception);
        } catch (const std::exception& e) {
            NES_ERROR2("SignatureContainmentUtil: Exception occurred while performing containment check among "
                       "queryIdAndCatalogEntryMapping {}",
                       e.what());
        }
    }
    return containmentRelationship;
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
    // if (!leftProjectionFOL && !rightProjectionFOL) == unsat
    //     && the column order is the same)
    //      true: return EQUALITY
    //      false:
    //          if (# of attr left sig > # of attr right sig)
    //              && !rightFOL && leftFOL == unsat, aka leftFOL ⊆ rightFOL
    //              && filters are equal
    //                  true: return Right sig contained
    //          else if (# of attr left sig < # of attr right sig)
    //              && (rightFOL && !leftFOL == unsat, aka rightFOL ⊆ leftFOL)
    //              && filters are equal
    //                  true: return Left sig contained
    // else: No_Containment
    if (checkEqualityConditionsUnsatisfied(leftQueryProjectionFOL, rightQueryProjectionFOL)
        && checkAttributeOrder(leftSignature, rightSignature)) {
        NES_TRACE2("Equal projection.");
        return ContainmentType::EQUALITY;
    } else if (leftSignature->getSchemaFieldToExprMaps().size() == rightSignature->getSchemaFieldToExprMaps().size()) {
        for (size_t i = 0; i < leftSignature->getSchemaFieldToExprMaps().size(); ++i) {
            if (leftSignature->getSchemaFieldToExprMaps()[i].size() > rightSignature->getSchemaFieldToExprMaps()[i].size()
                && checkContainmentConditionsUnsatisfied(rightQueryProjectionFOL, leftQueryProjectionFOL)
                && checkFilterContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY) {
                return ContainmentType::RIGHT_SIG_CONTAINED;
            } else if (leftSignature->getSchemaFieldToExprMaps()[i].size() < rightSignature->getSchemaFieldToExprMaps()[i].size()
                       && checkContainmentConditionsUnsatisfied(leftQueryProjectionFOL, rightQueryProjectionFOL)
                       && checkFilterContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY) {
                return ContainmentType::LEFT_SIG_CONTAINED;
            }
        }
    }
    return ContainmentType::NO_CONTAINMENT;
}

ContainmentType SignatureContainmentUtil::checkWindowContainment(const QuerySignaturePtr& leftSignature,
                                                                 const QuerySignaturePtr& rightSignature) {
    // if no window signature is present, return equality
    if (leftSignature->getWindowsExpressions().empty() && rightSignature->getWindowsExpressions().empty()) {
        return ContainmentType::EQUALITY;
    }
    // obtain the number of window operations. Use the number of window operations from the signature that has less window operations
    size_t numberOfWindows = rightSignature->getWindowsExpressions().size();
    if (numberOfWindows > leftSignature->getWindowsExpressions().size()) {
        numberOfWindows = leftSignature->getWindowsExpressions().size();
    }
    // each vector entry in the windowExpressions vector represents the signature of one window
    // we assume a bottom up approach for our containment algorithm, hence a window operation can only be partially shared if
    // the previous operations are completely sharable. As soon as there is no equality in window operations, we return the
    // obtained relationship
    //todo: #3503 introduce a mechanism to identify which window is contained in the presence of multiple windows
    ContainmentType containmentRelationship = ContainmentType::NO_CONTAINMENT;
    for (size_t i = 0; i < numberOfWindows; ++i) {
        // obtain each window signature in bottom up fashion
        const auto& leftWindow = leftSignature->getWindowsExpressions()[i];
        const auto& rightWindow = rightSignature->getWindowsExpressions()[i];
        NES_TRACE2("Starting with left window: {}", leftWindow.at("z3-window-expressions")->to_string());
        NES_TRACE2("Starting with right window: {}", rightWindow.at("z3-window-expressions")->to_string());
        // checks if the window ids are equal, operator sharing can only happen for equal window ids
        // the window id consists of the involved window-keys, and the time stamp attribute
        if (leftWindow.at("window-id")->to_string() == rightWindow.at("window-id")->to_string()) {
            NES_TRACE2("Same window ids.");
            //extract the z3-window-expressions
            z3::expr_vector leftQueryWindowConditions(*context);
            z3::expr_vector rightQueryWindowConditions(*context);
            leftQueryWindowConditions.push_back(to_expr(*context, *leftWindow.at("z3-window-expressions")));
            rightQueryWindowConditions.push_back(to_expr(*context, *rightWindow.at("z3-window-expressions")));
            NES_TRACE2("Created window FOL.");
            //checks if the number of aggregates is equal, for equal number of aggregates we
            // 1. check for complete equality
            // 2. check if a containment relationship exists,
            // e.g. z3 checks leftWindow-size <= rightWindow-size && leftWindow-slide <= rightWindow-slide
            NES_TRACE2("Number of Aggregates left window: {}", leftWindow.at("number-of-aggregates")->to_string());
            NES_TRACE2("Number of Aggregates right window: {}", rightWindow.at("number-of-aggregates")->to_string());
            if (leftWindow.at("number-of-aggregates")->get_numeral_int()
                == rightWindow.at("number-of-aggregates")->get_numeral_int()) {
                NES_TRACE2("Same number of aggregates.");
                if (checkContainmentConditionsUnsatisfied(rightQueryWindowConditions, leftQueryWindowConditions)) {
                    if (checkContainmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)) {
                        NES_TRACE2("Equal windows.");
                        containmentRelationship = ContainmentType::EQUALITY;
                    }
                    // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
                    // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
                    // additionally, we also check for projection equality
                    else if (checkWindowContainmentPossible(leftWindow, leftSignature, rightSignature)
                             && (checkProjectionContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY)) {
                        NES_TRACE2("Right window contained.");
                        containmentRelationship = ContainmentType::RIGHT_SIG_CONTAINED;
                    } else {
                        containmentRelationship = ContainmentType::NO_CONTAINMENT;
                    }
                    // first, we check that there is a window containment relationship then
                    // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
                    // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
                    // additionally, we also check for projection equality
                } else if (checkContainmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)
                           && checkWindowContainmentPossible(leftWindow, leftSignature, rightSignature)
                           && (checkProjectionContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY)) {
                    NES_TRACE2("Left window contained.");
                    containmentRelationship = ContainmentType::LEFT_SIG_CONTAINED;
                } else {
                    containmentRelationship = ContainmentType::NO_CONTAINMENT;
                }
                // checks if the number of aggregates for the left signature is smaller than the number of aggregates for the right
                // signature
            } else if (leftWindow.at("number-of-aggregates")->get_numeral_int()
                       < rightWindow.at("number-of-aggregates")->get_numeral_int()) {
                NES_TRACE2("Right Window has more Aggregates than left Window.");
                combineWindowAndProjectionFOL(leftSignature,
                                              rightSignature,
                                              leftQueryWindowConditions,
                                              rightQueryWindowConditions);
                // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
                // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
                // then check if the left window is contained
                if (checkWindowContainmentPossible(leftWindow, leftSignature, rightSignature)
                    && checkContainmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)) {
                    NES_TRACE2("Left window contained.");
                    containmentRelationship = ContainmentType::RIGHT_SIG_CONTAINED;
                } else {
                    containmentRelationship = ContainmentType::NO_CONTAINMENT;
                }
                // checks if the number of aggregates for the left signature is larger than the number of aggregates for the right
                // signature
            } else if (leftWindow.at("number-of-aggregates")->get_numeral_int()
                       > rightWindow.at("number-of-aggregates")->get_numeral_int()) {
                NES_TRACE2("Left Window has more Aggregates than right Window.");
                // combines window and projection FOL to find out containment relationships
                combineWindowAndProjectionFOL(leftSignature,
                                              rightSignature,
                                              leftQueryWindowConditions,
                                              rightQueryWindowConditions);
                // checkWindowContainmentPossible makes sure that filters are equal and no operations are included that cannot
                // be contained, i.e. Joins, Avg, and Median windows cannot share operations unless they are equal
                // then check if the right window is contained
                if (checkWindowContainmentPossible(leftWindow, leftSignature, rightSignature)
                    && checkContainmentConditionsUnsatisfied(rightQueryWindowConditions, leftQueryWindowConditions)) {
                    NES_TRACE2("Right window contained.");
                    containmentRelationship = ContainmentType::LEFT_SIG_CONTAINED;
                } else {
                    containmentRelationship = ContainmentType::NO_CONTAINMENT;
                }
            }
        }

        // stop the loop as soon as there is no equality relationship
        if (containmentRelationship != ContainmentType::EQUALITY) {
            return containmentRelationship;
        }
    }
    return containmentRelationship;
}

ContainmentType SignatureContainmentUtil::checkFilterContainment(const QuerySignaturePtr& leftSignature,
                                                                 const QuerySignaturePtr& rightSignature) {
    NES_TRACE2("SignatureContainmentUtil::checkContainment: Create new condition vectors.");
    z3::expr_vector leftQueryFilterConditions(*context);
    z3::expr_vector rightQueryFilterConditions(*context);
    NES_TRACE2("SignatureContainmentUtil::checkContainment: Add filter conditions.");
    leftQueryFilterConditions.push_back(to_expr(*context, *leftSignature->getConditions()));
    rightQueryFilterConditions.push_back(to_expr(*context, *rightSignature->getConditions()));
    NES_TRACE2("SignatureContainmentUtil::checkContainment: content of left sig expression vectors: {}",
               leftQueryFilterConditions.to_string());
    NES_TRACE2("SignatureContainmentUtil::checkContainment: content of right sig expression vectors: {}",
               rightQueryFilterConditions.to_string());
    //The rest of the method checks for filter containment as follows:
    //check if right sig ⊆ left sig for filters, i.e. if ((right cond && !left condition) == unsat) <=> right sig ⊆ left sig,
    //since we're checking for projection containment, the negation is on the side of the contained condition,
    //e.g. right sig ⊆ left sig <=> (((attr<5 && attr2==6) && !(attr1<=10 && attr2==45)) == unsat)
    //      true: check if left sig ⊆ right sig
    //          true: return EQUALITY
    //          false: return LEFT_SIG_CONTAINED
    //      false: check if left sig ⊆ right sig
    //          true: return RIGHT_SIG_CONTAINED
    //          false: return NO_CONTAINMENT
    if (checkContainmentConditionsUnsatisfied(leftQueryFilterConditions, rightQueryFilterConditions)) {
        if (checkContainmentConditionsUnsatisfied(rightQueryFilterConditions, leftQueryFilterConditions)) {
            NES_TRACE2("SignatureContainmentUtil::checkContainment: Equal filters.");
            return ContainmentType::EQUALITY;
        }
        NES_TRACE2("SignatureContainmentUtil::checkContainment: left sig contains right sig for filters.");
        return ContainmentType::RIGHT_SIG_CONTAINED;
    } else {
        if (checkContainmentConditionsUnsatisfied(rightQueryFilterConditions, leftQueryFilterConditions)) {
            NES_TRACE2("SignatureContainmentUtil::checkContainment: right sig contains left sig for filters.");
            return ContainmentType::LEFT_SIG_CONTAINED;
        }
    }
    return ContainmentType::NO_CONTAINMENT;
}

void SignatureContainmentUtil::createProjectionFOL(const QuerySignaturePtr& signature, z3::expr_vector& projectionFOL) {
    //check projection containment
    // if we are given a map value for the attribute, we create a FOL as attributeStringName == mapCondition, e.g. age == 25
    // else we indicate that the attribute is involved in the projection as attributeStingName == true
    // all FOL are added to the projectionCondition vector
    z3::expr_vector createSourceFOL(*context);
    NES_TRACE2("Length of schemaFieldToExprMaps: {}", signature->getSchemaFieldToExprMaps().size());
    for (auto schemaFieldToExpressionMap : signature->getSchemaFieldToExprMaps()) {
        for (auto [attributeName, z3Expression] : schemaFieldToExpressionMap) {
            NES_TRACE2("SignatureContainmentUtil::createProjectionFOL: strings: {}", attributeName);
            NES_TRACE2("SignatureContainmentUtil::createProjectionFOL: z3 expressions: {}", z3Expression->to_string());
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
    NES_TRACE2("Check unsat: {}", solver->check());
    NES_TRACE2("Content of solver: {}", solver->to_smt2());
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

bool SignatureContainmentUtil::checkEqualityConditionsUnsatisfied(const z3::expr_vector& leftConditions,
                                                                  const z3::expr_vector& rightConditions) {
    z3::expr_vector helper(*context);
    for (uint i = 0; i < leftConditions.size(); ++i) {
        NES_TRACE2("SignatureContainmentUtil::checkContainment: left expr: {}", leftConditions[i].to_string());
        NES_TRACE2("SignatureContainmentUtil::checkContainment: right expr: {}", rightConditions[i].to_string());
        helper.push_back(to_expr(*context, Z3_mk_eq(*context, leftConditions.operator[](i), rightConditions.operator[](i))));
    }
    NES_TRACE2("SignatureContainmentUtil::checkContainment: content of combined expression vectors: {}", helper.to_string());
    solver->push();
    solver->add(!z3::mk_and(helper).simplify());
    NES_TRACE2("Check unsat: {}", solver->check());
    bool conditionUnsatisfied = false;
    if (solver->check() == z3::unsat) {
        conditionUnsatisfied = true;
    }
    solver->pop(1);
    counter++;
    if (counter >= RESET_SOLVER_THRESHOLD) {
        resetSolver();
    }
    return conditionUnsatisfied;
}

bool SignatureContainmentUtil::checkAttributeOrder(const QuerySignaturePtr& leftSignature,
                                                   const QuerySignaturePtr& rightSignature) const {
    for (size_t j = 0; j < rightSignature->getColumns().size(); ++j) {
        NES_TRACE2("Containment order check for {}", rightSignature->getColumns()[j]);
        NES_TRACE2(" and {}", leftSignature->getColumns()[j]);
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

bool SignatureContainmentUtil::checkWindowContainmentPossible(const std::map<std::string, z3::ExprPtr>& currentWindow,
                                                              const QuerySignaturePtr& leftSignature,
                                                              const QuerySignaturePtr& rightSignature) {
    const auto& median = Windowing::WindowAggregationDescriptor::Type::Median;
    const auto& avg = Windowing::WindowAggregationDescriptor::Type::Avg;
    NES_TRACE2("Current window-id: {}", currentWindow.at("window-id")->to_string());
    NES_TRACE2("Current window-id != JoinWindow: {}", currentWindow.at("window-id")->to_string() != "\"JoinWindow\"");
    bool leftWindowContainsRightWindow = currentWindow.at("window-id")->to_string() != "\"JoinWindow\""
        && std::find(currentWindow.at("aggregate-types")->to_string().begin(),
                     currentWindow.at("aggregate-types")->to_string().end(),
                     magic_enum::enum_integer(median))
            != currentWindow.at("aggregate-types")->to_string().end()
        && std::find(currentWindow.at("aggregate-types")->to_string().begin(),
                     currentWindow.at("aggregate-types")->to_string().end(),
                     magic_enum::enum_integer(avg))
            != currentWindow.at("aggregate-types")->to_string().end()
        && (checkFilterContainment(leftSignature, rightSignature) == ContainmentType::EQUALITY);
    return leftWindowContainsRightWindow;
}

void SignatureContainmentUtil::resetSolver() {
    solver->reset();
    counter = 0;
}
}// namespace NES::Optimizer

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
    NES_TRACE("Checking for containment.");
    ContainmentType containmentRelationship = NO_CONTAINMENT;
    auto otherConditions = rightSignature->getConditions();
    auto conditions = leftSignature->getConditions();
    if (!conditions || !otherConditions) {
        NES_WARNING("Can't obtain containment relationships for null signatures");
        return NO_CONTAINMENT;
    }
    try {
        //In the following, we
        // 1. check for WindowContainment
        // In case of window equality, we continue to check for projection containment
        // In case of projection equality, we finally check for filter containment
        containmentRelationship = checkWindowContainment(leftSignature, rightSignature);
        NES_TRACE2("Check window containment returned: {}", containmentRelationship);
        if (containmentRelationship == EQUALITY) {
            containmentRelationship = checkProjectionContainment(leftSignature, rightSignature);
            NES_TRACE2("Check projection containment returned: {}", containmentRelationship);
            if (containmentRelationship == EQUALITY) {
                containmentRelationship = checkFilterContainment(leftSignature, rightSignature);
                NES_TRACE2("Check filter containment returned: {}", containmentRelationship);
            }
        }
    } catch (...) {
        auto exception = std::current_exception();
        try {
            std::rethrow_exception(exception);
        } catch (const std::exception& e) {
            NES_ERROR("SignatureContainmentUtil: Exception occurred while performing containment check among "
                      "queryIdAndCatalogEntryMapping "
                      << e.what());
        }
    }
    return containmentRelationship;
}

ContainmentType SignatureContainmentUtil::checkProjectionContainment(const QuerySignaturePtr& leftSignature,
                                                                     const QuerySignaturePtr& rightSignature) {
    z3::expr_vector leftQueryProjectionFOL(*context);
    z3::expr_vector rightQueryProjectionFOL(*context);
    //create the projection conditions for each signature
    createProjectionCondition(leftSignature, leftQueryProjectionFOL);
    createProjectionCondition(rightSignature, rightQueryProjectionFOL);
    // We first check if the first order logic (FOL) is equal for projections, if not we move on to check for containment relationships.
    // We added heuristic checks to prevent unnecessary calls to the SMT solver
    // if (!leftProjectionFOL && !rightProjectionFOL) == unsat
    //      true: return EQUALITY
    //      false:
    //          if (# of attr left sig > # of attr right sig)
    //              && !rightFOL && leftFOL == unsat, aka leftFOL ⊆ rightFOL
    //              && filters are equal
    //              && the column order is still the same, despite the containment relationship
    //                  true: return Right sig contained
    //          else if (# of attr left sig < # of attr right sig)
    //              && (rightFOL && !leftFOL == unsat, aka rightFOL ⊆ leftFOL)
    //              && filters are equal
    //              && the column order is still the same, despite the containment relationship
    //                  true: return Left sig contained
    // else: No_Containment
    if (equalityConditionsUnsatisfied(leftQueryProjectionFOL, rightQueryProjectionFOL)) {
        NES_TRACE("Equal projection.");
        return EQUALITY;
    } else if (leftSignature->getSchemaFieldToExprMaps().size() == rightSignature->getSchemaFieldToExprMaps().size()) {
        for (size_t i = 0; i < leftSignature->getSchemaFieldToExprMaps().size(); ++i) {
            if (leftSignature->getSchemaFieldToExprMaps()[i].size() > rightSignature->getSchemaFieldToExprMaps()[i].size()
                && containmentConditionsUnsatisfied(rightQueryProjectionFOL, leftQueryProjectionFOL)
                && checkFilterContainment(leftSignature, rightSignature) == EQUALITY
                && checkColumnOrder(leftSignature, rightSignature)) {
                return RIGHT_SIG_CONTAINED;
            } else if (leftSignature->getSchemaFieldToExprMaps()[i].size() < rightSignature->getSchemaFieldToExprMaps()[i].size()
                       && containmentConditionsUnsatisfied(leftQueryProjectionFOL, rightQueryProjectionFOL)
                       && checkFilterContainment(leftSignature, rightSignature) == EQUALITY
                       && checkColumnOrder(rightSignature, leftSignature)) {
                return LEFT_SIG_CONTAINED;
            }
        }
    }
    return NO_CONTAINMENT;
}

ContainmentType SignatureContainmentUtil::checkWindowContainment(const QuerySignaturePtr& leftSignature,
                                                                 const QuerySignaturePtr& rightSignature) {
    if (leftSignature->getWindowsExpressions().empty() && rightSignature->getWindowsExpressions().empty()) {
        return EQUALITY;
    }
    for (size_t i = 0; i < leftSignature->getWindowsExpressions().size(); ++i) {
        const auto& leftWindow = leftSignature->getWindowsExpressions()[i];
        const auto& rightWindow = rightSignature->getWindowsExpressions()[i];
        NES_TRACE("Starting with left window: " << leftWindow.at("z3-window-expressions")->to_string());
        NES_TRACE("Starting with right window: " << rightWindow.at("z3-window-expressions")->to_string());
        //checks if the window keys are equal, operator sharing can only happen for equal window-keys
        if (leftWindow.at("window-id")->to_string() == rightWindow.at("window-id")->to_string()) {
            NES_TRACE("Same window ids.");
            for (auto entry : leftWindow) {
                NES_TRACE2("Left window first entry {}", entry.first);
                NES_TRACE2("Left window second entry {}", entry.second->to_string());
            }
            for (auto entry : rightWindow) {
                NES_TRACE2("Left window first entry {}", entry.first);
                NES_TRACE2("Left window second entry {}", entry.second->to_string());
            }
            bool containmentRelationshipPossible = checkWindowContainmentPossible(leftWindow, leftSignature, rightSignature);
            //extract the z3-window-expressions
            z3::expr_vector leftQueryWindowConditions(*context);
            z3::expr_vector rightQueryWindowConditions(*context);
            leftQueryWindowConditions.push_back(to_expr(*context, *leftWindow.at("z3-window-expressions")));
            rightQueryWindowConditions.push_back(to_expr(*context, *rightWindow.at("z3-window-expressions")));
            NES_TRACE2("Created window FOL.");
            //checks if the number of aggregates is equal, for equal number of aggregates we check for complete equality
            //and if the windows contain each other, e.g. z3 checks window-size <= window-size && window-slide <= window-slide
            if (leftWindow.at("number-of-aggregates")->char_to_int() == rightWindow.at("number-of-aggregates")->char_to_int()) {
                NES_TRACE2("Same number of aggregates.");
                if (containmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)) {
                    if (containmentConditionsUnsatisfied(rightQueryWindowConditions, leftQueryWindowConditions)) {
                        NES_TRACE2("Equal windows.");
                        return EQUALITY;
                    }
                    //needs filter and projection equality
                    //makes sure that no operations are included that cannot be contained, i.e.
                    //Joins, Avg, and Median windows cannot share operations unless they are equal
                    if (containmentRelationshipPossible) {
                        NES_TRACE("Right window contained.");
                        return RIGHT_SIG_CONTAINED;
                    }
                } else if (containmentConditionsUnsatisfied(rightQueryWindowConditions, leftQueryWindowConditions)) {
                    //needs filter and projection equality
                    //makes sure that no operations are included that cannot be contained, i.e.
                    //Joins, Avg, and Median windows cannot share operations unless they are equal
                    if (containmentRelationshipPossible) {
                        NES_TRACE("Left window contained.");
                        return LEFT_SIG_CONTAINED;
                    }
                } else {
                    NES_TRACE("No containment.");
                    return NO_CONTAINMENT;
                }
            } else if (leftWindow.at("number-of-aggregates")->char_to_int()
                       > rightWindow.at("number-of-aggregates")->char_to_int()) {
                combineWindowAndProjectionConditions(leftSignature,
                                                     rightSignature,
                                                     leftQueryWindowConditions,
                                                     rightQueryWindowConditions);
                if (containmentRelationshipPossible
                    && containmentConditionsUnsatisfied(rightQueryWindowConditions, leftQueryWindowConditions)) {
                    NES_TRACE("Right window contained.");
                    return RIGHT_SIG_CONTAINED;
                }
            } else if (leftWindow.at("number-of-aggregates")->char_to_int()
                       < rightWindow.at("number-of-aggregates")->char_to_int()) {
                combineWindowAndProjectionConditions(leftSignature,
                                                     rightSignature,
                                                     leftQueryWindowConditions,
                                                     rightQueryWindowConditions);
                if (containmentRelationshipPossible
                    && containmentConditionsUnsatisfied(leftQueryWindowConditions, rightQueryWindowConditions)) {
                    NES_TRACE("Left window contained.");
                    return LEFT_SIG_CONTAINED;
                }
            }
        }
    }
    return NO_CONTAINMENT;
}
bool SignatureContainmentUtil::checkColumnOrder(const QuerySignaturePtr& leftSignature,
                                                const QuerySignaturePtr& rightSignature) const {
    size_t attributeFoundAt = 0;//index to track the position in the larger vector
    for (size_t j = 0; j < rightSignature->getColumns().size(); ++j) {
        while (attributeFoundAt < leftSignature->getColumns().size()
               && leftSignature->getColumns()[attributeFoundAt] != rightSignature->getColumns()[j]) {
            attributeFoundAt++;//keep moving forward in the larger vector until a match is found
        }
        if (attributeFoundAt == leftSignature->getColumns().size()) {
            return false;// if no match is found, the smaller vector is not fully contained by the larger vector
        }
        attributeFoundAt++;// move to the next position in the larger vector, also makes sure that attributes are in the right order
    }
    return true;
}
void SignatureContainmentUtil::combineWindowAndProjectionConditions(const QuerySignaturePtr& leftSignature,
                                                                    const QuerySignaturePtr& rightSignature,
                                                                    z3::expr_vector& leftQueryWindowConditions,
                                                                    z3::expr_vector& rightQueryWindowConditions) {
    z3::expr_vector leftQueryFOL(*context);
    z3::expr_vector rightQueryFOL(*context);
    //create conditions for projection containment
    createProjectionCondition(leftSignature, leftQueryFOL);
    createProjectionCondition(rightSignature, rightQueryFOL);
    for (const auto& condition : leftQueryFOL) {
        leftQueryWindowConditions.push_back(condition);
    }
    for (const auto& condition : rightQueryFOL) {
        rightQueryWindowConditions.push_back(condition);
    }
}

bool SignatureContainmentUtil::checkWindowContainmentPossible(const std::map<std::string, z3::ExprPtr>& leftWindow,
                                                              const QuerySignaturePtr& leftSignature,
                                                              const QuerySignaturePtr& rightSignature) {
    const auto& median = Windowing::WindowAggregationDescriptor::Median;
    const auto& avg = Windowing::WindowAggregationDescriptor::Avg;
    bool leftWindowContainsRightWindow = leftWindow.at("window-id")->to_string() != "JoinWindow"
        && std::find(leftWindow.at("aggregate-types")->to_string().begin(),
                     leftWindow.at("aggregate-types")->to_string().end(),
                     median)
            != leftWindow.at("aggregate-types")->to_string().end()
        && std::find(leftWindow.at("aggregate-types")->to_string().begin(),
                     leftWindow.at("aggregate-types")->to_string().end(),
                     avg)
            != leftWindow.at("aggregate-types")->to_string().end()
        && (checkFilterContainment(leftSignature, rightSignature) == EQUALITY);
    return leftWindowContainsRightWindow;
}

ContainmentType SignatureContainmentUtil::checkFilterContainment(const QuerySignaturePtr& leftSignature,
                                                                 const QuerySignaturePtr& rightSignature) {
    NES_TRACE("SignatureContainmentUtil::checkContainment: Create new condition vectors.");
    z3::expr_vector leftQueryFilterConditions(*context);
    z3::expr_vector rightQueryFilterConditions(*context);
    NES_TRACE("SignatureContainmentUtil::checkContainment: Add filter conditions.");
    leftQueryFilterConditions.push_back(to_expr(*context, *leftSignature->getConditions()));
    rightQueryFilterConditions.push_back(to_expr(*context, *rightSignature->getConditions()));
    NES_TRACE("SignatureContainmentUtil::checkContainment: content of left sig expression vectors: "
              << leftQueryFilterConditions.to_string());
    NES_TRACE("SignatureContainmentUtil::checkContainment: content of right sig expression vectors: "
              << rightQueryFilterConditions.to_string());
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
    if (containmentConditionsUnsatisfied(leftQueryFilterConditions, rightQueryFilterConditions)) {
        if (containmentConditionsUnsatisfied(rightQueryFilterConditions, leftQueryFilterConditions)) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: Equal filters.");
            return EQUALITY;
        }
        NES_TRACE("SignatureContainmentUtil::checkContainment: left sig contains right sig for filters.");
        return RIGHT_SIG_CONTAINED;
    } else {
        if (containmentConditionsUnsatisfied(rightQueryFilterConditions, leftQueryFilterConditions)) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: right sig contains left sig for filters.");
            return LEFT_SIG_CONTAINED;
        }
    }
    return NO_CONTAINMENT;
}

void SignatureContainmentUtil::createProjectionCondition(const QuerySignaturePtr& signature, z3::expr_vector& projectionFOL) {
    //check projection containment
    // if we are given a map value for the attribute, we create a FOL as attributeStringName == mapCondition, e.g. age == 25
    // else we indicate that the attribute is involved in the projection as attributeStingName == true
    // all FOL are added to the projectionCondition vector
    z3::expr_vector createSourceFOL(*context);
    NES_TRACE("Length of schemaFieldToExprMaps: " << signature->getSchemaFieldToExprMaps().size());
    for (auto schemaFieldToExpressionMap : signature->getSchemaFieldToExprMaps()) {
        for (auto [attributeName, z3Expression] : schemaFieldToExpressionMap) {
            NES_TRACE("SignatureContainmentUtil::createProjectionCondition: strings: " << attributeName);
            NES_TRACE("SignatureContainmentUtil::createProjectionCondition: z3 expressions: " << z3Expression->to_string());
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

bool SignatureContainmentUtil::containmentConditionsUnsatisfied(z3::expr_vector& negatedCondition, z3::expr_vector& condition) {
    solver->push();
    solver->add(!mk_and(negatedCondition).simplify());
    solver->push();
    solver->add(mk_and(condition).simplify());
    NES_TRACE("Check unsat: " << solver->check());
    NES_TRACE("Content of solver: " << solver->to_smt2());
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

bool SignatureContainmentUtil::equalityConditionsUnsatisfied(const z3::expr_vector& leftConditions,
                                                             const z3::expr_vector& rightConditions) {
    z3::expr_vector helper(*context);
    for (uint i = 0; i < leftConditions.size(); ++i) {
        NES_TRACE("SignatureContainmentUtil::checkContainment: show me expr: " << leftConditions[i].to_string());
        NES_TRACE("SignatureContainmentUtil::checkContainment: show me expr: " << rightConditions[i].to_string());
        helper.push_back(to_expr(*context, Z3_mk_eq(*context, leftConditions.operator[](i), rightConditions.operator[](i))));
    }
    NES_TRACE("SignatureContainmentUtil::checkContainment: content of combined expression vectors: " << helper.to_string());
    solver->push();
    solver->add(!z3::mk_and(helper).simplify());
    NES_TRACE("Check unsat: " << solver->check());
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

void SignatureContainmentUtil::resetSolver() {
    solver->reset();
    counter = 0;
}
}// namespace NES::Optimizer

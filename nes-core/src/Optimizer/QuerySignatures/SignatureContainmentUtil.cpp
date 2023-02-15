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

namespace NES::Optimizer {

SignatureContainmentUtilPtr SignatureContainmentUtil::create(const z3::ContextPtr& contextSig1Contained) {
    return std::make_shared<SignatureContainmentUtil>(contextSig1Contained);
}

SignatureContainmentUtil::SignatureContainmentUtil(const z3::ContextPtr& contextSig1Contained) : counter(0) {
    //need different context for two solvers
    this->context = contextSig1Contained;
    this->solver = std::make_unique<z3::solver>(*contextSig1Contained);
}

ContainmentDetected SignatureContainmentUtil::checkContainment(const QuerySignaturePtr& signature1,
                                                               const QuerySignaturePtr& signature2) {
    NES_TRACE("SignatureContainmentUtil::checkContainment: Checking for containment.");
    try {
        auto otherConditions = signature2->getConditions();
        auto conditions = signature1->getConditions();
        if (!conditions || !otherConditions) {
            NES_WARNING("SignatureContainmentUtil::checkContainment: Can't compare equality between null signatures");
            return NO_CONTAINMENT;
        }

        z3::expr_vector firstQueryProjectionConditions(*context);
        z3::expr_vector secondQueryProjectionConditions(*context);
        //create conditions for projection containment
        createProjectionCondition(signature1, firstQueryProjectionConditions);
        createProjectionCondition(signature2, secondQueryProjectionConditions);

        NES_TRACE("SignatureContainmentUtil::checkContainment: Add map conditions and check.");
        NES_TRACE("SignatureContainmentUtil::checkContainment: content of Sig 2 expression vectors: "
                  << firstQueryProjectionConditions.to_string());
        NES_TRACE("SignatureContainmentUtil::checkContainment: content of Sig 2 expression vectors: "
                  << secondQueryProjectionConditions.to_string());
        NES_TRACE("SignatureContainmentUtil::checkContainment: Add projection sig1 conditions.");

        solver->push();
        //create FOL for projection containment to check if Sig2 contained by Sig1
        solver->add(z3::mk_and(firstQueryProjectionConditions).simplify());
        solver->push();
        solver->add(!z3::mk_and(secondQueryProjectionConditions).simplify());
        NES_TRACE("SignatureContainmentUtil::checkContainment: Check unsat for projection: " << solver->check());
        //The rest of this method checks for containment as follows:
        //  check if sig2 ⊆ sig1 for projections (including map conditions), i.e. if ((!cond2 && cond1) == unsat) <=> sig2 ⊆ sig1,
        // since we're checking for projection containment, the negation is on the side of the contained condition,
        // e.g. sig2 ⊆ sig1 <=> ((!(attr1==true && attr2==45) && (attr1==true && attr2==45, attr3==true)) == unsat)
        //      true: check sig1 ⊆ sig2 for projections (including map conditions) --> if true, we have equal projections,
        //      i.e. can only check other containment relationships if sources, projections, and maps are equal
        //          true: check filter containment --> see checkFilterContainment() for details
        //          false: return SIG_TWO_CONTAINED
        //      false: check sig1 ⊆ sig2 for projections (including map conditions)
        //          true: return SIG_ONE_CONTAINED
        //          false: return NO_CONTAINMENT
        //check if Sig2 contained by Sig1 for projection
        if (solver->check() == z3::unsat) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: Sig1 contains Sig2.");
            //check for equality in projection, we can only check other containment relationships if sources, projections and maps are equal
            resetSolver(2);
            solver->push();
            solver->add(!z3::mk_and(firstQueryProjectionConditions).simplify());
            solver->push();
            solver->add(z3::mk_and(secondQueryProjectionConditions).simplify());
            if (solver->check() == z3::unsat) {
                NES_TRACE("SignatureContainmentUtil::checkContainment: Equal sources, equal projection.");
                resetSolver(2);
                //todo:  #3494 add window containment
                //check for filter containment
                return checkFilterContainment(signature1, signature2);
            }
            return SIG_TWO_CONTAINED;
        } else {
            //check if new query contained by SQP for projection containment identification
            //since we require equal sources to check for further containment relationships, we cannot check for filter nor window containment here
            resetSolver(2);
            solver->push();
            solver->add(!z3::mk_and(firstQueryProjectionConditions).simplify());
            solver->push();
            solver->add(z3::mk_and(secondQueryProjectionConditions).simplify());
            NES_TRACE("SignatureContainmentUtil::checkContainment: Check unsat for projection: " << solver->check());
            if (solver->check() == z3::unsat) {
                NES_TRACE("SignatureContainmentUtil::checkContainment: Sig2 contains Sig1.");
                return SIG_ONE_CONTAINED;
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
        return NO_CONTAINMENT;
    }
    return NO_CONTAINMENT;
}
ContainmentDetected SignatureContainmentUtil::checkFilterContainment(const QuerySignaturePtr& signature1,
                                                                     const QuerySignaturePtr& signature2) {

    NES_TRACE("SignatureContainmentUtil::checkContainment: Create new condition vectors.");
    z3::expr_vector firstQueryFilterConditions(*context);
    z3::expr_vector secondQueryFilterConditions(*context);
    NES_TRACE("SignatureContainmentUtil::checkContainment: Add filter conditions.");

    firstQueryFilterConditions.push_back(to_expr(*context, *signature1->getConditions()));
    secondQueryFilterConditions.push_back(to_expr(*context, *signature2->getConditions()));

    NES_TRACE("SignatureContainmentUtil::checkContainment: content of Sig 2 expression vectors: "
              << firstQueryFilterConditions.to_string());
    NES_TRACE("SignatureContainmentUtil::checkContainment: content of Sig 2 expression vectors: "
              << secondQueryFilterConditions.to_string());
    //The rest of the method checks for filter containment as follows:
    //check if sig2 ⊆ sig1 for filters, i.e. if ((cond2 && !cond1) == unsat) <=> sig2 ⊆ sig1,
    //since we're checking for projection containment, the negation is on the side of the contained condition,
    //e.g. sig2 ⊆ sig1 <=> (((attr<5 && attr2==6) && !(attr1<=10 && attr2==45)) == unsat)
    //      true: check if sig1 ⊆ sig2
    //          true: return EQUALITY
    //          false: return SIG_TWO_CONTAINED
    //      false: check if sig1 ⊆ sig2
    //          true: return SIG_ONE_CONTAINED
    //          false: return NO_CONTAINMENT
    solver->push();
    solver->add(!z3::mk_and(firstQueryFilterConditions).simplify());
    solver->push();
    solver->add(z3::mk_and(secondQueryFilterConditions).simplify());
    NES_TRACE("SignatureContainmentUtil::checkContainment: Check unsat: " << solver->check());
    if (solver->check() == z3::unsat) {
        resetSolver(2);
        solver->push();
        solver->add(z3::mk_and(firstQueryFilterConditions).simplify());
        solver->push();
        solver->add(!z3::mk_and(secondQueryFilterConditions).simplify());
        if (solver->check() == z3::unsat) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: Equal filters.");
            return EQUALITY;
        }
        NES_TRACE("SignatureContainmentUtil::checkContainment: Sig1 contains Sig2 for filters.");
        return SIG_TWO_CONTAINED;
    } else {
        resetSolver(2);
        solver->push();
        solver->add(z3::mk_and(firstQueryFilterConditions).simplify());
        solver->push();
        solver->add(!z3::mk_and(secondQueryFilterConditions).simplify());
        if (solver->check() == z3::unsat) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: Sig2 contains Sig1 for filters.");
            return SIG_ONE_CONTAINED;
        }
    }
    return NO_CONTAINMENT;
}

void SignatureContainmentUtil::createProjectionCondition(const QuerySignaturePtr& signature,
                                                         z3::expr_vector& projectionCondition) {
    //todo: #3495 think about order for projection
    //check projection containment
    // if we are given a map value for the attribute, we create a FOL as attributeStringName == mapCondition, e.g. age == 25
    // else we indicate that the attribute is involved in the projection as attributeStingName == true
    // all FOL are added to the projectionCondition vector
    for (auto exp : signature->getSchemaFieldToExprMaps()) {
        for (auto entry : exp) {
            NES_TRACE("SignatureContainmentUtil::createProjectionCondition: strings: " << entry.first);
            NES_TRACE("SignatureContainmentUtil::createProjectionCondition: z3 expressions: " << entry.second->to_string());
            z3::ExprPtr expr = std::make_shared<z3::expr>(context->bool_const((entry.first.c_str())));
            if (entry.second->to_string() != entry.first) {
                if (entry.second->is_int()) {
                    expr = std::make_shared<z3::expr>(context->int_const(entry.first.c_str()));
                } else if (entry.second->is_fpa()) {
                    expr = std::make_shared<z3::expr>(context->fpa_const<64>(entry.first.c_str()));
                } else if (entry.second->is_string_value()) {
                    expr = std::make_shared<z3::expr>(context->string_const(entry.first.c_str()));
                }
                projectionCondition.push_back(to_expr(*context, Z3_mk_eq(*context, *expr, *entry.second)));
            } else {
                z3::ExprPtr columnIsUsed = std::make_shared<z3::expr>(context->bool_val(true));
                projectionCondition.push_back(to_expr(*context, Z3_mk_eq(*context, *expr, *columnIsUsed)));
            }
        }
    }
}

bool SignatureContainmentUtil::resetSolver(uint8_t numberOfValuesToPop) {
    solver->pop(numberOfValuesToPop);
    counter++;
    if (counter >= 20050) {
        solver->reset();
        counter = 0;
        return true;
    }
    return false;
}

}// namespace NES::Optimizer

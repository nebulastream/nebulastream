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
#include <z3++.h>

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
        //for projection containment, Q1 contains Q2
        solver->add(z3::mk_and(firstQueryProjectionConditions).simplify());
        solver->push();
        solver->add(!z3::mk_and(secondQueryProjectionConditions).simplify());
        NES_TRACE("SignatureContainmentUtil::checkContainment: Check unsat for projection: " << solver->check());
        //check if Sig2 contained by Sig1 for projection
        if (solver->check() == z3::unsat) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: Sig1 contains Sig2.");
            //check for equality in projection
            solver->pop(2);
            counter++;
            if (counter >= 20050) {
                resetSolver();
            }
            solver->push();
            solver->add(!z3::mk_and(firstQueryProjectionConditions).simplify());
            solver->push();
            solver->add(z3::mk_and(secondQueryProjectionConditions).simplify());
            if (solver->check() == z3::unsat) {
                NES_TRACE("SignatureContainmentUtil::checkContainment: Equal sources, equal projection.");
                solver->pop(2);
                counter++;
                if (counter >= 20050) {
                    resetSolver();
                }
                //todo:  #3494 add window containment
                //check for filter containment
                return checkFilterContainment(signature1, signature2);
            }
            return SIG_TWO_CONTAINED;
            //check if new query contained by new query
        } else {
            solver->pop(2);
            counter++;
            if (counter >= 20050) {
                resetSolver();
            }
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
            //NES_ERROR("SignatureEqualityUtil: Exception occurred while performing equality check among queryIdAndCatalogEntryMapping " << e.what());
        }
        return NO_CONTAINMENT;
    }
    return NO_CONTAINMENT;
}
ContainmentDetected SignatureContainmentUtil::checkFilterContainment(const QuerySignaturePtr& signature1,
                                                                     const QuerySignaturePtr& signature2) {
    NES_TRACE("SignatureContainmentUtil::checkContainment: Remove previous conditions.");
    z3::expr_vector firstQueryFilterConditions(*context);
    z3::expr_vector secondQueryFilterConditions(*context);
    NES_TRACE("SignatureContainmentUtil::checkContainment: Add filter conditions.");
    firstQueryFilterConditions.push_back(to_expr(*context, *signature1->getConditions()));
    secondQueryFilterConditions.push_back(to_expr(*context, *signature2->getConditions()));
    NES_TRACE("SignatureContainmentUtil::checkContainment: content of Sig 2 expression vectors: "
              << firstQueryFilterConditions.to_string());
    NES_TRACE("SignatureContainmentUtil::checkContainment: content of Sig 2 expression vectors: "
              << secondQueryFilterConditions.to_string());
    solver->push();
    solver->add(!z3::mk_and(firstQueryFilterConditions).simplify());
    solver->push();
    solver->add(z3::mk_and(secondQueryFilterConditions).simplify());
    NES_TRACE("SignatureContainmentUtil::checkContainment: Check unsat: " << solver->check());
    if (solver->check() == z3::unsat) {
        solver->pop(2);
        counter++;
        if (counter >= 20050) {
            resetSolver();
        }
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
        solver->pop(2);
        counter++;
        if (counter >= 20050) {
            resetSolver();
        }
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

void SignatureContainmentUtil::createProjectionCondition(const QuerySignaturePtr& signature1,
                                                         z3::expr_vector& projectionCondition) {
    //todo: #3495 think about order for projection
    //check projection containment
    for (auto exp : signature1->getSchemaFieldToExprMaps()) {
        for (auto entry : exp) {
            NES_TRACE("SignatureContainmentUtil::createProjectionCondition: strings: " << entry.first);
            NES_TRACE("SignatureContainmentUtil::createProjectionCondition: z3 expressions: " << entry.second->to_string());
            //z3::ExprPtr orderExpr = std::make_shared<z3::expr>(context->int_const((entry.first+"_").c_str()));
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

bool SignatureContainmentUtil::resetSolver() {
    solver->reset();
    counter = 0;
    return true;
}

}// namespace NES::Optimizer

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

SignatureContainmentUtilPtr SignatureContainmentUtil::create(const z3::ContextPtr& contextSig1Contained,
                                                       const z3::ContextPtr& contextSig2Containment) {
    return std::make_shared<SignatureContainmentUtil>(contextSig1Contained, contextSig2Containment);
}

SignatureContainmentUtil::SignatureContainmentUtil(const z3::ContextPtr& contextSig1Contained, const z3::ContextPtr& contextSig2Containment)
    : counter(0) {
    //need different context for two solvers
    this->contextSig1Contained = contextSig1Contained;
    this->contextSig2Contained = contextSig2Containment;
    this->solverSig1Contained = std::make_unique<z3::solver>(*contextSig1Contained);
    this->solverSig2Contained = std::make_unique<z3::solver>(*contextSig2Containment);
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

        z3::expr_vector leftSig1Contained(*contextSig1Contained);
        z3::expr_vector rightSig1Contained(*contextSig1Contained);
        z3::expr_vector leftSig2Contained(*contextSig2Contained);
        z3::expr_vector rightSig2Contained(*contextSig2Contained);
        //create conditions for projection containment
        createProjectionCondition(signature1, leftSig1Contained, leftSig2Contained);
        NES_TRACE("SignatureContainmentUtil::checkContainment: content of Sig 1 expression vectors: " << leftSig1Contained.to_string());
        createProjectionCondition(signature1, rightSig1Contained, rightSig2Contained);
        NES_TRACE("SignatureContainmentUtil::checkContainment: content of Sig 2 expression vectors: " << rightSig1Contained.to_string());

        solverSig1Contained->push();
        solverSig1Contained->add(!z3::mk_and(leftSig1Contained).simplify());
        solverSig1Contained->add(z3::mk_and(rightSig1Contained).simplify());
        solverSig2Contained->push();
        solverSig2Contained->add(z3::mk_and(rightSig2Contained).simplify());
        solverSig2Contained->add(!z3::mk_and(leftSig2Contained).simplify());
        //check if Sig2 contained by Sig1 for projection
        if (solverSig1Contained->check() == z3::unsat) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: Sig1 contains Sig2.");
            //check for equality in projection
            if (solverSig2Contained->check() == z3::unsat) {
                NES_TRACE("SignatureContainmentUtil::checkContainment: Equal sources, equal projection.");
                solverSig1Contained->pop();
                solverSig2Contained->pop();
                counter++;
                if (counter >= 20050) {
                    resetSolver();
                }
                //todo: add window containment
                //check for filter containment
                leftSig1Contained.empty();
                rightSig1Contained.empty();
                leftSig2Contained.empty();
                rightSig2Contained.empty();
                leftSig1Contained.push_back(to_expr(*contextSig1Contained, signature1->getConditions()->operator Z3_ast()));
                rightSig1Contained.push_back(to_expr(*contextSig1Contained, signature2->getConditions()->operator Z3_ast()));
                leftSig2Contained.push_back(to_expr(*contextSig2Contained, signature1->getConditions()->operator Z3_ast()));
                rightSig2Contained.push_back(to_expr(*contextSig2Contained, signature2->getConditions()->operator Z3_ast()));
                solverSig1Contained->push();
                solverSig1Contained->add(z3::mk_and(leftSig1Contained).simplify());
                solverSig1Contained->add(!z3::mk_and(rightSig1Contained).simplify());
                solverSig2Contained->push();
                solverSig2Contained->add(!z3::mk_and(rightSig2Contained).simplify());
                solverSig2Contained->add(z3::mk_and(leftSig2Contained).simplify());
                if (solverSig2Contained->check() == z3::unsat) {
                    if (solverSig1Contained->check() == z3::unsat){
                        NES_TRACE("SignatureContainmentUtil::checkContainment: Equal filters.");
                        return EQUALITY;
                    }
                    NES_TRACE("SignatureContainmentUtil::checkContainment: Sig1 contains Sig2 for filters.");
                    return SIG_TWO_CONTAINED;
                } else if (solverSig1Contained->check() == z3::unsat) {
                    NES_TRACE("SignatureContainmentUtil::checkContainment: Equal filters.");
                    return SIG_ONE_CONTAINED;
                }
            }
            return SIG_TWO_CONTAINED;
            //check if new query contained by new query
        } else if (solverSig2Contained->check() == z3::unsat) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: Sig2 contains Sig1.");
            return SIG_ONE_CONTAINED;
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
//todo: This should be a part of signature creation
void SignatureContainmentUtil::createProjectionCondition(const QuerySignaturePtr& signature1,
                                                         z3::expr_vector& left,
                                                         z3::expr_vector& leftSQP) {
    uint16_t order = 0;
    //check projection containment
    for (auto exp : signature1->getSchemaFieldToExprMaps()) {
        for (auto entry : exp) {
            z3::ExprPtr expr = std::make_shared<z3::expr>(contextSig1Contained->int_const(entry.first.c_str()));
            if (entry.second->is_int()) {
                std::string val = "1" + std::to_string(order);
                left.push_back(to_expr(*contextSig1Contained, Z3_mk_eq(*contextSig1Contained, *expr, contextSig1Contained->int_val(std::stoi(val)))));
                leftSQP.push_back(to_expr(*contextSig2Contained, Z3_mk_eq(*contextSig2Contained, *expr, contextSig1Contained->int_val(std::stoi(val)))));
            } else if (entry.second->is_fpa()) {
                std::string val = "2" + std::to_string(order);
                left.push_back(to_expr(*contextSig1Contained, Z3_mk_eq(*contextSig1Contained, *expr, contextSig1Contained->int_val(std::stoi(val)))));
                leftSQP.push_back(to_expr(*contextSig2Contained, Z3_mk_eq(*contextSig2Contained, *expr, contextSig1Contained->int_val(std::stoi(val)))));
            } else if (entry.second->is_string_value()) {
                std::string val = "3" + std::to_string(order);
                left.push_back(to_expr(*contextSig1Contained, Z3_mk_eq(*contextSig1Contained, *expr, contextSig1Contained->int_val(std::stoi(val)))));
                leftSQP.push_back(to_expr(*contextSig2Contained, Z3_mk_eq(*contextSig2Contained, *expr, contextSig1Contained->int_val(std::stoi(val)))));
            }
            counter++;
        }
    }
}

bool SignatureContainmentUtil::resetSolver() {
    //    this->context->set("solver2_unknown", true);
    //    this->context->set("ignore_solver1", true);
    //    this->context->set("timeout", 1);
    //    solver = std::make_unique<z3::solver>(*context);
    solverSig1Contained->reset();
    solverSig2Contained->reset();
    counter = 0;
    return true;
}

}// namespace NES::Optimizer

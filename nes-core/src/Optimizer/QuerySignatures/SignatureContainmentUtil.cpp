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

SignatureContainmentUtilPtr SignatureContainmentUtil::create(const z3::ContextPtr& context,
                                                       const z3::ContextPtr& contextSQPContainment) {
    return std::make_shared<SignatureContainmentUtil>(context, contextSQPContainment);
}

SignatureContainmentUtil::SignatureContainmentUtil(const z3::ContextPtr& context, const z3::ContextPtr& contextSQPContainment)
    : counter(0) {
    //need different context for two solvers
    this->context = context;
    this->contextSQPContainment = contextSQPContainment;
    this->solver = std::make_unique<z3::solver>(*context);
    this->solverSQPContainment = std::make_unique<z3::solver>(*contextSQPContainment);
}
//todo: change back to identify containment without operators; we already know operators
//todo: do smaller changes to incrementally add containment to nes
std::map<ContainmentOperator, ContainmentDetected> SignatureContainmentUtil::checkContainment(const QuerySignaturePtr& signature1,
                                                                                           const QuerySignaturePtr& signature2) {
    NES_TRACE("SignatureContainmentUtil::checkContainment: Checking for containment.");
    std::map<ContainmentOperator, ContainmentDetected> containmentRelationships = {};
    try {
        auto otherConditions = signature2->getConditions();
        auto conditions = signature1->getConditions();
        if (!conditions || !otherConditions) {
            NES_WARNING("SignatureContainmentUtil::checkContainment: Can't compare equality between null signatures");
            return containmentRelationships;
        }

        z3::expr_vector left(*context);
        z3::expr_vector right(*context);
        z3::expr_vector leftSQP(*contextSQPContainment);
        z3::expr_vector rightSQP(*contextSQPContainment);
        //check projection containment
        for (auto exp : signature1->getSchemaFieldToExprMaps()) {
            for (auto entry : exp) {
                //todo: change/add extra schemaFieldToExprMaps signature generation, that eliminates this step
                //todo: instead of setting entry.second == entry.second, I'd rather have entry.second == true, do you know how to create a truth value?
                //todo: take care of order, think more on how to do projection containment

                left.push_back(to_expr(*context, Z3_mk_eq(*context, *entry.second, *entry.second)));
                leftSQP.push_back(to_expr(*contextSQPContainment, Z3_mk_eq(*contextSQPContainment, *entry.second, *entry.second)));
            }
        }
        for (auto exp : signature2->getSchemaFieldToExprMaps()) {
            for (auto entry : exp) {
                right.push_back(to_expr(*context, Z3_mk_eq(*context, *entry.second, *entry.second)));
                rightSQP.push_back(to_expr(*contextSQPContainment, Z3_mk_eq(*contextSQPContainment, *entry.second, *entry.second)));
            }
        }
        solver->push();
        solver->add(!z3::mk_and(left).simplify());
        solver->add(z3::mk_and(right).simplify());
        solverSQPContainment->push();
        solverSQPContainment->add(z3::mk_and(rightSQP).simplify());
        solverSQPContainment->add(!z3::mk_and(leftSQP).simplify());
        //check if new query contained by SQP
        if (solver->check() == z3::unsat) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: SQP contains new query.");
            //check for equality in projection
            if (solverSQPContainment->check() == z3::unsat) {
                containmentRelationships.insert({ContainmentOperator::PROJECTION, ContainmentDetected::EQUALITY});
                NES_TRACE("SignatureContainmentUtil::checkContainment: Equal sources, equal projection.");
                solver->pop();
                solverSQPContainment->pop();
                counter++;
                if (counter >= 20050) {
                    resetSolver();
                }
                //check for window containment
                solver->push();
            }
            containmentRelationships.insert({ContainmentOperator::PROJECTION, ContainmentDetected::NEW_QUERY_CONTAINED});
            //check if new query contained by new query
        } else if (solverSQPContainment->check() == z3::unsat) {
            NES_TRACE("SignatureContainmentUtil::checkContainment: New query contains SQP.");
            containmentRelationships.insert({ContainmentOperator::PROJECTION, ContainmentDetected::SQP_CONTAINED});
        }
    } catch (...) {
        auto exception = std::current_exception();
        try {
            std::rethrow_exception(exception);
        } catch (const std::exception& e) {
            //NES_ERROR("SignatureEqualityUtil: Exception occurred while performing equality check among queryIdAndCatalogEntryMapping " << e.what());
        }
        return containmentRelationships;
    }
    return containmentRelationships;
}

bool SignatureContainmentUtil::resetSolver() {
    //    this->context->set("solver2_unknown", true);
    //    this->context->set("ignore_solver1", true);
    //    this->context->set("timeout", 1);
    //    solver = std::make_unique<z3::solver>(*context);
    solver->reset();
    solverSQPContainment->reset();
    counter = 0;
    return true;
}

}// namespace NES::Optimizer

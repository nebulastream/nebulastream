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

#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/Utils/SignatureEqualityUtil.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

SignatureEqualityUtilPtr SignatureEqualityUtil::create(const z3::ContextPtr& context) {
    return std::make_shared<SignatureEqualityUtil>(context);
}

SignatureEqualityUtil::SignatureEqualityUtil(const z3::ContextPtr& context) : counter(0) {
    this->context = context;
    //    this->context->set("solver2_unknown", true);
    //    this->context->set("ignore_solver1", true);
    //    this->context->set("timeout", 1);
    solver = std::make_unique<z3::solver>(*context);
}

bool SignatureEqualityUtil::checkEquality(const QuerySignaturePtr& signature1, const QuerySignaturePtr& signature2) {
    NES_TRACE("QuerySignature: Equating signatures");

    try {
        auto otherConditions = signature2->getConditions();
        auto conditions = signature1->getConditions();
        if (!conditions || !otherConditions) {
            NES_WARNING("QuerySignature: Can't compare equality between null signatures");
            return false;
        }

        //Check the number of columns extracted by both queries
        auto otherColumns = signature2->getColumns();
        auto columns = signature1->getColumns();
        if (columns.size() != otherColumns.size()) {
            NES_WARNING("QuerySignature: Both signatures have different column entries");
            return false;
        }

        //Check the number of window expressions extracted from both queries
        auto otherWindowExpressions = signature2->getWindowsExpressions();
        auto windowsExpressions = signature1->getWindowsExpressions();
        if (windowsExpressions.size() != otherWindowExpressions.size()) {
            NES_WARNING("QuerySignature: Both signatures have different window expressions");
            return false;
        }

        //Check if two columns are identical
        auto otherSchemaFieldToExprMaps = signature2->getSchemaFieldToExprMaps();
        auto schemaFieldToExprMaps = signature1->getSchemaFieldToExprMaps();

        //Check both have same number of schema maps
        if (otherSchemaFieldToExprMaps.size() != schemaFieldToExprMaps.size()) {
            NES_WARNING("QuerySignature: Both signatures have different number of Schema Filed to Expr Maps");
            return false;
        }

        //If column from one signature doesn't exist in other signature then they are not equal.
        for (auto schemaFieldToExprMap : schemaFieldToExprMaps) {
            bool schemaMatched = false;
            for (auto otherSchemaMapItr = otherSchemaFieldToExprMaps.begin();
                 otherSchemaMapItr != otherSchemaFieldToExprMaps.end();
                 otherSchemaMapItr++) {
                z3::expr_vector colChecks(*context);
                for (uint64_t index = 0; index < columns.size(); index++) {
                    auto colExpr = schemaFieldToExprMap[columns[index]];
                    auto otherColExpr = (*otherSchemaMapItr)[otherColumns[index]];
                    auto equivalenceCheck = to_expr(*context, Z3_mk_eq(*context, *colExpr, *otherColExpr));
                    colChecks.push_back(equivalenceCheck);
                }

                solver->push();
                solver->add(!z3::mk_and(colChecks).simplify());
                schemaMatched = solver->check() == z3::unsat;
                solver->pop();
                counter++;
                if (counter >= 20050) {
                    resetSolver();
                }
                //                NES_ERROR("SCHEMA CHK Z3 " << counter);
                //If schema is matched then remove the other schema from the list to avoid duplicate matching

                if (schemaMatched) {
                    otherSchemaFieldToExprMaps.erase(otherSchemaMapItr);
                    break;
                }
            }

            //If a matching schema doesn't exists in other signature then two signatures are different
            if (!schemaMatched) {
                NES_WARNING("QuerySignature: Both signatures have different column entries");
                return false;
            }
        }

        //Compute all CNF conditions for check
        z3::expr_vector allConditions(*context);

        //Convert window definitions from both signature into equality conditions
        //If window key from one signature doesn't exists in other signature then they are not equal.
        for (const auto& windowExpression : windowsExpressions) {
            if (otherWindowExpressions.find(windowExpression.first) == otherWindowExpressions.end()) {
                NES_WARNING("Window expression with key " << windowExpression.first
                                                          << " doesn't exists in window expressions of other signature");
                return false;
            }
            //For each column expression of the column in other signature we try to create a DNF using
            // each column expression of the same column in this signature.
            z3::ExprPtr otherWindowExpression = otherWindowExpressions[windowExpression.first];
            allConditions.push_back(to_expr(*context, Z3_mk_eq(*context, *otherWindowExpression, *windowExpression.second)));
        }

        //Add conditions from both signature into the collection of all conditions
        allConditions.push_back(to_expr(*context, Z3_mk_eq(*context, *conditions, *otherConditions)));

        //Create a negation of CNF of all conditions collected till now
        solver->push();
        solver->add(!z3::mk_and(allConditions).simplify());
        //        NES_ERROR("COND CHK Z3: " << counter);
        bool equal = solver->check() == z3::unsat;
        solver->pop();
        counter++;
        if (counter >= 20050) {
            resetSolver();
        }
        //        NES_ERROR("Z3 Equality " << counter);
        return equal;
    } catch (...) {
        auto eptr = std::current_exception();
        try {
            std::rethrow_exception(eptr);
        } catch (const std::exception& e) {
            //            NES_ERROR("SignatureEqualityUtil: Exception occurred while performing equality check among queries " << e.what());
        }
        return false;
    }
}

bool SignatureEqualityUtil::resetSolver() {
    //    this->context->set("solver2_unknown", true);
    //    this->context->set("ignore_solver1", true);
    //    this->context->set("timeout", 1);
    //    solver = std::make_unique<z3::solver>(*context);
    solver->reset();
    counter = 0;
    return true;
}

}// namespace NES::Optimizer

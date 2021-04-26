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

SignatureEqualityUtilPtr SignatureEqualityUtil::create(z3::ContextPtr context) {
    return std::make_shared<SignatureEqualityUtil>(context);
}

SignatureEqualityUtil::SignatureEqualityUtil(z3::ContextPtr context) {
    this->context = context;
    this->context->set("solver2_unknown", true);
    this->context->set("ignore_solver1", true);
    this->context->set("timeout", 100);
    solver = std::make_unique<z3::solver>(*context);
}

bool SignatureEqualityUtil::checkEquality(QuerySignaturePtr signature1, QuerySignaturePtr signature2) {
    NES_TRACE("QuerySignature: Equating signatures");

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

    //Check if two columns are identical
    //If column from one signature doesn't exists in other signature then they are not equal.
    auto otherSchemaFieldToExprMaps = signature2->getSchemaFieldToExprMaps();

    //Iterate over all distinct schema maps
    // and identify if there is an equivalent schema map in the other signature
    for (auto& schemaFieldToExpMaps : signature1->getSchemaFieldToExprMaps()) {
        bool schemaExists = false;
        for (auto otherSchemaMapItr = otherSchemaFieldToExprMaps.begin(); otherSchemaMapItr != otherSchemaFieldToExprMaps.end();
             otherSchemaMapItr++) {
            bool schemaMatched = false;
            //Iterate over all the field expressions from one schema and other
            // and check if they are matching
            for (auto& [fieldName, fieldExpr] : schemaFieldToExpMaps) {
                bool fieldMatch = false;
                for (auto& [otherFieldName, otherFieldExpr] : *otherSchemaMapItr) {
                    //                    solver->push();
                    //                    solver->add((*fieldExpr != *otherFieldExpr).simplify());
                    //                    bool equal = solver->check() == z3::unsat;
                    //                    solver->pop();
                    //                    if (equal) {
                    if (z3::eq(*fieldExpr, *otherFieldExpr)) {
                        fieldMatch = true;
                        break;
                    }
                }
                //If field is not matched with any of the other's field then schema is mis-matched
                if (!fieldMatch) {
                    schemaMatched = false;
                    break;
                }
                schemaMatched = true;
            }

            //If schema is matched then remove the other schema from the list to avoid duplicate matching
            if (schemaMatched) {
                otherSchemaFieldToExprMaps.erase(otherSchemaMapItr);
                schemaExists = true;
                break;
            }
        }

        //If a matching schema doesn't exists in other signature then two signatures are different
        if (!schemaExists) {
            NES_WARNING("QuerySignature: Both signatures have different column entries");
            return false;
        }
    }

    //Compute all CNF conditions for check
    z3::expr_vector allConditions(*context);
    //Check the number of window expressions extracted from both queries
    auto otherWindowExpressions = signature2->getWindowsExpressions();
    auto windowsExpressions = signature1->getWindowsExpressions();
    if (windowsExpressions.size() != otherWindowExpressions.size()) {
        NES_WARNING("QuerySignature: Both signatures have different window expressions");
        return false;
    }

    //Convert window definitions from both signature into equality conditions
    //If window key from one signature doesn't exists in other signature then they are not equal.
    for (auto windowExpression : windowsExpressions) {
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
    //    NES_ERROR(*solver);
    bool equal = solver->check() == z3::unsat;
    //    NES_ERROR("Equality " << equal);
    solver->pop();
    return equal;
}

}// namespace NES::Optimizer

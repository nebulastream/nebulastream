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
#include <Optimizer/Utils/OperatorSchemas.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

QuerySignaturePtr QuerySignature::create(z3::ExprPtr conditions, std::vector<std::string> columns,
                                         OperatorSchemasPtr operatorSchemas,
                                         std::map<std::string, z3::ExprPtr> windowsExpressions) {
    return std::make_shared<QuerySignature>(
        QuerySignature(std::move(conditions), std::move(columns), std::move(operatorSchemas), std::move(windowsExpressions)));
}

QuerySignature::QuerySignature(z3::ExprPtr conditions, std::vector<std::string> columns, OperatorSchemasPtr operatorSchemas,
                               std::map<std::string, z3::ExprPtr> windowsExpressions)
    : conditions(std::move(conditions)), columns(std::move(columns)), operatorTupleSchemaMap(std::move(operatorSchemas)),
      windowsExpressions(std::move(windowsExpressions)) {}

z3::ExprPtr QuerySignature::getConditions() { return conditions; }

const std::vector<std::string>& QuerySignature::getColumns() { return columns; }

const std::map<std::string, z3::ExprPtr>& QuerySignature::getWindowsExpressions() { return windowsExpressions; }

OperatorSchemasPtr QuerySignature::getOperatorSchemas() { return operatorTupleSchemaMap; }

bool QuerySignature::isEqual(QuerySignaturePtr other) {

    NES_TRACE("QuerySignature: Equating signatures");

    if (!conditions || !other->getConditions()) {
        NES_WARNING("QuerySignature: Can't compare equality between null signatures");
        return false;
    }

    //Extract the Z3 context and initialize a solver
    z3::context& context = conditions->ctx();
    z3::solver solver(context);

    //Check the number of columns extracted by both queries
    auto otherColumns = other->columns;
    if (columns.size() != otherColumns.size()) {
        NES_WARNING("QuerySignature: Both signatures have different column entries");
        return false;
    }

    //Check if two columns are identical
    //If column from one signature doesn't exists in other signature then they are not equal.
    auto otherOperatorTupleSchemaMap = other->getOperatorSchemas();
    auto otherSchemaMaps = otherOperatorTupleSchemaMap->getOperatorSchemas();

    //Iterate over all distinct schema maps
    // and identify if there is an equivalent schema map in the other signature
    for (auto schemaMap : operatorTupleSchemaMap->getOperatorSchemas()) {
        bool schemaExists = false;
        for (auto otherSchemaMapItr = otherSchemaMaps.begin(); otherSchemaMapItr != otherSchemaMaps.end(); otherSchemaMapItr++) {
            bool schemaMatched = false;
            //Iterate over all the field expressions from one schema and other
            // and check if they are matching
            for (auto [fieldName, fieldExpr] : schemaMap) {
                bool fieldMatch = false;
                for (auto [otherFieldName, otherFieldExpr] : *otherSchemaMapItr) {
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
                otherSchemaMaps.erase(otherSchemaMapItr);
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
    z3::expr_vector allConditions(context);
    //Check the number of window expressions extracted from both queries
    auto otherWindowExpressions = other->windowsExpressions;
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
        allConditions.push_back(to_expr(context, Z3_mk_eq(context, *otherWindowExpression, *windowExpression.second)));
    }

    //Add conditions from both signature into the collection of all conditions
    allConditions.push_back(to_expr(context, Z3_mk_eq(context, *conditions, *other->conditions)));

    //Create a negation of CNF of all conditions collected till now
    solver.add(!z3::mk_and(allConditions));
    NES_DEBUG("Solving: " << solver);
    return solver.check() == z3::unsat;
}
}// namespace NES::Optimizer

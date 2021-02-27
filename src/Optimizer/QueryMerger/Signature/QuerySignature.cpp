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
#include <Optimizer/Utils/OperatorTupleSchemaMap.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

QuerySignaturePtr QuerySignature::create(z3::ExprPtr conditions, std::vector<std::string> columns,
                                         OperatorTupleSchemaMapPtr operatorTupleSchemaMap,
                                         std::map<std::string, z3::ExprPtr> windowsExpressions) {
    return std::make_shared<QuerySignature>(QuerySignature(conditions, columns, operatorTupleSchemaMap, windowsExpressions));
}

QuerySignature::QuerySignature(z3::ExprPtr conditions, std::vector<std::string> columns,
                               OperatorTupleSchemaMapPtr operatorTupleSchemaMap,
                               std::map<std::string, z3::ExprPtr> windowsExpressions)
    : conditions(conditions), columns(columns), operatorTupleSchemaMap(operatorTupleSchemaMap),
      windowsExpressions(windowsExpressions) {}

z3::ExprPtr QuerySignature::getConditions() { return conditions; }

std::vector<std::string> QuerySignature::getColumns() { return columns; }

std::map<std::string, z3::ExprPtr> QuerySignature::getWindowsExpressions() { return windowsExpressions; }

const OperatorTupleSchemaMapPtr& QuerySignature::getOperatorTupleSchemaMap() const { return operatorTupleSchemaMap; }

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
    for (auto& fieldName : columns) {
        auto found = std::find(otherColumns.begin(), otherColumns.end(), fieldName);
        if (found == otherColumns.end()) {
            NES_WARNING("QuerySignature: Both signatures have different column entries");
            return false;
        }
    }

    //Convert columns from both the signatures into equality conditions.
    //If column from one signature doesn't exists in other signature then they are not equal.
    z3::expr_vector allConditions(context);
    auto otherOperatorTupleSchemaMap = other->getOperatorTupleSchemaMap();

    for (auto schemaMap : operatorTupleSchemaMap->getSchemaMaps()) {
        for (auto& colField : columns) {
            z3::expr_vector columnCondition(context);
            auto colExpr = schemaMap[colField];
            for (auto otherSchemaMap : otherOperatorTupleSchemaMap->getSchemaMaps()) {
                auto otherColExpr = otherSchemaMap[colField];
                columnCondition.push_back(z3::to_expr(context, Z3_mk_eq(context, *colExpr, *otherColExpr)));
            }
            //Add DNF of col conditions to all conditions
            allConditions.push_back(z3::mk_or(columnCondition));
        }
    }

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

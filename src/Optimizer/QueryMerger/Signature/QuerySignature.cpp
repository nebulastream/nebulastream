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
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

QuerySignaturePtr QuerySignature::create(z3::ExprPtr conditions, std::map<std::string, std::vector<z3::ExprPtr>> columns,
                                         std::map<std::string, z3::ExprPtr> windowsExpressions) {
    return std::make_shared<QuerySignature>(QuerySignature(conditions, columns, windowsExpressions));
}

QuerySignature::QuerySignature(z3::ExprPtr conditions, std::map<std::string, std::vector<z3::ExprPtr>> columns,
                               std::map<std::string, z3::ExprPtr> windowsExpressions)
    : conditions(conditions), columns(columns), windowsExpressions(windowsExpressions) {}

z3::ExprPtr QuerySignature::getConditions() { return conditions; }

std::map<std::string, std::vector<z3::ExprPtr>> QuerySignature::getColumns() { return columns; }

std::map<std::string, z3::ExprPtr> QuerySignature::getWindowsExpressions() { return windowsExpressions; }

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

    //Convert columns from both the signatures into equality conditions.
    //If column from one signature doesn't exists in other signature then they are not equal.
    z3::expr_vector allConditions(context);
    for (auto columnEntry : columns) {
        if (otherColumns.find(columnEntry.first) == otherColumns.end()) {
            NES_WARNING("Column " << columnEntry.first << " doesn't exists in column list of other signature");
            return false;
        }
        //For each column expression of the column in other signature we try to create a DNF using
        // each column expression of the same column in this signature.
        auto otherColumnExpressions = otherColumns[columnEntry.first];
        for (auto& otherColumnExpression : otherColumnExpressions) {
            //Create DNF for the other column expression
            z3::expr_vector columnEqualityConditions(context);
            for (auto& columnExpression : columnEntry.second) {
                columnEqualityConditions.push_back(
                    to_expr(context, Z3_mk_eq(context, *otherColumnExpression, *columnExpression)));
            }
            //Add the DNF to all conditions
            allConditions.push_back(z3::mk_or(columnEqualityConditions));
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
        auto otherWindowExpression = otherWindowExpressions[windowExpression.first];
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

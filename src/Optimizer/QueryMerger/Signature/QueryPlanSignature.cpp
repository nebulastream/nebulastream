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

#include <Optimizer/QueryMerger/Signature/QueryPlanSignature.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

QueryPlanSignaturePtr QueryPlanSignature::create(z3::ExprPtr conds, std::map<std::string, std::vector<z3::ExprPtr>> cols) {
    return std::make_shared<QueryPlanSignature>(QueryPlanSignature(conds, cols));
}

QueryPlanSignature::QueryPlanSignature(z3::ExprPtr conds, std::map<std::string, std::vector<z3::ExprPtr>> cols)
    : conds(conds), cols(cols) {}

z3::ExprPtr QueryPlanSignature::getConds() { return conds; }

std::map<std::string, std::vector<z3::ExprPtr>> QueryPlanSignature::getCols() { return cols; }

bool QueryPlanSignature::isEqual(QueryPlanSignaturePtr other) {

    z3::context& context = conds->ctx();
    z3::solver solver(context);

    auto otherCols = other->cols;
    if (cols.size() != otherCols.size()) {
        return false;
    }

    for (auto ele : cols) {
        if (otherCols.find(ele.first) == otherCols.end()) {
            NES_WARNING("Column " << ele.first << " doesn't exists in column list of other signature");
            return false;
        }
        auto otherColExprs = otherCols[ele.first];
        for (auto& otherColExpr : otherColExprs) {
            z3::expr_vector conds(context);
            for (auto& colExpr : ele.second) {
                conds.push_back(!to_expr(context, Z3_mk_eq(context, *otherColExpr, *colExpr)));
            }
            solver.add(z3::mk_or(conds));
        }
    }
    solver.add(!to_expr(context, Z3_mk_eq(context, *conds, *other->conds)));
    NES_DEBUG("Solving: "<< solver);
    return solver.check() == z3::unsat;
}

}// namespace NES::Optimizer

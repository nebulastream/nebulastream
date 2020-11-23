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

namespace NES::Optimizer {

QueryPlanSignaturePtr QueryPlanSignature::create(z3::ExprPtr conds, std::map<std::string, std::vector<z3::ExprPtr>> cols) {
    return std::make_shared<QueryPlanSignature>(QueryPlanSignature(conds, cols));
}

QueryPlanSignature::QueryPlanSignature(z3::ExprPtr conds, std::map<std::string, std::vector<z3::ExprPtr>> cols)
    : conds(conds), cols(cols) {}

z3::ExprPtr QueryPlanSignature::getConds() { return conds; }

std::map<std::string, std::vector<z3::ExprPtr>> QueryPlanSignature::getCols() { return cols; }

}// namespace NES::Optimizer

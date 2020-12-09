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

#include <Optimizer/Utils/Z3ExprAndFieldMap.hpp>

namespace NES::Optimizer {

Z3ExprAndFieldMap::Z3ExprAndFieldMap(z3::ExprPtr expr, std::map<std::string, z3::ExprPtr> fieldMap) : expr(expr), fieldMap(fieldMap) {}

z3::ExprPtr Z3ExprAndFieldMap::getExpr() { return expr; }

std::map<std::string, z3::ExprPtr> Z3ExprAndFieldMap::getFieldMap() { return fieldMap; }

Z3ExprAndFieldMapPtr Z3ExprAndFieldMap::create(z3::ExprPtr expr, std::map<std::string, z3::ExprPtr> constMap) {
    return std::make_shared<Z3ExprAndFieldMap>(expr, constMap);
}
}// namespace NES::Optimizer

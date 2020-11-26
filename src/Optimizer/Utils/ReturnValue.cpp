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

//
// Created by ankit on 26.11.20.
//

#include <Optimizer/Utils/ReturnValue.hpp>

namespace NES::Optimizer {

ReturnValue::ReturnValue(z3::ExprPtr expr, std::map<std::string, z3::ExprPtr> constMap) : expr(expr), constMap(constMap) {}

const z3::ExprPtr& ReturnValue::getExpr() const { return expr; }

const std::map<std::string, z3::ExprPtr>& ReturnValue::getConstMap() const { return constMap; }

ReturnValuePtr ReturnValue::create(z3::ExprPtr expr, std::map<std::string, z3::ExprPtr> constMap) {
    return std::make_shared<ReturnValue>(expr, constMap);
}
}// namespace NES::Optimizer

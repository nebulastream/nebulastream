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

#ifndef NES_RETURNVALUE_HPP
#define NES_RETURNVALUE_HPP

#include <memory>
#include <map>

namespace z3{
class expr;
typedef std::shared_ptr<expr> ExprPtr;
}

namespace NES::Optimizer {
class ReturnValue;
typedef std::shared_ptr<ReturnValue> ReturnValuePtr;

class ReturnValue {

  public:
    ReturnValue(z3::ExprPtr expr, std::map<std::string, z3::ExprPtr> constMap);

    static ReturnValuePtr create(z3::ExprPtr expr, std::map<std::string, z3::ExprPtr> constMap);

    const z3::ExprPtr& getExpr() const;

    const std::map<std::string, z3::ExprPtr>& getConstMap() const;

  private:
    z3::ExprPtr expr;
    std::map<std::string, z3::ExprPtr> constMap;
};
}// namespace NES::Optimizer
#endif//NES_RETURNVALUE_HPP

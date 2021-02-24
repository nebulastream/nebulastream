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

#ifndef NES_COLUMNEXPRESSIONS_HPP
#define NES_COLUMNEXPRESSIONS_HPP

#include <memory>
#include <vector>

namespace z3 {
class expr;
typedef std::shared_ptr<expr> ExprPtr;
}// namespace z3

namespace NES::Optimizer {
class ColumnExpressions;
typedef std::unique_ptr<ColumnExpressions> ColumnExpressionsPtr;

/**
 * @brief This class holds a collection of column expressions for a column and the unique identifier of the table it is coming from
 */
class ColumnExpressions {

  public:
    static ColumnExpressionsPtr create(uint32_t id, std::vector<z3::ExprPtr> colExpressions);

  private:
    ColumnExpressions(uint32_t id, std::vector<z3::ExprPtr> colExpressions);
    uint32_t id;
    std::vector<z3::ExprPtr> colExpressions;
};
}// namespace NES::Optimizer
#endif//NES_COLUMNEXPRESSIONS_HPP

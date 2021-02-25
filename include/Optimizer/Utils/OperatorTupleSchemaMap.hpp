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

#ifndef NES_OPERATORTUPLESCHEMAMAP_HPP
#define NES_OPERATORTUPLESCHEMAMAP_HPP

#include <map>
#include <memory>
#include <vector>

namespace z3 {
class expr;
typedef std::shared_ptr<expr> ExprPtr;
}// namespace z3

namespace NES::Optimizer {
class OperatorTupleSchemaMap;
typedef std::shared_ptr<OperatorTupleSchemaMap> OperatorTupleSchemaMapPtr;

/**
 * @brief This class holds for an operator a collection of schemas for each distinct tuple experienced by it.
 *
 * For Example:
 * - A Union operator will experience tuples with 2 distinct schemas.
 * - A downstream Map operator to a union will also experience tuples with 2 distinct schemas.
 * - A down stream Union operator to two distinct Union operators will experience 4 distinct schemas (2 from left and 2 from right child).
 * - A down stream Join operator to two distinct Union operator can experience maximum 2 X 2 distinct schemas.
 */
class OperatorTupleSchemaMap {

  public:
    static OperatorTupleSchemaMapPtr create(std::vector<std::map<std::string, z3::ExprPtr>> schemaMaps);

    static OperatorTupleSchemaMapPtr createEmpty();

    /**
     * @brief Get the vector of Schema maps
     * @return a vector of schema maps
     */
    const std::vector<std::map<std::string, z3::ExprPtr>>& getSchemaMaps() const;

  private:
    OperatorTupleSchemaMap();
    OperatorTupleSchemaMap(std::vector<std::map<std::string, z3::ExprPtr>> schemaMaps);
    std::vector<std::map<std::string, z3::ExprPtr>> schemaMaps;
};
}// namespace NES::Optimizer
#endif//NES_OPERATORTUPLESCHEMAMAP_HPP

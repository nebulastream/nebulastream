/*
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

#ifndef NES_LOGICALREORDERBUFFERSOPERATOR_HPP
#define NES_LOGICALREORDERBUFFERSOPERATOR_HPP

#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>

namespace NES {

class ReorderTupleBuffersLogicalOperator : public LogicalUnaryOperator {
  public:
    ReorderTupleBuffersLogicalOperator(OperatorId id);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;

    [[nodiscard]] std::string toString() const override;

    OperatorPtr copy() override;
     bool inferSchema() override;
     void inferStringSignature() override;
};

using ReorderTupleBuffersLogicalOperatorPtr = std::shared_ptr<ReorderTupleBuffersLogicalOperator>;

}
#endif//NES_LOGICALREORDERBUFFERSOPERATOR_HPP

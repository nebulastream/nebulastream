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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALDEMULTIPLEXOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALDEMULTIPLEXOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief The physical demultiplex operator
 * This operator receives one data stream form its upstream operator and forwards it to multiple down-stream operators.
 * Thus it has one child node and multiple parent nodes.
 * Example query plan:
 *                                             --- OperatorY --- Sink
 *                                           /
 * --- Data --- OperatorX --- Demultiplex ---
 *                                           \
 *                                            --- Sink
 */
class PhysicalDemultiplexOperator : public PhysicalUnaryOperator {
  public:
    PhysicalDemultiplexOperator(OperatorId id, SchemaPtr const& inputSchema);
    static PhysicalOperatorPtr create(OperatorId id, SchemaPtr const& inputSchema);
    static PhysicalOperatorPtr create(SchemaPtr inputSchema);
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALDEMULTIPLEXOPERATOR_HPP_

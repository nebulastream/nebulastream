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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSOURCEOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSOURCEOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {
/**
 * @brief Physical Source operator.
 */
class PhysicalSourceOperator : public PhysicalUnaryOperator, public AbstractScanOperator {
  public:
    PhysicalSourceOperator(OperatorId id,
                           OriginId originId,
                           SchemaPtr inputSchema,
                           SchemaPtr outputSchema,
                           SourceDescriptorPtr sourceDescriptor);
    static std::shared_ptr<PhysicalSourceOperator> create(OperatorId id,
                                                          OriginId originId,
                                                          const SchemaPtr& inputSchema,
                                                          const SchemaPtr& outputSchema,
                                                          const SourceDescriptorPtr& sourceDescriptor);
    static std::shared_ptr<PhysicalSourceOperator>
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, SourceDescriptorPtr sourceDescriptor);

    /**
     * @brief Gets the source descriptor for this source operator
     * @return SourceDescriptorPtr
     */
    SourceDescriptorPtr getSourceDescriptor();

    /**
     * @brief Sets the origin id for this source operator
     * @param originId
     */
    void setOriginId(OriginId originId);

    /**
     * @brief Gets the origin id
     * @return OriginId
     */
    OriginId getOriginId();
    std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    SourceDescriptorPtr sourceDescriptor;
    OriginId originId;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif // NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSOURCEOPERATOR_HPP_

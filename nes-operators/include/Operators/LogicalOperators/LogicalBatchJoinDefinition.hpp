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

#ifndef NES_CORE_INCLUDE_WINDOWING_LOGICALBATCHJOINDEFINITION_HPP_
#define NES_CORE_INCLUDE_WINDOWING_LOGICALBATCHJOINDEFINITION_HPP_

#include <Operators/LogicalOperators/Windows/Joins/JoinForwardRefs.hpp>
#include <cstdint>

namespace NES::Join::Experimental {

/**
 * @brief Runtime definition of a join operator
 * @experimental
 */
class LogicalBatchJoinDefinition {// todo jm its dumb that this is in the windowing dir

  public:
    static LogicalBatchJoinDefinitionPtr create(const FieldAccessExpressionNodePtr& keyTypeBuild,
                                                const FieldAccessExpressionNodePtr& keyTypeProbe,
                                                uint64_t numberOfInputEdgesLeft,
                                                uint64_t numberOfInputEdgesRight);

    explicit LogicalBatchJoinDefinition(FieldAccessExpressionNodePtr keyTypeBuild,
                                        FieldAccessExpressionNodePtr keyTypeProbe,
                                        uint64_t numberOfInputEdgesLeft,
                                        uint64_t numberOfInputEdgesRight);

    /**
    * @brief getter/setter for on build join key
    */
    FieldAccessExpressionNodePtr getBuildJoinKey();

    /**
   * @brief getter/setter for on probe join key
   */
    FieldAccessExpressionNodePtr getProbeJoinKey();

    /**
   * @brief getter build schema
   */
    SchemaPtr getBuildSchema();

    /**
   * @brief getter probe schema
   */
    SchemaPtr getProbeSchema();

    /**
     * @brief number of input edges. Need to define a clear concept for this
     * @experimental This is experimental API
     * @return
     */
    uint64_t getNumberOfInputEdgesBuild() const;

    /**
     * @brief number of input edges. Need to define a clear concept for this
     * @return
     */
    uint64_t getNumberOfInputEdgesProbe() const;

    /**
     * @brief Update the left and right stream types upon type inference
     * @param buildSchema
     * @param probeSchema
     */
    void updateInputSchemas(SchemaPtr buildSchema, SchemaPtr probeSchema);

    /**
     * @brief Update the output stream type upon type inference
     * @param outputSchema the type of the output stream
     */
    void updateOutputDefinition(SchemaPtr outputSchema);

    /**
     * @brief Getter of the output stream schema
     * @return the output stream schema
     */
    [[nodiscard]] SchemaPtr getOutputSchema() const;

    void setNumberOfInputEdgesBuild(uint64_t numberOfInputEdgesLeft);
    void setNumberOfInputEdgesProbe(uint64_t numberOfInputEdgesRight);

  private:
    FieldAccessExpressionNodePtr keyTypeBuild;
    FieldAccessExpressionNodePtr keyTypeProbe;
    SchemaPtr buildSchema{nullptr};
    SchemaPtr probeSchema{nullptr};
    SchemaPtr outputSchema{nullptr};
    uint64_t numberOfInputEdgesBuild;
    uint64_t numberOfInputEdgesProbe;
};

using LogicalBatchJoinDefinitionPtr = std::shared_ptr<LogicalBatchJoinDefinition>;
}// namespace NES::Join::Experimental
#endif// NES_CORE_INCLUDE_WINDOWING_LOGICALBATCHJOINDEFINITION_HPP_

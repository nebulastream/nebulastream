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

#pragma once

#include <Execution/Operators/DelayTuples.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

/**
 * @brief Physical DelayTuples operator.
 */
class PhysicalDelayTuplesOperator : public PhysicalUnaryOperator, public AbstractScanOperator, public AbstractEmitOperator
{
public:
    /**
     * @brief Constructor for the physical DelayTuples operator
     * @param id operator id
     * @param inputSchema input schema for the DelayTuples operator
     */
    PhysicalDelayTuplesOperator(
        OperatorId id, const SchemaPtr& inputSchema, float const& unorderedness, uint64_t const& minDelay, uint64_t const& maxDelay);

    /**
     * @brief Creates a physical DelayTuples operator
     * @param id operator id
     * @param inputSchema
     * @return PhysicalOperatorPtr
     */
    static PhysicalOperatorPtr
    create(OperatorId id, const SchemaPtr& inputSchema, float const& unorderedness, uint64_t const& minDelay, uint64_t const& maxDelay);

    /**
     * @brief Creates a physical DelayTuples operator
     * @param inputSchema
     * @return PhysicalOperatorPtr
     */
    static PhysicalOperatorPtr
    create(SchemaPtr inputSchema, float const& unorderedness, uint64_t const& minDelay, uint64_t const& maxDelay);

    float getUnorderedness() const;

    uint64_t getMinDelay() const;

    uint64_t getMaxDelay() const;

    std::string toString() const override;

    OperatorPtr copy() override;

protected:
    float unorderedness;
    uint64_t minDelay;
    uint64_t maxDelay;
};
} // namespace NES::QueryCompilation::PhysicalOperators

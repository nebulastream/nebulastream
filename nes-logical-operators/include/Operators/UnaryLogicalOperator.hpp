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

#include <vector>
#include <Operators/LogicalOperator.hpp>

namespace NES
{

/// @brief Logical unary operator. It hat at most one input data source.
class UnaryLogicalOperator : public LogicalOperator
{
public:
    explicit UnaryLogicalOperator();

    /// @brief infers the input and out schema of this operator depending on its child.
    /// @throws Exception if the schema could not be infers correctly or if the inferred types are not valid.
    /// @param typeInferencePhaseContext needed for stamp inferring
    /// @return true if schema was correctly inferred
    bool inferSchema() override;

    /// @brief infers the origin id from the child operator.
    void inferInputOrigins() override;

    void setOutputSchema(const Schema& schema) override
    {
        outputSchema = schema;
    }

    Schema getOutputSchema() const override
    {
        return outputSchema;
    }

    void setInputSchema(const Schema& schema) {
        inputSchema = schema;
    }

    [[nodiscard]] Schema getInputSchema() const
    {
        return inputSchema;
    }

    void setInputOriginIds(const std::vector<OriginId>& originIds)
    {
        inputOriginIds = originIds;
    }

    std::vector<OriginId> getInputOriginIds() const
    {
        return inputOriginIds;
    }

    std::vector<OriginId> getOutputOriginIds() const override
    {
        return inputOriginIds;
    }

protected:
    Schema inputSchema;
    std::vector<OriginId> inputOriginIds;
};
}

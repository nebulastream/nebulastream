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

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators
{

class PhysicalShuffleBufferOperator : public PhysicalUnaryOperator
{
public:
    PhysicalShuffleBufferOperator(
        OperatorId id,
        const std::shared_ptr<Schema>& inputSchema,
        const float& degreeOfDisorder,
        const std::chrono::milliseconds& minDelay,
        const std::chrono::milliseconds& maxDelay);
    static std::shared_ptr<PhysicalOperator> create(
        OperatorId id,
        const std::shared_ptr<Schema>& inputSchema,
        const float& degreeOfDisorder,
        const std::chrono::milliseconds& minDelay,
        const std::chrono::milliseconds& maxDelay);
    static std::shared_ptr<PhysicalOperator> create(
        std::shared_ptr<Schema> inputSchema,
        const float& degreeOfDisorder,
        const std::chrono::milliseconds& minDelay,
        const std::chrono::milliseconds& maxDelay);
    float getUnorderedness() const;
    std::chrono::milliseconds getMinDelay() const;
    std::chrono::milliseconds getMaxDelay() const;
    std::string toString() const override;
    std::shared_ptr<Operator> copy() override;

protected:
    float degreeOfDisorder;
    std::chrono::milliseconds minDelay;
    std::chrono::milliseconds maxDelay;
};
}

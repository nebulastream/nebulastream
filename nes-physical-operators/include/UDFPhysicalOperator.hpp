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

#include <memory>
#include <optional>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

class UdfStage;

/// Runs the UDFs of a UDFLogicalOperator in the worker: marshals the arguments of all of them, makes one call into the UdfStage
/// per record, and writes the results to the record.
class UDFPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    struct Argument
    {
        PhysicalFunction function;
        DataType declaredType;
    };

    struct Result
    {
        Record::RecordFieldIdentifier field;
        DataType type;
    };

    /// Loads the UDFs. `arguments` holds the arguments of all of them one after the other, and `results` one entry per UDF.
    UDFPhysicalOperator(std::vector<Argument> arguments, std::vector<Result> results, const std::vector<UdfDescriptor>& udfs);
    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::vector<Argument> arguments;
    std::vector<Result> results;
    std::shared_ptr<UdfStage> stage;

    std::optional<PhysicalOperator> child;
};

}

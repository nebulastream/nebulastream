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

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <UdfDescriptor.hpp>
#include <UdfInterpreter.hpp>

namespace NES
{

class UdfStage;
class ArrowBufferRef;

/// Runs the UDFs of a UDFLogicalOperator in the worker on batches of records, like a pipeline breaker inside the pipeline: execute
/// writes each record together with the values of the UDFs' arguments into an Arrow batch. Once the batch is full, and on close,
/// every UDF runs once on the batch, see UdfBackend::executeBatch, its result becomes a column of the batch, and the records of the
/// batch go on to the child. The child is opened and closed once, with the operator.
class UDFBatchPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    struct Argument
    {
        PhysicalFunction function;
        DataType declaredType;
        /// The column of the batch that holds its values.
        Record::RecordFieldIdentifier field;
    };

    struct Result
    {
        Record::RecordFieldIdentifier field;
        DataType type;
    };

    /// What a batch needs at runtime: where the arguments and results of each UDF are in `layout`.
    struct Batch
    {
        std::shared_ptr<UdfStage> stage;
        std::shared_ptr<ArrowBufferRef> layout;
        /// Per UDF, the columns of its arguments and the column of its result.
        std::vector<std::vector<size_t>> argumentColumns;
        std::vector<size_t> resultColumns;
        /// The argument columns of all UDFs one after the other, for a fused stage.
        std::vector<size_t> allArgumentColumns;
    };

    /// Loads the UDFs. `arguments` holds the arguments of all of them one after the other, and `results` one entry per UDF.
    /// `batchLayout` has a column for each field of the input records, each argument and each result. The records that go on to
    /// the child have the `outputFields`. With `fused`, all UDFs run on a batch in one call into the language, see UdfStage.
    UDFBatchPhysicalOperator(
        std::vector<Argument> arguments,
        std::vector<Result> results,
        const std::vector<UdfDescriptor>& udfs,
        UdfInterpreter interpreter,
        std::shared_ptr<ArrowBufferRef> batchLayout,
        std::vector<Record::RecordFieldIdentifier> outputFields,
        bool fused = false);
    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    /// Runs the UDFs on the batch in the operator's state and passes its records to the child.
    void processBatch(ExecutionContext& ctx) const;

    std::vector<Argument> arguments;
    std::vector<Record::RecordFieldIdentifier> outputFields;
    std::shared_ptr<Batch> batch;

    std::optional<PhysicalOperator> child;
};

}

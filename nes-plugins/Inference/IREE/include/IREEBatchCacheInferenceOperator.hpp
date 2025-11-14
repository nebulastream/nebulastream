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
#include <Functions/PhysicalFunction.hpp>
#include <PhysicalOperator.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <WindowProbePhysicalOperator.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <PredictionCacheConfiguration.hpp>

namespace NES
{

class IREEBatchCacheInferenceOperator : public WindowProbePhysicalOperator
{
public:
    IREEBatchCacheInferenceOperator(
        const OperatorHandlerId operatorHandlerId,
        std::vector<PhysicalFunction> inputs,
        std::vector<std::string> outputFieldNames,
        std::shared_ptr<Interface::BufferRef::TupleBufferRef> tupleBufferRef,
        Configurations::PredictionCacheOptions predictionCacheOptions);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void setup(ExecutionContext& executionCtx, CompilationContext&) const override;

    [[nodiscard]] Record createRecord(const Record& featureRecord, const std::vector<Record::RecordFieldIdentifier>& projections) const;

    [[nodiscard]] std::optional<struct PhysicalOperator> getChild() const override { return child; }
    void setChild(PhysicalOperator child) override { this->child = std::move(child); }

    bool isVarSizedInput = false;
    bool isVarSizedOutput = false;
    size_t outputSize = 0;
    size_t inputSize = 0;

private:
    const std::vector<PhysicalFunction> inputs;
    const std::vector<std::string> outputFieldNames;
    std::optional<PhysicalOperator> child;
    std::shared_ptr<Interface::BufferRef::TupleBufferRef> tupleBufferRef;
    Configurations::PredictionCacheOptions predictionCacheOptions;

protected:
    void performInference(
        const Interface::PagedVectorRef& pagedVectorRef,
        Interface::BufferRef::TupleBufferRef& tupleBufferRef,
        ExecutionContext& executionCtx) const;
};

}

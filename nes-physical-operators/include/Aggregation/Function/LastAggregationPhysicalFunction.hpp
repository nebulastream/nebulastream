/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
*/

#pragma once

#include <cstddef>
#include <memory>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include "DataTypes/DataType.hpp"
#include "Functions/PhysicalFunction.hpp"
#include "Interface/Record.hpp"
#include <val_base.hpp>
#include <val_ptr.hpp>
#include "Runtime/TupleBuffer.hpp"
#include "ExecutionContext.hpp"
#include "Interface/TimestampRef.hpp"
#include "Time/Timestamp.hpp"

namespace NES
{

class LastAggregationPhysicalFunction final : public AggregationPhysicalFunction
{
public:
    LastAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<PagedVectorTupleLayout> tupleLayout);

    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        nautilus::val<TupleBuffer*> parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record,
        const nautilus::val<Timestamp>& timestamp,
        const AggregationInputBuffer& inputBuffer) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<TupleBuffer*> parentBuffer1,
        nautilus::val<AggregationState*> aggregationState2,
        nautilus::val<TupleBuffer*> parentBuffer2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<TupleBuffer*> parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(
        nautilus::val<AggregationState*> aggregationState,
        nautilus::val<TupleBuffer*> parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;

private:
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout;
};

}

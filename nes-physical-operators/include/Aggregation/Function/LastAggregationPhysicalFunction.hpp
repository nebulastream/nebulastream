/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
*/

#pragma once

#include <cstddef>
#include <memory>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>

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
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record,
        const nautilus::val<Timestamp>& timestamp) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;

private:
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout;
};

}

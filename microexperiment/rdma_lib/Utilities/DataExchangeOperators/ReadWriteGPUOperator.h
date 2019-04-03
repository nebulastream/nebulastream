//
// Created by Toso, Lorenzo on 2019-03-28.
//

#pragma once

#include "ReadWriteOperator.h"
#include <atomic>
#include "cuda_includes.h"

class ReadWriteGPUOperator : public ReadWriteOperator
{
private:

    std::atomic_bool can_start_gpu;

public:
    explicit ReadWriteGPUOperator(ConnectionCollection &connections)
    : ReadWriteOperator(connections)
    , can_start_gpu(false) {}


    void start_gpu();

private:
    void receive_tuples_from(size_t target_rank, const std::function<void(const StructuredTupleBuffer&, std::atomic_char*)> & tuple_processor) override;
};




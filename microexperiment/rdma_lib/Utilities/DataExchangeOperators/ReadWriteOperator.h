//
// Created by Toso, Lorenzo on 2019-03-18.
//

#pragma once

#include <SynchronizedBlockwiseQueue.h>
#include "AbstractDataExchangeOperator.h"



#define BUFFER_USED_SENDER_DONE 127
#define BUFFER_READY_FLAG 0
#define BUFFER_USED_FLAG 1
#define BUFFER_BEING_PROCESSED_FLAG 2

static_assert(JOIN_WRITE_BUFFER_SIZE > (WRITE_RECEIVE_BUFFER_COUNT+1) * sizeof(RegionToken), "JOIN_WRITE_BUFFER_SIZE > (WRITE_RECEIVE_BUFFER_COUNT+1) * sizeof(RegionToken)");

class ReadWriteOperator : public AbstractDataExchangeOperator {

public:
    explicit ReadWriteOperator(ConnectionCollection &connections)
            : AbstractDataExchangeOperator(connections){}

private:
    void receive_tuples_from(size_t target_rank, const std::function<void(const StructuredTupleBuffer&, std::atomic_char*)> & tuple_processor) override;
    void send_matching_tuples_to (size_t target_rank, const ProbeTable * probeTable, const Limits & limits, const std::function<bool(size_t)> & matcher) override;
    void send_tuples_to_all (const ProbeTable * probeTable, const Limits & limits) override;

    void read_sign_buffer(size_t target_rank, Buffer* sign_buffer, RegionToken* sign_token) const;

    void copy_received_tokens(const std::vector<StructuredTupleBuffer> &sendBuffers, std::vector<RegionToken*> &region_tokens, RegionToken*&sign_token) const;
};




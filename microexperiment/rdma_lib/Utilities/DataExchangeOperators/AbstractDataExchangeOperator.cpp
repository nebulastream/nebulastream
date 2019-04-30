//
// Created by Toso, Lorenzo on 2019-03-18.
//

#include "AbstractDataExchangeOperator.h"

Timestamp AbstractDataExchangeOperator::duration() const
{
    return *std::max_element(measured_network_times.begin(), measured_network_times.end());;
}

void AbstractDataExchangeOperator::start()
{
    can_start = true;
}

void AbstractDataExchangeOperator::join()
{
    for(auto & t : threads)
        t.join();
    can_start = false;
}

bool AbstractDataExchangeOperator::all_done_sending() const
{
    return std::all_of(done_with_sending.begin(), done_with_sending.end(), [](bool b) { return b; });
}

void AbstractDataExchangeOperator::async_receive_tuples_from(size_t target_rank, const std::function<void(const StructuredTupleBuffer &, std::atomic_char*)> &tuple_processor)
{
    TRACE("Starting routine to receiver tuples from %lu!\n", rank);
    threads.emplace_back([target_rank, tuple_processor, this](){ receive_tuples_from(target_rank, tuple_processor); });
    TRACE("There are now %lu threads in the vector\n", threads.size());
}

void AbstractDataExchangeOperator::async_send_tuples_to_all(const ProbeTable* probeTable, const Limits &limits)
{
    threads.emplace_back([&probeTable, limits, this](){ send_tuples_to_all(probeTable, limits); });
}

void AbstractDataExchangeOperator::async_send_matching_tuples_to(size_t target_rank, const ProbeTable* probeTable, const Limits &limits, const std::function<bool(size_t)> &matcher)
{
    TRACE("Starting routine to send tuples to %lu!\n", target_rank);
    threads.emplace_back([target_rank, probeTable, limits, matcher, this](){ send_matching_tuples_to(target_rank, probeTable, limits, matcher); });
    TRACE("There are now %lu threads in the vector\n", threads.size());
}

void AbstractDataExchangeOperator::prepare_send_buffer( StructuredTupleBuffer &structuredSendBuffer,
                                                        size_t &next_tuple_index,
                                                        const ProbeTable* probeTable,
                                                        const Limits &limits,
                                                        const std::function<bool(size_t)> &matcher) const
{

    size_t tuples_in_this_buffer = 0;

    for(; next_tuple_index < limits.end_index; next_tuple_index++)
    {

        auto & join_key = probeTable->keys[next_tuple_index];

        if(!matcher(join_key)) {
            continue;
        }

        structuredSendBuffer.key_ptr[tuples_in_this_buffer] = probeTable->keys[next_tuple_index];
        structuredSendBuffer.payload_ptr[tuples_in_this_buffer] = probeTable->payloads[next_tuple_index];

        //TRACE("Assigned\n");
        tuples_in_this_buffer++;

        if(tuples_in_this_buffer == structuredSendBuffer.max_elements_per_buffer) {
            next_tuple_index++;
            break;
        }
    }
    //TRACE("Broke\n");
    *structuredSendBuffer.count_ptr = tuples_in_this_buffer;

    if(tuples_in_this_buffer < structuredSendBuffer.max_elements_per_buffer)
    {
        // Buffer is not full! Since we assumed it would be, our payloads are now off! Moving them to the right position (Directly after the keys)
        memmove(structuredSendBuffer.key_ptr + tuples_in_this_buffer, structuredSendBuffer.payload_ptr, tuples_in_this_buffer * sizeof(PPayload));
    }
}

AbstractDataExchangeOperator::AbstractDataExchangeOperator(ConnectionCollection &connections)
: connections(connections)
{
    can_start = false;
    size_t process_count = MPIHelper::get_process_count();

    done_with_sending.resize(process_count, false);
    measured_network_times.resize(process_count*2, 0);

}


//
// Created by Toso, Lorenzo on 2019-03-18.
//

#pragma once
#include <atomic>
#include <thread>
#include <algorithm>
#include <SynchronizedBlockwiseQueue.h>
#include "Debug.h"
#include "ConnectionCollection.h"
#include "TimeTools.hpp"
#include "MPIHelper.h"
#include "Tables.h"
#include "Limits.h"


struct StructuredTupleBuffer {
    const size_t size;
    const size_t max_elements_per_buffer;


    Buffer * send_buffer = nullptr;
    size_t * count_ptr = nullptr;
    size_t * key_ptr = nullptr;
    PPayload * payload_ptr = nullptr;
    RequestToken * requestToken = nullptr;

    StructuredTupleBuffer(void* memory, size_t size)
            : size(size)
            , max_elements_per_buffer((size - sizeof(size_t)) / (sizeof(size_t) + sizeof(PPayload))) {
        count_ptr = (size_t*)memory;
        key_ptr = count_ptr + 1;
        payload_ptr = reinterpret_cast<PPayload*>(key_ptr + max_elements_per_buffer);
    }


    StructuredTupleBuffer(VerbsConnection & connection, size_t size)
            : size(size)
            , max_elements_per_buffer((size - sizeof(size_t)) / (sizeof(size_t) + sizeof(PPayload))) {
        send_buffer = connection.allocate_buffer(size);
        count_ptr = (size_t*)send_buffer->getData();
        key_ptr = count_ptr + 1;
        payload_ptr = reinterpret_cast<PPayload*>(key_ptr + max_elements_per_buffer);
        requestToken = connection.create_request_token();
        requestToken->setCompleted(true);
    }

    StructuredTupleBuffer(StructuredTupleBuffer && other)
    :size(other.size)
    ,max_elements_per_buffer(other.max_elements_per_buffer){
        std::swap(send_buffer, other.send_buffer);
        std::swap(count_ptr, other.count_ptr);
        std::swap(key_ptr, other.key_ptr);
        std::swap(payload_ptr, other.payload_ptr);
        std::swap(requestToken, other.requestToken);
    }

    virtual ~StructuredTupleBuffer() {
        if(send_buffer != nullptr){
            TRACE("StructuredSendBuffer::DESTRUCTOR CALLED!!!\n")
            delete send_buffer;
        }
        delete requestToken;
    }

};
//
//class AbstractDataExchangeOperator {
//
//private:
//
//protected:
//    std::atomic_bool can_start;
//    ConnectionCollection & connections;
//    std::vector<bool> done_with_sending;
//    std::vector<Timestamp> measured_network_times;
//    std::vector<std::thread> threads;
//
//public:
//    explicit AbstractDataExchangeOperator(ConnectionCollection &connections);
//
//    Timestamp duration() const;
//
//    void start();
//    void join();
//
//    bool all_done_sending() const;
//
//    void async_receive_tuples_from(size_t target_rank, const std::function<void(const StructuredTupleBuffer &, std::atomic_char*)> & tuple_processor);
//    void async_send_tuples_to_all(const ProbeTable * probeTable, const Limits & limits);
//    void async_send_matching_tuples_to(size_t target_rank, const ProbeTable * probeTable, const Limits & limits, const std::function<bool(size_t)> &matcher);
//
//
//protected:
//    virtual void send_tuples_to_all (const ProbeTable * probeTable, const Limits & limits) = 0;
//    virtual void send_matching_tuples_to (size_t target_rank, const ProbeTable * probeTable, const Limits & limits, const std::function<bool(size_t)> & matcher) = 0;
//    virtual void receive_tuples_from(size_t target_rank, const std::function<void(const StructuredTupleBuffer &, std::atomic_char*)> & tuple_processor) = 0;
//
//    void prepare_send_buffer(
//            StructuredTupleBuffer & structuredSendBuffer,
//            size_t & next_tuple_index,
//            const ProbeTable * probeTable,
//            const Limits & limits,
//            const std::function<bool(size_t)> & matcher
//            ) const;
//
//};




//
// Created by Toso, Lorenzo on 2019-03-29.
//

#include "SendReceiveOperator.h"

void SendReceiveOperator::receive_tuples_from(size_t target_rank, const std::function<void(const StructuredTupleBuffer &, std::atomic_char*)> &tuple_processor)
{
    TRACE("Started routine to receive tuples from %lu!\n", target_rank);

    std::vector<Buffer*> recv_buffers(JOIN_SEND_BUFFER_COUNT);
    for(size_t i = 0; i < JOIN_SEND_BUFFER_COUNT; i++){
        recv_buffers[i] = connections[target_rank].allocate_buffer(JOIN_SEND_BUFFER_SIZE);
        connections[target_rank].post_receive(recv_buffers[i]);
    }

    TRACE("can not start receiving tuples from (%lu)\n", target_rank);
    while(!can_start){
        std::this_thread::yield();
    };
    TRACE("can start receiving tuples from (%lu)\n", target_rank);
    size_t total_tuple_count = 0;

    auto start_time = TimeTools::now();

    ReceiveElement receiveElement;
    while(!done_with_sending[target_rank]){
        connections[target_rank].wait_for_receive(receiveElement);

        size_t received_elements = *(size_t*)receiveElement.buffer->getData();

        if (received_elements > 0) {
            TRACE("Received something from rank %lu!\n", target_rank);

            tuple_processor(StructuredTupleBuffer(receiveElement.buffer->getData(), JOIN_SEND_BUFFER_SIZE), nullptr);

            connections[target_rank].post_receive(receiveElement.buffer);
            total_tuple_count += received_elements;
            //TRACE("Recv request to %lu successful. Received %lu elements!\n", target_rank, received_elements);
        }
        else {
            TRACE("FINISH REQUEST 0 FROM PROCESS %lu!\n", target_rank);
            for(size_t i = 1; i < JOIN_SEND_BUFFER_COUNT; i++){
                connections[target_rank].wait_for_receive(receiveElement);
                TRACE("FINISH REQUEST %lu FROM PROCESS %lu!\n", i, target_rank);
            }
            done_with_sending[target_rank] = true;
            TRACE("PROCESS %lu IS DONE!\n", target_rank);
        }
    }
    auto end_time = TimeTools::now();
    measured_network_times[MPIHelper::get_rank()] = end_time - start_time;
    TRACE2("Received a total of %lu tuples from rank %lu\n", total_tuple_count, target_rank);

    for(size_t i = 0; i < JOIN_SEND_BUFFER_COUNT; i++){
        delete recv_buffers[i];
    }
    TRACE("%lu- Deleted all recv for target rank %lu\n", TimeTools::millis(), target_rank);
}

void SendReceiveOperator::send_tuples_to_all(const ProbeTable* probeTable, const Limits &limits)
{

    TRACE("Initializing send-thread\n");
    size_t process_count = MPIHelper::get_process_count();
    size_t rank = MPIHelper::get_rank();

    void * send_buffer = malloc(JOIN_SEND_BUFFER_SIZE);

    std::vector<Buffer*> buffers(process_count);
    std::vector<RequestToken*> request_tokens(process_count);

    for(size_t i = 0; i < process_count; i++){
        if(i == rank)
            continue;
        buffers[i] = connections[i].register_buffer(send_buffer, JOIN_SEND_BUFFER_SIZE);
        request_tokens[i] = connections[i].create_request_token();
    }

    TRACE("Registered buffers\n");

    size_t max_elements_per_buffer = (JOIN_SEND_BUFFER_SIZE - sizeof(size_t)) / (sizeof(size_t) + sizeof(PPayload));

    auto count_ptr = (size_t*)send_buffer;
    auto key_ptr = count_ptr + 1;
    auto payload_ptr = (PPayload*)(key_ptr + max_elements_per_buffer);


    while(!can_start){
        std::this_thread::yield();
    };

    TRACE("Starting to send tuples to peers.\n")
    auto start_time = TimeTools::now();

    for (size_t tuple_index = limits.start_index; tuple_index < limits.end_index; tuple_index+=max_elements_per_buffer) {
        *count_ptr = std::min(limits.end_index - tuple_index, max_elements_per_buffer);

        if(*count_ptr == 0)
            break;

        memcpy(key_ptr, probeTable->keys.data() + tuple_index, (*count_ptr) * sizeof(size_t));
        memcpy(payload_ptr, probeTable->payloads.data() + tuple_index, (*count_ptr) * sizeof(PPayload));

        for ( size_t i = 0; i < process_count; i++) {
            if(i == rank)
                continue;
            connections[i].send(buffers[i], request_tokens[i]);
            //TRACE("Sending %lu tuples to %lu\n", *count_ptr, i);
        }
        for ( size_t i = 0; i < process_count; i++) {
            if(i == rank)
                continue;
            request_tokens[i]->waitUntilCompleted();
            //TRACE("Finished Sending %lu tuples to %lu\n", *count_ptr, i);
        }
    }
    TRACE("Done sending tuples\n");

    done_with_sending[rank] = true;
    auto end_time = TimeTools::now();
    measured_network_times[process_count + rank] = end_time - start_time;


    *count_ptr = 0;
    for (size_t i = 0; i < process_count; i++) {
        if(i == rank)
            continue;

        for (size_t j = 0; j < JOIN_SEND_BUFFER_COUNT; j++) {
            TRACE("SENDING FINISH MESSAGE %lu TO %lu\n", j, i)
            if (j == JOIN_SEND_BUFFER_COUNT - 1) {
                TRACE("DONE SENDING FINISH MESSAGES TO %lu\n", i)
            }
            connections[i].send_blocking(buffers[i]);
        }
    }

    for (size_t i = 0; i < process_count; i++) {
        if(i == rank)
            continue;
        TRACE("Deleting buffer %lu\n", i)
        delete(buffers[i]);
        delete(request_tokens[i]);
    }
    TRACE("Freeing send buffer\n")
    free(send_buffer);
}

void SendReceiveOperator::send_matching_tuples_to(size_t target_rank, const ProbeTable* probeTable, const Limits &limits, const std::function<bool(size_t)> &matcher)
{

    TRACE("send_matching_tuples_to(%lu)\n", target_rank);

    RequestToken* request_token = connections[target_rank].create_request_token();


    StructuredTupleBuffer structuredSendBuffer(connections[target_rank], JOIN_SEND_BUFFER_SIZE);

    TRACE("can not start sending tuples to (%lu)\n", target_rank);
    while(!can_start){TRACE("!can_start sending\n");};
    TRACE("can start sending tuples to (%lu)\n", target_rank);
    size_t total_tuple_count = 0;

    auto start_time = TimeTools::now();

    size_t tuples_tested_until_buffer_was_full = 0;
    for (size_t index = limits.start_index; index < limits.end_index; index += tuples_tested_until_buffer_was_full) {

        size_t tuples_in_this_buffer = 0;

        size_t this_buffer_index = 0;
        for(this_buffer_index = 0; tuples_in_this_buffer < structuredSendBuffer.max_elements_per_buffer && (this_buffer_index+index) < limits.end_index ; this_buffer_index++){
            auto & join_key = probeTable->keys[index + this_buffer_index];

            if(!matcher(join_key)) {
                continue;
            }

            structuredSendBuffer.key_ptr[tuples_in_this_buffer] = probeTable->keys[index + this_buffer_index];
            structuredSendBuffer.payload_ptr[tuples_in_this_buffer] = probeTable->payloads[index + this_buffer_index];
            tuples_in_this_buffer++;
            //TRACE("added %lu tuples to buffer for rank (%lu)\n", tuples_in_this_buffer, target_rank);
        }
        tuples_tested_until_buffer_was_full = this_buffer_index;

        if (tuples_in_this_buffer == 0) // Were through and we didn't add any tuple to the buffer!
            break;


        *structuredSendBuffer.count_ptr = tuples_in_this_buffer;
        total_tuple_count += tuples_in_this_buffer;

        if(tuples_in_this_buffer < structuredSendBuffer.max_elements_per_buffer)
        {// Buffer is not full! Since we assumed it would be, our payloads are now off! Moving them to the right position (Directly after the keys)
            memmove(structuredSendBuffer.key_ptr + tuples_in_this_buffer, structuredSendBuffer.payload_ptr, tuples_in_this_buffer * sizeof(PPayload));
        }

        TRACE("Sending %lu tuples to %lu\n", tuples_in_this_buffer, target_rank);
        connections[target_rank].send_blocking(structuredSendBuffer.send_buffer); //todo muss eigentlich nicht blocking sein
        TRACE("Done Sending %lu tuples to %lu\n", tuples_in_this_buffer, target_rank);
    }
    TRACE("Done sending. Sending EOF messages.\n");
    TRACE2("Sent a total of %lu tuples to rank %lu\n", total_tuple_count, target_rank);
    *structuredSendBuffer.count_ptr = 0;
    for(size_t i = 0; i < JOIN_SEND_BUFFER_COUNT; i++){
        connections[target_rank].send_blocking(structuredSendBuffer.send_buffer);
        TRACE("Sent Finish request %lu to %lu\n", i+1, target_rank);
    }

    done_with_sending[MPIHelper::get_rank()] = true;
    TRACE("Set done sending flag\n");
    auto end_time = TimeTools::now();
    measured_network_times[MPIHelper::get_process_count() + target_rank] = end_time - start_time;

    delete request_token;
}

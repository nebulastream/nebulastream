//
// Created by Toso, Lorenzo on 2019-03-18.
//

#include "ReadWriteOperator.h"

void ReadWriteOperator::receive_tuples_from(size_t target_rank, const std::function<void(const StructuredTupleBuffer &, std::atomic_char*)> &tuple_processor)
{

    TRACE("Started routine to receive tuples from %lu!\n", target_rank);

    std::vector<Buffer*> recv_buffers(WRITE_RECEIVE_BUFFER_COUNT);
    std::vector<RegionToken*> region_tokens(WRITE_RECEIVE_BUFFER_COUNT+1);
    std::vector<std::atomic_char> buffer_ready_sign(WRITE_RECEIVE_BUFFER_COUNT);
    for(auto & r : buffer_ready_sign)
        r = BUFFER_READY_FLAG;

    Buffer * sign_buffer = nullptr;

    for(size_t i = 0; i < WRITE_RECEIVE_BUFFER_COUNT+1; i++)
    {
        if (i < WRITE_RECEIVE_BUFFER_COUNT) {
            recv_buffers[i] = connections[target_rank].allocate_buffer(JOIN_WRITE_BUFFER_SIZE);
            region_tokens[i] = recv_buffers[i]->createRegionToken();
        } else {
            sign_buffer = connections[target_rank].register_buffer(buffer_ready_sign.data(), WRITE_RECEIVE_BUFFER_COUNT);
            region_tokens[i] = sign_buffer->createRegionToken();
        }
        memcpy((RegionToken*)recv_buffers[0]->getData() + i, region_tokens[i], sizeof(RegionToken));
    }

    TRACE("PREPARED EVERYTHING FOR RECEIVING!\n");
    while(!can_start){
        std::this_thread::yield();
    }
    auto start_time = TimeTools::now();

    connections[target_rank].send_blocking(recv_buffers[0]);


    size_t total_received_tuples = 0;
    size_t index = 0;

    while(true)
    {
        index ++;
        index %= WRITE_RECEIVE_BUFFER_COUNT;

        if (buffer_ready_sign[index] == BUFFER_USED_FLAG || buffer_ready_sign[index] == BUFFER_USED_SENDER_DONE)
        {
            bool is_done = buffer_ready_sign[index] == BUFFER_USED_SENDER_DONE;

            if(is_done) // this is done so that the loop later doesnt try to process this again
                buffer_ready_sign[index] = BUFFER_READY_FLAG;

            total_received_tuples += ((size_t*) recv_buffers[index]->getData())[0];
            TRACE2("Received %lu tuples from %lu on buffer %lu\n", ((size_t*) recv_buffers[index]->getData())[0], target_rank, index);

            tuple_processor(StructuredTupleBuffer(recv_buffers[index]->getData(), JOIN_WRITE_BUFFER_SIZE), &buffer_ready_sign[index]);

            if(is_done)
                break;
        }
    }

    for(index = 0; index < WRITE_RECEIVE_BUFFER_COUNT; index++)
    {
        if (buffer_ready_sign[index] == BUFFER_USED_FLAG) {
            total_received_tuples += ((size_t*) recv_buffers[index]->getData())[0];
            tuple_processor(StructuredTupleBuffer(recv_buffers[index]->getData(), JOIN_WRITE_BUFFER_SIZE), &buffer_ready_sign[index]);
        }
    }
    done_with_sending[target_rank] = true;

    TRACE2("Done receiving! Received a total of %lu elements.\n", total_received_tuples);

    auto end_time = TimeTools::now();
    measured_network_times[MPIHelper::get_rank()] = end_time - start_time;


    for (auto & token : region_tokens)
        delete token;
    for (auto & buffer : recv_buffers)
        delete buffer;
}

void ReadWriteOperator::send_matching_tuples_to(size_t target_rank, const ProbeTable* probeTable, const Limits &limits, const std::function<bool(size_t)> &matcher)     {
    TRACE("send_matching_tuples_to!\n");

    std::vector<RegionToken*> region_tokens(WRITE_RECEIVE_BUFFER_COUNT);

    std::vector<StructuredTupleBuffer> sendBuffers;
    for(size_t i = 0; i < WRITE_SEND_BUFFER_COUNT; i++)
        sendBuffers.emplace_back(StructuredTupleBuffer(connections[target_rank], JOIN_WRITE_BUFFER_SIZE));

    std::vector<char> buffer_ready_sign(WRITE_RECEIVE_BUFFER_COUNT, BUFFER_READY_FLAG);
    auto sign_buffer = connections[target_rank].register_buffer(buffer_ready_sign.data(), WRITE_RECEIVE_BUFFER_COUNT);
    RegionToken* sign_token = nullptr;


    TRACE2("Blocking to receive tokens!!\n");
    connections[target_rank].post_and_receive_blocking(sendBuffers[0].send_buffer);
    TRACE2("Received tokens!!\n");
    copy_received_tokens(sendBuffers, region_tokens, sign_token);

    TRACE2("PREPARED EVERYTHING FOR SENDING!\n");

    while(!can_start){
        std::this_thread::yield();
    }
    auto start_time = TimeTools::now();

    size_t next_tuple_index = limits.start_index;

    size_t total_sent_tuples = 0;
    size_t send_buffer_index = 0;

    for(size_t receive_buffer_index = 0; receive_buffer_index < WRITE_RECEIVE_BUFFER_COUNT; receive_buffer_index=(receive_buffer_index+1)%WRITE_RECEIVE_BUFFER_COUNT)
    {
        if(receive_buffer_index == 0)
        {
            read_sign_buffer(target_rank, sign_buffer, sign_token);
        }
        if(buffer_ready_sign[receive_buffer_index] == BUFFER_READY_FLAG)
        {
            prepare_send_buffer(sendBuffers[send_buffer_index], next_tuple_index, probeTable, limits, matcher);

            sendBuffers[send_buffer_index].requestToken->waitUntilCompleted();
            connections[target_rank].write(sendBuffers[send_buffer_index].send_buffer, region_tokens[receive_buffer_index], sendBuffers[send_buffer_index].requestToken);

            TRACE2("Writing %lu tuples to rank %lu on buffer %lu!\n", *sendBuffers[send_buffer_index].count_ptr, target_rank, receive_buffer_index);
            total_sent_tuples += *sendBuffers[send_buffer_index].count_ptr;

            if (next_tuple_index < limits.end_index){
                buffer_ready_sign[receive_buffer_index] = BUFFER_USED_FLAG;
                connections[target_rank].write(sign_buffer, sign_token, receive_buffer_index, receive_buffer_index, 1);
                TRACE2("Done writing sign_buffer\n");
            }
            else {
                buffer_ready_sign[receive_buffer_index] = BUFFER_USED_SENDER_DONE;
                connections[target_rank].write_blocking(sign_buffer, sign_token, receive_buffer_index, receive_buffer_index, 1);
                TRACE("Sent last tuples and marked as BUFFER_USED_SENDER_DONE\n");
                break;
            }
            send_buffer_index=(send_buffer_index+1)%WRITE_SEND_BUFFER_COUNT;

        }
    }
    TRACE2("Done sending! Sent a total of %lu elements.\n", total_sent_tuples);
    done_with_sending[MPIHelper::get_rank()] = true;
    auto end_time = TimeTools::now();
    measured_network_times[MPIHelper::get_process_count() + target_rank] = end_time - start_time;
}

void ReadWriteOperator::copy_received_tokens(const std::vector<StructuredTupleBuffer> &sendBuffers, std::vector<RegionToken*> &region_tokens, RegionToken*&sign_token) const {
    for(size_t i = 0; i <= WRITE_RECEIVE_BUFFER_COUNT; i++){
        if ( i < WRITE_RECEIVE_BUFFER_COUNT){
            region_tokens[i] = static_cast<RegionToken*>(malloc(sizeof(RegionToken)));
            memcpy(region_tokens[i], (RegionToken*)sendBuffers[0].send_buffer->getData() + i, sizeof(RegionToken));
        } else {
            sign_token = static_cast<RegionToken*>(malloc(sizeof(RegionToken)));
            memcpy(sign_token, (RegionToken*)sendBuffers[0].send_buffer->getData() + i, sizeof(RegionToken));
        }
    }
}

void ReadWriteOperator::read_sign_buffer(size_t target_rank, Buffer* sign_buffer, RegionToken* sign_token) const {
    TRACE("reading sign_buffer: \n");
    connections[target_rank].read_blocking(sign_buffer, sign_token);
    TRACE("sign_buffer: ");
    for(int i = 0; i < WRITE_RECEIVE_BUFFER_COUNT; i++)
            TRACE("%d ",buffer_ready_sign[i]);
    TRACE("\n");
    TRACE("Total Sent tuples: %lu\n", total_sent_tuples);
}

void ReadWriteOperator::send_tuples_to_all(const ProbeTable* probeTable, const Limits &limits) {
    for(size_t i = 0; i < MPIHelper::get_process_count(); i++){
        if(i == MPIHelper::get_rank())
            continue;
        async_send_matching_tuples_to(i, probeTable, limits, [](size_t key){ return true; });
    }
};

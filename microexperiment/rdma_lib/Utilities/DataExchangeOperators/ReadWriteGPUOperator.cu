//
// Created by Toso, Lorenzo on 2019-03-28.
//

#include "ReadWriteGPUOperator.h"

void ReadWriteGPUOperator::start_gpu() {
    can_start_gpu = true;
}


void ReadWriteGPUOperator::receive_tuples_from(size_t target_rank, const std::function<void(const StructuredTupleBuffer &, std::atomic_char*)> &tuple_processor) {

    TRACE2("Started routine to receive tuples from %lu!\n", target_rank);

    std::vector<void*> recv_buffer_memory(WRITE_RECEIVE_BUFFER_COUNT);
    std::vector<Buffer*> recv_buffers(WRITE_RECEIVE_BUFFER_COUNT);
    std::vector<RegionToken*> region_tokens(WRITE_RECEIVE_BUFFER_COUNT + 1);
    std::vector<std::shared_ptr<std::thread>> buffer_threads(WRITE_RECEIVE_BUFFER_COUNT, nullptr);

    std::vector<std::atomic_char> buffer_ready_sign(WRITE_RECEIVE_BUFFER_COUNT);
    for (auto &b : buffer_ready_sign)
        b = BUFFER_READY_FLAG;

    auto sign_buffer = connections[target_rank].register_buffer(buffer_ready_sign.data(), WRITE_RECEIVE_BUFFER_COUNT);


    while (!(can_start_gpu && can_start))
    {
        std::this_thread::yield();
    }
    auto start_time = TimeTools::now();

    TRACE2("Allocating GPU memory\n");

    for (size_t i = 0; i <= WRITE_RECEIVE_BUFFER_COUNT; i++)
    {
        if (i < WRITE_RECEIVE_BUFFER_COUNT)
        {
            cudaMalloc(&recv_buffer_memory[i], JOIN_WRITE_BUFFER_SIZE);

            recv_buffers[i] = connections[target_rank].register_buffer(recv_buffer_memory[i], JOIN_WRITE_BUFFER_SIZE);
            region_tokens[i] = recv_buffers[i]->createRegionToken();

            cudaMemcpy((RegionToken*) recv_buffers[0]->getData() + i, region_tokens[i], sizeof(RegionToken), cudaMemcpyHostToDevice);

            TRACE2("Done allocating buffer %lu\n", i);
        } else {
            region_tokens[WRITE_RECEIVE_BUFFER_COUNT] = sign_buffer->createRegionToken();

            cudaMemcpy((RegionToken*) recv_buffers[0]->getData() + WRITE_RECEIVE_BUFFER_COUNT, region_tokens[WRITE_RECEIVE_BUFFER_COUNT], sizeof(RegionToken), cudaMemcpyHostToDevice);
        }
    }
    TRACE2("Done copying last regiontoken\n");


    TRACE2("Starting to send tokens\n");
    connections[target_rank].send_blocking(recv_buffers[0]);
    TRACE2("Done sending tokens\n");

    size_t index = 0;
    while (true) {
        index++;
        index %= WRITE_RECEIVE_BUFFER_COUNT;

        if (buffer_ready_sign[index] == BUFFER_USED_SENDER_DONE)
            break;

        if (buffer_ready_sign[index] == BUFFER_USED_FLAG) {

            TRACE("Received something on buffer %lu!\n", index);
            if(buffer_threads[index] != nullptr)
                buffer_threads[index]->join();

            buffer_ready_sign[index] = BUFFER_BEING_PROCESSED_FLAG;
            buffer_threads[index] = std::make_shared<std::thread>([this,&recv_buffers,index, &buffer_ready_sign, &tuple_processor] {
                tuple_processor(
                        StructuredTupleBuffer(recv_buffers[index]->getData(), JOIN_WRITE_BUFFER_SIZE),
                        &buffer_ready_sign[index]);
            });
        }
    }

    for (index = 0; index < WRITE_RECEIVE_BUFFER_COUNT; index++)
    {
        if(buffer_threads[index] != nullptr)
            buffer_threads[index]->join();
        if (buffer_ready_sign[index] != BUFFER_READY_FLAG) {
            tuple_processor(StructuredTupleBuffer(recv_buffers[index]->getData(), JOIN_WRITE_BUFFER_SIZE),
                &buffer_ready_sign[index]
            );
        }
    }

    done_with_sending[target_rank] = true;

    auto end_time = TimeTools::now();
    measured_network_times[MPIHelper::get_rank()] = end_time - start_time;


    for (auto &token : region_tokens)
        delete token;
    for (auto &buffer : recv_buffers)
        delete buffer;
    for (auto &memory : recv_buffer_memory)
        cudaFree(memory);
}
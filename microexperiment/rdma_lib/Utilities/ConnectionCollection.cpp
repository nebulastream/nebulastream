//
// Created by Toso, Lorenzo on 2019-01-11.
//

#include <vector>
#include "ConnectionCollection.h"
#include "MPIHelper.h"
#include "Debug.h"
#include <infinity/infinity.h>
#include <zconf.h>
#include <string.h>
VerbsConnection &ConnectionCollection::operator[](int idx) {
    return *connections[idx];
}

const VerbsConnection &ConnectionCollection::operator[](int idx) const {
    return *connections[idx];
}

/// Assumes that gathered_values is allocated correctly
void ConnectionCollection::gather_ex(const void* local_value, size_t* local_lengths, void* gathered_values, size_t target_rank) {
    TRACE("Entering gather_ex\n");
    size_t rank = MPIHelper::get_rank();
    size_t process_count = MPIHelper::get_process_count();

    std::vector<infinity::memory::Buffer*> buffers(process_count);
    if (rank == target_rank) {

        size_t displacement = 0;
        for (size_t i = 0; i < process_count; i++){
            char* destination = ((char*) gathered_values) + displacement;

            if (i == rank) {
                memcpy(destination, local_value, local_lengths[i]);
            } else {
                buffers[i] = connections[i]->register_buffer(destination, local_lengths[i]);
            }

            displacement += local_lengths[i];
        }
    }
    else {
        if(local_lengths[rank] > 0)
            buffers[target_rank] = connections[target_rank]->register_buffer(const_cast<void*>(local_value), local_lengths[rank]);
    }

    for (size_t i = 0; i < process_count; i++) {

        if (i == rank && rank != target_rank) {
            connections[target_rank]->barrier();
            TRACE("local_lengths[rank] > 0: %lu\n", local_lengths[rank]);
            if(local_lengths[rank] > 0){
                connections[target_rank]->send_blocking(buffers[target_rank]);
                delete buffers[target_rank];
            }
        }
        else {
            if (rank == target_rank && i != rank){
                connections[i]->barrier();
                TRACE("local_lengths[i] > 0: %lu\n", local_lengths[i]);
                if(local_lengths[i] > 0)
                    connections[i]->post_and_receive_blocking(buffers[i]);
                delete buffers[i];
            }
        }
    }
}


/// This function expects gathered_values to be allocated and large enough
void ConnectionCollection::gather_dif(const void* local_value, size_t local_length, void* gathered_values, size_t &gathered_length, size_t target_rank) {

    size_t process_count = MPIHelper::get_process_count();

    size_t gathered_lengths[process_count];
    gather(&local_length, &gathered_lengths, sizeof(size_t), target_rank);

    gather_ex(local_value, gathered_lengths, gathered_values, target_rank);
}

/// This method is used to gather different amounts of data from the individual nodes to one node
void* ConnectionCollection::gather_dif(const void* local_value, size_t local_length, size_t &gathered_length, size_t target_rank) {

    size_t process_count = MPIHelper::get_process_count();
    size_t gathered_lengths[process_count];

    all_gather(&local_length, sizeof(size_t), &gathered_lengths);

    gathered_length = 0;

    if(MPIHelper::get_rank() == target_rank)
    {
        for(size_t i = 0; i < process_count; i++)
            gathered_length += gathered_lengths[i];
    }

    TRACE("Allocating %lu\n", gathered_length);
    void * gathered_values = malloc(gathered_length);
    TRACE("Allocated %lu\n", gathered_length);
    gather_ex(local_value, gathered_lengths, gathered_values, target_rank);
    TRACE("gather_ex done\n");
    return gathered_values;
}

/// This function expects gathered_values to be allocated and large enough
void ConnectionCollection::gather(const void* local_value, void* gathered_values, size_t length_per_connection, size_t target_rank) {

    size_t process_count = MPIHelper::get_process_count();

    size_t all_local_lengths[process_count];
    for(size_t i = 0; i < process_count; i++)
        all_local_lengths[i] = length_per_connection;

    gather_ex(local_value, all_local_lengths, gathered_values, target_rank);
}
/// Allocates a necessary buffer
void* ConnectionCollection::gather(const void* local_value, size_t length_per_connection, size_t target_rank) {

    size_t process_count = MPIHelper::get_process_count();
    size_t result_length = process_count * length_per_connection;

    void * gathered_values = malloc(result_length);
    gather(local_value, gathered_values, length_per_connection, target_rank);
    return gathered_values;
}



void ConnectionCollection::add_connection_to(const ConnectionInfoProvider &infoProvider) {
    size_t target_rank = infoProvider.get_target_rank();
    if (target_rank >= connections.size())
        connections.resize(target_rank + 1);

    printf("Starting connection to rank %lu\n", target_rank);
    connections[target_rank] = std::make_shared<VerbsConnection>(&infoProvider);
}

ConnectionCollection::ConnectionCollection(const ConnectionInfoProvider &infoProvider) {
    add_connection_to(infoProvider);
}

void ConnectionCollection::bcast(void* local_value, size_t length, size_t sender) {
    size_t rank = MPIHelper::get_rank();
    size_t process_count = MPIHelper::get_process_count();

    if(process_count == 1)
        return;

    const size_t BUFFER_SIZE = 1024*512;

    if (rank == sender) {
        for (size_t i = 0; i < process_count; i++) {
            if (i == rank)
                continue;
            connections[i]->send_buffered(BUFFER_SIZE, local_value, length, 1);
        }
    } else {
        connections[sender]->recv_buffered(BUFFER_SIZE, local_value, length, 1);
    }
}

void ConnectionCollection::barrier_all() {

    TRACE("Entered barrier_all.\n");
    size_t rank = MPIHelper::get_rank();
    size_t process_count = MPIHelper::get_process_count();

    TRACE("Connections size: %lu\n", connections.size());
    if(process_count == 2){
        connections[rank == 0 ? 1 : 0]->barrier()   ;
        return;
    }


    if (rank == 0) {
        for(size_t i = 1; i < process_count; i++){
            connections[i]->barrier();
            TRACE2("Done with first barrier.\n");
        }
        for(size_t i = 1; i < process_count; i++){
            TRACE2("Waiting for second barrier with %lu.\n", i);
            connections[i]->send_notification();
            TRACE2("Done with second barrier.\n");
        }
    }
    else {
        connections[0]->barrier();
        TRACE2("Done with first barrier.\n");
        //usleep(1*1000);
        TRACE2("Waiting for second barrier with %d.\n", 0);
        connections[0]->wait_for_notification();
        TRACE2("Done with second barrier.\n");
    }

}

void*  ConnectionCollection::all_gather(const void* local_value, size_t length_per_connection) {

    size_t process_count = MPIHelper::get_process_count();
    size_t results_size = length_per_connection * process_count;

    void * result = malloc(results_size);

    gather(local_value, result, length_per_connection, 0);
    bcast(result, results_size, 0);

    return result;
}

void ConnectionCollection::all_gather(const void* local_value, size_t length_per_connection, void* gathered_values) {

    size_t process_count = MPIHelper::get_process_count();
    size_t results_size = length_per_connection * process_count;

    gather(local_value, gathered_values, length_per_connection, 0);
    bcast(gathered_values, results_size, 0);


}

size_t ConnectionCollection::sum(size_t local_value, size_t destination_rank) {
    size_t process_count = MPIHelper::get_process_count();

    size_t gathered_values[process_count];
    gather(&local_value, gathered_values, sizeof(size_t), destination_rank);

    size_t sum = 0;
    for(size_t i = 0; i < process_count; i++)
        sum += gathered_values[i];
    return sum;
}



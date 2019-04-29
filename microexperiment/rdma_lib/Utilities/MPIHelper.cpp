//
// Created by Toso, Lorenzo on 2018-11-14.
//
#include <mpi.h>
#include "MPIHelper.h"
#include <cstdio>

void MPIHelper::print_node_info(){
    size_t rank = get_rank();
    int name_length;
    char node_name[MAX_HOST_NAME_SIZE];
    get_host_name(node_name, name_length);

    printf("Rank %lu running on %s\n", rank, node_name);
}
size_t MPIHelper::get_rank(){
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return static_cast<size_t>(rank);
}
size_t MPIHelper::get_process_count(){
    int process_count;
    MPI_Comm_size(MPI_COMM_WORLD, &process_count);
    return static_cast<size_t>(process_count);
}

void MPIHelper::get_host_name(char * name, int & len){
    MPI_Get_processor_name(name, &len);
}
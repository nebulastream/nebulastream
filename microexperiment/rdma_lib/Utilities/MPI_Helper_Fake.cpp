//
// Created by Toso, Lorenzo on 2018-11-14.
//

#include "MPIHelper.h"
#include <cstdio>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <limits.h>
#include <cstddef>

size_t MPIHelper::rank = 0;
size_t MPIHelper::process_count = 0;

void MPIHelper::set_rank(size_t rank){
    MPIHelper::rank = rank;
}
void MPIHelper::set_process_count(size_t process_count){
    MPIHelper::process_count = process_count;
}

std::string MPIHelper::get_host_name() {
    char hostname[HOST_NAME_MAX];
    int len = HOST_NAME_MAX;
    get_host_name(hostname, len);
    return std::string(hostname);
}

void MPIHelper::print_node_info(){
    char hostname[HOST_NAME_MAX];
    int len = HOST_NAME_MAX;
    get_host_name(hostname, len);
    size_t rank = get_rank();

    printf("Rank %lu running on %s\n", rank, hostname);
}
size_t MPIHelper::get_rank(){
    return MPIHelper::rank;
}
size_t MPIHelper::get_process_count(){
    return MPIHelper::process_count;
}
void MPIHelper::get_host_name(char * name, int & max_len){
    size_t len = static_cast<size_t>(max_len);
    gethostname(name, len);
}
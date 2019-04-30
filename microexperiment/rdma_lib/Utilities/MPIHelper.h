//
// Created by Toso, Lorenzo on 2018-11-14.
//

#ifndef MAMPI_MPI_HELPER_H
#define MAMPI_MPI_HELPER_H

#include <cstddef>
#include <string>

class MPIHelper {
#define MAX_HOST_NAME_SIZE 255

private:
    static size_t rank;
    static size_t process_count;
public:
    static void set_rank(size_t rank);
    static void set_process_count(size_t process_count);

    static void print_node_info();
    static size_t get_rank();
    static size_t get_process_count();
    static void get_host_name(char * name, int & len);
    static std::string get_host_name();
};

#endif //MAMPI_MPI_HELPER_H

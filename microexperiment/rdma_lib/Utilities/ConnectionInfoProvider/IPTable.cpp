//
// Created by Toso, Lorenzo on 2018-12-04.
//

#include "Debug.h"
#include <mpi.h>
#include "IPTable.h"
#include "MPIHelper.h"
#include <utmpx.h>
#include <zconf.h>
#include <cstring>
#include "ConnectionUtilities.h"



IPTable IPTable::generate() {
    MPI_Barrier(MPI_COMM_WORLD);
    IPTable table;

    gather_device_names(table);
    gather_node_names(table);
    gather_numa_node_counts(table);
    gather_ips(table);
    gather_ib_ports(table);
    gather_device_indices(table);

    if(MPIHelper::get_rank() == 0){
        for(int i = 0; i < MPIHelper::get_process_count(); i++){
            TRACE("Rank %d on node %s has IP %s on device %s on NUMA node %d\n", i, table.node_name_map[i].c_str(), table.ip_map[i].c_str(), table.device_map[i].c_str(), table.numa_node_map[i]);
        }
    }

    return table;
}

void IPTable::gather_device_names(IPTable &table) {
    gather_strings(table, ConnectionUtilities::get_ib_device(), MAX_DEVICE_NAME_LENGTH, table.device_map);
}

void IPTable::gather_numa_node_counts(IPTable &table) {
    auto process_count = MPIHelper::get_process_count();
    auto numa_node = ConnectionUtilities::get_numa_node();
    int numa_nodes[process_count];

    MPI_Allgather(&numa_node, 1, MPI_INT,
                  numa_nodes, 1, MPI_INT,
                  MPI_COMM_WORLD);

    for(int i = 0; i < process_count; i++){
        table.numa_node_map[i] = numa_nodes[i];
    }
}
void IPTable::gather_ib_ports(IPTable &table) {
    auto port = ConnectionUtilities::get_ib_port();
    gather_ints(table, port, table.port_map);
}
void IPTable::gather_device_indices(IPTable & table) {
    auto index = ConnectionUtilities::get_ib_device_index();
    gather_ints(table, index, table.device_index_map);
}
void IPTable::gather_ints(IPTable &table, const int & local_value, std::map<int, int> & map) {
    auto process_count = MPIHelper::get_process_count();
    int all_values[process_count];

    MPI_Allgather(&local_value, 1, MPI_INT,
                  all_values, 1, MPI_INT,
                  MPI_COMM_WORLD);

    for(int i = 0; i < process_count; i++){
        map[i] = all_values[i];
    }
}

void IPTable::gather_node_names(IPTable &table) {
    gather_strings(table, ConnectionUtilities::get_node_name(), MPI_MAX_PROCESSOR_NAME, table.node_name_map);
}

void IPTable::gather_strings(IPTable &table, const std::string & local_value, size_t MAX_LENGTH, std::map<int, std::string> & map) {
    auto process_count = MPIHelper::get_process_count();
    char local_buffer[MAX_LENGTH];
    memset(local_buffer,0, MAX_LENGTH);

    char large_buffer[process_count * MAX_LENGTH];
    local_value.copy(local_buffer, local_value.size());


    MPI_Allgather(local_buffer, static_cast<int>(MAX_LENGTH), MPI_CHAR,
                  large_buffer, static_cast<int>(MAX_LENGTH), MPI_CHAR,
                  MPI_COMM_WORLD );

    for(int i = 0; i < process_count; i++){
        char* p = large_buffer + i*MAX_LENGTH;
        map[i] = std::string(p);
    }
}

void IPTable::gather_ips(IPTable & table) {
    gather_strings(table, ConnectionUtilities::get_ip(), MAX_IP_LENGTH, table.ip_map);
}

const std::string & IPTable::get_ip(size_t rank) const{
    return ip_map.at(rank);
}

const std::string & IPTable::get_device(size_t rank) const{
    return device_map.at(rank);
}

int IPTable::get_numa_node(size_t rank) const{
    return numa_node_map.at(rank);
}

const std::string & IPTable::get_node_name(size_t rank) const{
    return node_name_map.at(rank);
}

int IPTable::get_ib_port(size_t rank) const {
    return port_map.at(rank);
}

int IPTable::get_device_index(size_t rank) const {
    return device_index_map.at(rank);
}


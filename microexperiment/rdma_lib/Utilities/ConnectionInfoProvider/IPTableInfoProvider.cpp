//
// Created by Toso, Lorenzo on 2018-12-19.
//

#include "IPTableInfoProvider.h"
#include "MPIHelper.h"

IPTableInfoProvider::IPTableInfoProvider(const IPTable &ipTable, size_t target_rank)
:ipTable(ipTable)
,target_rank(target_rank){

}

size_t IPTableInfoProvider::get_target_rank() const {
    return target_rank;
}

u_int16_t IPTableInfoProvider::get_device_index() const {
    return static_cast<u_int16_t>(ipTable.get_device_index(MPIHelper::get_rank()));
}

u_int16_t IPTableInfoProvider::get_device_port() const {
    return static_cast<u_int16_t>(ipTable.get_ib_port(MPIHelper::get_rank()));
}

u_int16_t IPTableInfoProvider::get_connection_port() const {
    size_t rank = MPIHelper::get_rank();
    size_t process_count = MPIHelper::get_process_count();

    if (rank < target_rank) {
        return (uint16_t)(32768 + rank * process_count + target_rank);
    } else {
        return  (uint16_t)(32768 + target_rank * process_count + rank);
    }
}

std::string IPTableInfoProvider::get_target_ip() const {
    return ipTable.get_ip(target_rank);
}

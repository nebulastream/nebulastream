//
// Created by Toso, Lorenzo on 2019-01-11.
//

#include "RankfileInfoProvider.h"
#include "ConnectionUtilities.h"
#include "MPIHelper.h"
#include <fstream>
#include <regex>


RankfileInfoProvider::RankfileInfoProvider(const Rankfile & rankfile, size_t target_rank)
:target_rank(target_rank)
,rankfile(rankfile){
}

size_t RankfileInfoProvider::get_target_rank() const {
    return target_rank;
}

u_int16_t RankfileInfoProvider::get_device_index() const {
    return rankfile[MPIHelper::get_rank()].device_index;
}

u_int16_t RankfileInfoProvider::get_device_port() const {
    return rankfile[MPIHelper::get_rank()].device_port;
}

u_int16_t RankfileInfoProvider::get_connection_port() const {
    size_t my_rank = MPIHelper::get_rank();

    if (target_rank < my_rank) {
        return (uint16_t)(32768 + target_rank * MPIHelper::get_process_count() + my_rank);
    } else {
        return (uint16_t)(32768 + my_rank * MPIHelper::get_process_count() + target_rank);
    }

}

std::string RankfileInfoProvider::get_target_ip() const {
    return rankfile[target_rank].ip;
}

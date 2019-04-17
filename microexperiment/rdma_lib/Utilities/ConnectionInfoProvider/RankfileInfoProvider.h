//
// Created by Toso, Lorenzo on 2019-01-11.
//

#pragma once

#include "ConnectionInfoProvider.h"
#include <cstddef>
#include <vector>
#include "Rankfile.h"

class RankfileInfoProvider : public ConnectionInfoProvider {
private:
    const size_t target_rank;
    const Rankfile rankfile;
public:
    RankfileInfoProvider(const Rankfile & rankfile, size_t target_rank);

    size_t get_target_rank() const override;
    u_int16_t get_device_index() const override;
    u_int16_t get_device_port() const override;
    u_int16_t get_connection_port() const override;
    std::string get_target_ip() const override;
};




//
// Created by Toso, Lorenzo on 2018-12-19.
//

#pragma once

#include "ConnectionInfoProvider.h"
#include "IPTable.h"

class IPTableInfoProvider : public ConnectionInfoProvider {

private:
    const IPTable & ipTable;
    const size_t target_rank;
public:
    IPTableInfoProvider(const IPTable & ipTable, size_t target_rank);

    size_t get_target_rank() const override;
    u_int16_t get_device_index() const override;
    u_int16_t get_device_port() const override;
    u_int16_t get_connection_port() const override;
    std::string get_target_ip() const override;
};




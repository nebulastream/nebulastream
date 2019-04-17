//
// Created by Toso, Lorenzo on 2018-12-19.
//

#pragma once

#include <string>

class ConnectionInfoProvider {
public:
    virtual size_t get_target_rank() const = 0;
    virtual u_int16_t get_device_index() const = 0;
    virtual u_int16_t get_device_port() const = 0;
    virtual u_int16_t get_connection_port() const = 0;
    virtual std::string get_target_ip() const = 0;
};




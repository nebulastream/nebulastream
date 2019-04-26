//
// Created by Toso, Lorenzo on 2018-12-19.
//

#pragma once


#include "ConnectionInfoProvider.h"

class SimpleInfoProvider : public ConnectionInfoProvider {

private:
    size_t target_rank;
    u_int16_t device_index;
    u_int16_t device_port;
    u_int16_t connection_port;
    const std::string & ip;

public:
    SimpleInfoProvider(size_t target_rank, const std::string & device_name, u_int16_t device_Port, u_int16_t connection_port, const std::string & ip);

    size_t get_target_rank() const override;
    u_int16_t get_device_index() const override;
    u_int16_t get_device_port() const override;
    u_int16_t get_connection_port() const override;
    std::string get_target_ip() const override;

};




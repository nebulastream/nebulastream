//
// Created by Toso, Lorenzo on 2018-12-19.
//

#include "SimpleInfoProvider.h"

SimpleInfoProvider::SimpleInfoProvider(size_t target_rank, u_int16_t device_index, u_int16_t device_Port, u_int16_t connection_port, const std::string & ip)
:target_rank(target_rank)
,device_index(device_index)
,device_port(device_Port)
,connection_port(connection_port)
,ip(ip)
{

}


size_t SimpleInfoProvider::get_target_rank() const {
    return target_rank;
}
u_int16_t SimpleInfoProvider::get_device_index() const {
    return device_index;
}
u_int16_t SimpleInfoProvider::get_device_port() const {
    return device_port;
}
u_int16_t SimpleInfoProvider::get_connection_port() const {
    return connection_port;
}
std::string SimpleInfoProvider::get_target_ip() const {
    return ip;
}
//
// Created by Toso, Lorenzo on 2018-12-19.
//

#include "SimpleInfoProvider.h"
#include <infinity/core/Context.h>
#include <vector>
typedef struct {
    std::string name;
    size_t index = 0;
} IbDevice;


std::vector<IbDevice> get_available_devices()
{
    int32_t numberOfInstalledDevices = 0;
    ibv_device **ibvDeviceList = ibv_get_device_list(&numberOfInstalledDevices);

    std::vector<IbDevice> devices(numberOfInstalledDevices);

    for(int i = 0; i < numberOfInstalledDevices; i++)
    {
        devices[i].name = ibvDeviceList[i]->name;
        devices[i].index = i;
    }
    return devices;
}

SimpleInfoProvider::SimpleInfoProvider(size_t target_rank, const std::string & device_name, u_int16_t device_Port, u_int16_t connection_port, const std::string & ip)
:target_rank(target_rank)
,device_index(device_index)
,device_port(device_Port)
,connection_port(connection_port)
,ip(ip)
{
    auto devices = get_available_devices();
        for(auto & dev : devices)
            if(dev.name == device_name)
                device_index = dev.index;
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

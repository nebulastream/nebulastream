//
// Created by Toso, Lorenzo on 2019-01-18.
//

#pragma once

#include <cstddef>
#include <string>
#include <vector>

struct DeviceInfo{
    size_t rank;
    std::string ip;
    std::string host_name;
    uint16_t device_index;
    uint16_t device_port;
    uint16_t numa_node;
};

class Rankfile {
private:
    std::vector<DeviceInfo> device_infos;
private:
    explicit Rankfile(const std::vector<DeviceInfo> &device_infos);

public:
    static Rankfile load(const std::string & path);
    const DeviceInfo &operator[](int index) const;
    DeviceInfo &operator[](int index);

    size_t process_count();
};




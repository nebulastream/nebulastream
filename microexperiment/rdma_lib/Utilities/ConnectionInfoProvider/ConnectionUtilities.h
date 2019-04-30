//
// Created by Toso, Lorenzo on 2018-12-05.
//

#pragma once

#include <string>

class ConnectionUtilities {
private:
public:
    ConnectionUtilities() = delete;

    static std::string get_node_name();
    static int get_numa_node();
    static std::string get_ip(const std::string &node_name, int numa_node, int cpu_index);
    static std::string get_ip();
    static std::string get_ib_device(int numa_node, int cpu_index);
    static std::string get_ib_device();

    static bool is_bottom_half(int cpu_index);

    static int get_ib_port();

    static int get_ib_device_index();
    static int get_ib_device_index(int numa_node, int cpu_index);
};




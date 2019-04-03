//
// Created by Toso, Lorenzo on 2018-12-04.
//

#ifndef MAMPI_IPTABLE_H
#define MAMPI_IPTABLE_H

#include <string>
#include <map>

#define MAX_DEVICE_NAME_LENGTH 255
//todo might be changed for IPv6
#define MAX_IP_LENGTH 200

class IPTable {
private:

    std::map<int, std::string> ip_map;
    std::map<int, std::string> device_map;
    std::map<int, int> numa_node_map;
    std::map<int, std::string> node_name_map;
    std::map<int, int> port_map;
    std::map<int, int> device_index_map;

    IPTable() = default;
public:
    int get_ib_port(size_t rank) const;
    const std::string & get_ip(size_t rank) const;
    const std::string & get_device(size_t rank) const;
    int get_device_index(size_t rank) const;
    int get_numa_node(size_t rank) const;
    const std::string & get_node_name(size_t rank) const;

    static IPTable generate();


private:
    static void gather_strings(IPTable &table, const std::string & local_value, size_t MAX_LENGTH, std::map<int, std::string> & map);
    static void gather_ints(IPTable &table, const int & local_value, std::map<int, int> & map);

    static void gather_node_names(IPTable &table);
    static void gather_numa_node_counts(IPTable &table);
    static void gather_device_names( IPTable &table);
    static void gather_ips(IPTable & table);
    static void gather_ib_ports(IPTable &table);
    static void gather_device_indices(IPTable & table);
};
#endif //MAMPI_IPTABLE_H

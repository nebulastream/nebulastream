//
// Created by Toso, Lorenzo on 2018-12-05.
//

#include "ConnectionUtilities.h"
#include <numa.h>
#include "MPIHelper.h"

std::string ConnectionUtilities::get_node_name() {
    int name_length = MAX_HOST_NAME_SIZE;
    char node_name[MAX_HOST_NAME_SIZE];
    MPIHelper::get_host_name(node_name, name_length);
    return std::string(node_name);
}

int ConnectionUtilities::get_numa_node() {
    int cpu = sched_getcpu();
    int numa_node = numa_node_of_cpu(cpu);
    return numa_node;
}

std::string ConnectionUtilities::get_ip(const std::string &node_name, int numa_node, int cpu_index) {
    auto is_bottom = is_bottom_half(cpu_index);


    if(node_name == "cloud-40"){
        switch(numa_node){
            case 0:
                if(is_bottom)
                    return "192.168.5.10";
                else
                    return "192.168.5.11";
            case 1:
                if(is_bottom)
                    return "192.168.5.12";
                else
                    return "192.168.5.13";
            default:
                return "error";
        }
    }
    if(node_name == "cloud-41"){
        switch(numa_node){
            case 0:
                if(is_bottom)
                    return "192.168.5.20";
                else
                    return "192.168.5.21";
            case 1:
                if(is_bottom)
                    return "192.168.5.22";
                else
                    return "192.168.5.23";
            default:
                return "error";
        }
    }
    if(node_name == "cloud-42"){
        switch(numa_node){
            case 0:
                if(is_bottom)
                    return "192.168.5.30";
                else
                    return "192.168.5.31";
            case 1:
                if(is_bottom)
                    return "192.168.5.32";
                else
                    return "192.168.5.33";
            default:
                return "error";
        }
    }
    if(node_name == "cloud-43"){
        switch(numa_node){
            case 0:
                if(is_bottom)
                    return "192.168.5.40";
                else
                    return "192.168.5.41";
            case 1:
                if(is_bottom)
                    return "192.168.5.42";
                else
                    return "192.168.5.43";
            default:
                return "error";
        }
    }
    return "error";
}

std::string ConnectionUtilities::get_ip() {
    return get_ip(get_node_name(), get_numa_node(), sched_getcpu());
}

std::string ConnectionUtilities::get_ib_device(int numa_node, int cpu_index) {
    auto bottom = is_bottom_half(cpu_index);

    switch(numa_node){
        case 0:
            if(bottom)
                return "ib0";
            else
                return "ib1";
        case 1:
            if(bottom)
                return "ib2";
            else
                return "ib3";
        default:
            return "error";
    }
}

bool ConnectionUtilities::is_bottom_half(int cpu_index) {
    int numa_nodes = numa_num_configured_nodes();
    int max_cpus = numa_num_configured_cpus();
    int max_cpus_per_node = max_cpus/numa_nodes;    //todo this has the assumptions that all nodes have the same amount of cpus
    return (cpu_index - get_numa_node()*max_cpus_per_node) < max_cpus_per_node/2;
}

std::string ConnectionUtilities::get_ib_device() {
    return get_ib_device(get_numa_node(), sched_getcpu());
}

int ConnectionUtilities::get_ib_port() {
    /*
    return is_bottom_half(sched_getcpu()) ? 1 : 2;
     */
    return 1;
}

int ConnectionUtilities::get_ib_device_index() {
    return get_ib_device_index(get_numa_node(), sched_getcpu());
}

int ConnectionUtilities::get_ib_device_index(int numa_node, int cpu_index) {

    auto bottom = is_bottom_half(cpu_index);

    switch(numa_node){
        case 0:
            if(bottom)
                return 3;
            else
                return 2;
        case 1:
            if(bottom)
                return 1;
            else
                return 0;
        default:
            return 3;
    }
}

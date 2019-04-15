//
// Created by Toso, Lorenzo on 2019-01-18.
//

#ifndef MAMPI_NUMAUTILITIES_H
#define MAMPI_NUMAUTILITIES_H
#include <numa.h>
#include "Debug.h"

static void pin_to_numa(size_t numa_node) {
    /*
    QUICK_PRINT("Pinning to numa node %lu\n", numa_node);
    int max_cpus = numa_num_configured_cpus();

    cpu_set_t mask;
    CPU_ZERO(&mask);

    for(int i = 0; i < max_cpus; i++){
        size_t node_of_cpu = static_cast<size_t>(numa_node_of_cpu(i));
        if(node_of_cpu == numa_node){
            CPU_SET(i, &mask);
        }
    }
    sched_setaffinity(0, sizeof(cpu_set_t), &mask);*/
    numa_run_on_node(static_cast<int>(numa_node));
}

#endif //MAMPI_NUMAUTILITIES_H

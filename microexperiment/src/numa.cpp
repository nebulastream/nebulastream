#include <numa.h>
#include <iostream>
#include <cstdio>
#include <iostream>
using namespace std;

int main() {
    size_t node = 0;
    numa_run_on_node(static_cast<int>(node));
    numa_set_preferred(node);

    size_t* bla = new size_t[999];

    int status[1];
    int ret_code;
    status[0]=-1;
    printf("Memory at %p is at %d node (id %d) (node %d)\n",
            bla, status[0], 0, numa_node_of_cpu(sched_getcpu()));

//    /s/##################
    node = 1;
    numa_run_on_node(static_cast<int>(node));
    numa_set_preferred(node);

    size_t* bla2 = new size_t[999];

    status[1];
    ret_code;
    status[0]=-1;
    printf("Memory at %p is at %d node (id %d) (node %d)\n",
            bla2, status[0], 0, numa_node_of_cpu(sched_getcpu()));


    return 0;
}

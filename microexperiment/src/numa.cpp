#include <numa.h>
#include <iostream>
#include <cstdio>
#include <iostream>
#include <numaif.h>
#include <unistd.h>
using namespace std;

int main() {
    size_t node = 0;
//    numa_run_on_node(static_cast<int>(node));
//    numa_set_preferred(node);
//    size_t nr_nodes = numa_max_node()+1;
//    struct bitmask * asd = numa_bitmask_alloc(nr_nodes);
//    numa_bitmask_setbit(asd, node);
//    numa_set_membind(asd);
//    size_t* bla = new size_t[999999]{0};;
//    cout << "test=" << bla[0] << endl;
    int status[1];
    int ret_code;
    status[0]=-1;
    void * ptr_to_check = numa_alloc_onnode(999999, node);
    ((size_t*)ptr_to_check)[0] = 123;

    ret_code=move_pages(0 /*self memory */, 1, &ptr_to_check,
       NULL, status, 0);
    printf("Memory at %p is at %d node (id %d) (node %d)\n",
            ptr_to_check, status[0], 0, numa_node_of_cpu(sched_getcpu()));
    int numa_node = -1;

    get_mempolicy(&numa_node, NULL, 0, (void*)ptr_to_check, MPOL_F_NODE | MPOL_F_ADDR);
    cout << "node= " << numa_node << ",";
//    /s/##################
    size_t node2 = 1;
//    numa_run_on_node(static_cast<int>(node2));
//    numa_set_preferred(node2);
//    nr_nodes = numa_max_node()+1;
//    struct bitmask * asd2 = numa_bitmask_alloc(nr_nodes);
//    numa_bitmask_setbit(asd2, node2);
//    numa_set_membind(asd2);
//    size_t* bla2 = new size_t[999999]{0};
    void * ptr_to_check2 = numa_alloc_onnode(999999, node2);
    ((size_t*)ptr_to_check2)[0] = 123;
//    cout << "test=" << bla2[0] << endl;

    status[1];
    ret_code;
    status[0]=-1;
    ret_code=move_pages(0 /*self memory */, 1, &ptr_to_check2,
       NULL, status, 0);

    printf("Memory at %p is at %d node (id %d) (node %d)\n",
            ptr_to_check2, status[0], 0, numa_node_of_cpu(sched_getcpu()));
    get_mempolicy(&numa_node, NULL, 0, (void*)ptr_to_check2, MPOL_F_NODE | MPOL_F_ADDR);
        cout << "node= " << numa_node << ",";

    return 0;
}

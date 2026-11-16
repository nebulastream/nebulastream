/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

/* Binds a shared memory file to a NUMA node: pool_bind <file> <node>. The policy belongs to the shared memory, so it holds for every
   process that maps it. The UDF processes of the benchmark run it on the buffer pool of the worker (UDF_BENCH_POOL_NODE), as a child
   process: ctypes in PyPy slows every compiled loop of the process that imports it (E48). */

#define _GNU_SOURCE
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

int main(int argc, char** argv)
{
    if (argc != 3)
    {
        fprintf(stderr, "usage: %s <file> <node>\n", argv[0]);
        return 2;
    }
    const int fd = open(argv[1], O_RDWR);
    struct stat status;
    if (fd < 0 || fstat(fd, &status) != 0)
    {
        perror(argv[1]);
        return 1;
    }
    void* memory = mmap(0, (size_t)status.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    const int node = atoi(argv[2]);
    unsigned long mask[2] = {0, 0};
    mask[node / 64] = 1UL << (node % 64);
    /* MPOL_BIND is 2 */
    if (memory == MAP_FAILED || syscall(SYS_mbind, memory, (size_t)status.st_size, 2, mask, 129, 0) != 0)
    {
        perror("mmap or mbind");
        return 1;
    }
    return 0;
}

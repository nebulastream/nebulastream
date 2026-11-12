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

/* Where should the pages of a buffer be that one process writes and a process on another NUMA node reads?

       gcc -O2 -o numa_handoff numa_handoff.c
       ./numa_handoff <mode> <writer cpu> <reader cpu> <writer node> <reader node> <buffer bytes> <pool bytes> <depth> <buffers>

   A writer process fills buffers of a shared pool with 8-byte stores, one after the other, and hands each to a reader process,
   which sums its 8-byte words. The pool is used in a circle, as a buffer pool that is larger than the caches. At most <depth>
   buffers wait for the reader. Both processes spin while they wait, so there are no system calls in the hand-off.

   Modes, which say where the pages of the pool are:
       writer    on the node of the writer: the writer writes local memory, the reader reads remote memory
       reader    on the node of the reader: the writer writes remote memory, the reader reads local memory
       move      on the node of the writer; the writer moves the pages of a buffer to the node of the reader after it filled
                 the buffer (move_pages), and the pages stay there
       moveback  as move, and the writer moves the pages back to its node before it fills the buffer again

   The program reports how many pages the kernel migrated during the run, for all processes of the machine (/proc/vmstat).

   Prints one JSON line: nanoseconds per buffer over all (wall), in the writer for the fill and for the moves, and in the reader
   for the sum. */

#define _GNU_SOURCE
#include <errno.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MPOL_BIND 2
#define MPOL_MF_MOVE 2
#define MPOL_MF_MOVE_ALL 4
#define PAGE 4096

struct Control
{
    _Alignas(64) atomic_ulong tail; /* buffers the writer filled */
    _Alignas(64) atomic_ulong head; /* buffers the reader summed */
    _Alignas(64) atomic_ulong started; /* both processes are pinned and ready */
    _Alignas(64) double fillSeconds, moveSeconds, sumSeconds, wallSeconds;
    unsigned long checksum;
};

static double now(void)
{
    struct timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    return (double)time.tv_sec + 1e-9 * (double)time.tv_nsec;
}

static void pin(int cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof(set), &set) != 0)
    {
        perror("sched_setaffinity");
        exit(1);
    }
}

static void bindToNode(void* memory, size_t bytes, int node)
{
    unsigned long mask[2] = {0, 0};
    mask[node / 64] = 1UL << (node % 64);
    if (syscall(SYS_mbind, memory, bytes, MPOL_BIND, mask, 129, 0) != 0)
    {
        perror("mbind");
        exit(1);
    }
}

/* Moves the pages of a buffer to a node and returns the seconds of the call */
static double movePages(char* buffer, size_t pages, int node, int flags)
{
    void* addresses[64];
    int nodes[64];
    int status[64];
    for (size_t page = 0; page < pages; ++page)
    {
        addresses[page] = buffer + page * PAGE;
        nodes[page] = node;
    }
    const double started = now();
    syscall(SYS_move_pages, 0, pages, addresses, nodes, status, flags);
    return now() - started;
}

/* A counter of /proc/vmstat. The kernel counts the pages it migrated for all processes, so the machine must be idle otherwise. */
static unsigned long vmstat(const char* name)
{
    FILE* file = fopen("/proc/vmstat", "r");
    char key[128];
    unsigned long value = 0, found = 0;
    while (file && fscanf(file, "%127s %lu", key, &value) == 2)
    {
        if (strcmp(key, name) == 0)
        {
            found = value;
            break;
        }
    }
    if (file)
    {
        fclose(file);
    }
    return found;
}

int main(int argc, char** argv)
{
    if (argc != 10)
    {
        fprintf(stderr, "usage: %s <writer|reader|move|moveback> <writer cpu> <reader cpu> <writer node> <reader node> <buffer bytes> <pool bytes> <depth> <buffers>\n", argv[0]);
        return 2;
    }
    const char* mode = argv[1];
    const int writerCpu = atoi(argv[2]), readerCpu = atoi(argv[3]), writerNode = atoi(argv[4]), readerNode = atoi(argv[5]);
    const size_t bufferBytes = strtoull(argv[6], 0, 10), poolBytes = strtoull(argv[7], 0, 10);
    const unsigned long depth = strtoull(argv[8], 0, 10), buffers = strtoull(argv[9], 0, 10);
    const size_t poolBuffers = poolBytes / bufferBytes, words = bufferBytes / 8, pages = bufferBytes / PAGE;
    const int move = strcmp(mode, "move") == 0 || strcmp(mode, "moveback") == 0;
    const int back = strcmp(mode, "moveback") == 0;
    if (bufferBytes % PAGE != 0 || pages > 64 || poolBuffers <= depth + 1)
    {
        fprintf(stderr, "the buffer must be whole pages, at most 64, and the pool must hold more than depth + 1 buffers\n");
        return 2;
    }

    struct Control* control = mmap(0, sizeof(struct Control), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    const int fd = memfd_create("numa-handoff", 0);
    if (control == MAP_FAILED || fd < 0 || ftruncate(fd, (off_t)poolBytes) != 0)
    {
        perror("shared memory");
        return 1;
    }
    char* pool = mmap(0, poolBytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (pool == MAP_FAILED)
    {
        perror("mmap");
        return 1;
    }
    /* The pool is shared memory, so the policy holds for both processes; all pages exist before the measurement.
       A moved page leaves its node, so the pool of the move modes has no policy and automatic NUMA balancing may also move it;
       the runs are short against the scan period of the balancing. */
    const int poolNode = strcmp(mode, "reader") == 0 ? readerNode : writerNode;
    bindToNode(pool, poolBytes, poolNode);
    memset(pool, 1, poolBytes);
    if (move)
    {
        syscall(SYS_mbind, pool, poolBytes, 0 /* MPOL_DEFAULT */, 0, 0, 0);
    }
    const int moveFlags = geteuid() == 0 ? MPOL_MF_MOVE_ALL : MPOL_MF_MOVE;

    const pid_t reader = fork();
    if (reader == 0)
    {
        pin(readerCpu);
        /* Touch every page, so that the reader has all pages mapped, as a UDF process that has seen the whole pool */
        unsigned long sum = 0;
        for (size_t offset = 0; offset < poolBytes; offset += PAGE)
        {
            sum += (unsigned long)pool[offset];
        }
        atomic_fetch_add(&control->started, 1);
        while (atomic_load(&control->started) < 2) { }
        double seconds = 0;
        for (unsigned long sequence = 0; sequence < buffers; ++sequence)
        {
            while (atomic_load_explicit(&control->tail, memory_order_acquire) == sequence) { }
            const uint64_t* data = (const uint64_t*)(pool + (sequence % poolBuffers) * bufferBytes);
            const double started = now();
            for (size_t word = 0; word < words; ++word)
            {
                sum += data[word];
            }
            seconds += now() - started;
            atomic_store_explicit(&control->head, sequence + 1, memory_order_release);
        }
        control->sumSeconds = seconds;
        control->checksum = sum;
        _exit(0);
    }

    pin(writerCpu);
    atomic_fetch_add(&control->started, 1);
    while (atomic_load(&control->started) < 2) { }
    double fillSeconds = 0, moveSeconds = 0;
    const unsigned long migratedBefore = vmstat("pgmigrate_success"), failedBefore = vmstat("pgmigrate_fail");
    const double begin = now();
    for (unsigned long sequence = 0; sequence < buffers; ++sequence)
    {
        while (sequence - atomic_load_explicit(&control->head, memory_order_acquire) >= depth) { }
        char* buffer = pool + (sequence % poolBuffers) * bufferBytes;
        if (back && sequence >= poolBuffers)
        {
            moveSeconds += movePages(buffer, pages, writerNode, moveFlags);
        }
        const double started = now();
        uint64_t* data = (uint64_t*)buffer;
        for (size_t word = 0; word < words; ++word)
        {
            data[word] = sequence + word;
        }
        const double filled = now();
        fillSeconds += filled - started;
        if (move)
        {
            moveSeconds += movePages(buffer, pages, readerNode, moveFlags);
        }
        atomic_store_explicit(&control->tail, sequence + 1, memory_order_release);
    }
    while (atomic_load(&control->head) < buffers) { }
    control->wallSeconds = now() - begin;
    control->fillSeconds = fillSeconds;
    control->moveSeconds = moveSeconds;
    waitpid(reader, 0, 0);
    const double per = 1e9 / (double)buffers;
    printf("{\"mode\": \"%s\", \"writer_cpu\": %d, \"reader_cpu\": %d, \"writer_node\": %d, \"reader_node\": %d, \"buffer_bytes\": %zu, "
           "\"pool_bytes\": %zu, \"depth\": %lu, \"buffers\": %lu, \"root\": %s, \"wall_ns_per_buffer\": %.0f, \"fill_ns_per_buffer\": %.0f, "
           "\"move_ns_per_buffer\": %.0f, \"sum_ns_per_buffer\": %.0f, \"gb_per_s\": %.2f, \"pages_migrated\": %lu, \"migrations_failed\": %lu}\n",
           mode, writerCpu, readerCpu, writerNode, readerNode, bufferBytes, poolBytes, depth, buffers, geteuid() == 0 ? "true" : "false",
           control->wallSeconds * per, control->fillSeconds * per, control->moveSeconds * per, control->sumSeconds * per,
           (double)buffers * (double)bufferBytes / control->wallSeconds / 1e9, vmstat("pgmigrate_success") - migratedBefore, vmstat("pgmigrate_fail") - failedBefore);
    return 0;
}

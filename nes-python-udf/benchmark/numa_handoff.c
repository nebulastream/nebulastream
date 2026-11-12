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

/* Where should the pages of a buffer be that one process writes and a process on another NUMA node reads and answers?

       gcc -O2 -o numa_handoff numa_handoff.c
       ./numa_handoff writer_cpu=2 reader_cpu=64 request_node=0 result_node=4 buffer=16384 stride=16 answer=separate ...

   The model of a worker thread and a UDF process. A writer process (the worker) fills request buffers of a shared pool with
   8-byte stores and hands each to a reader process (the UDF process). The reader reads one 8-byte word every `stride` bytes of
   the buffer, and writes one 8-byte answer for each word it read. The writer reads the answers of a buffer after the reader
   is done with it. The pools are used in a circle, as a buffer pool that is larger than the caches. At most `depth` buffers
   wait for the reader. Both processes spin while they wait, so there are no system calls in the hand-off.

   Arguments, all name=value:
       writer_cpu, reader_cpu       the CPUs of the two processes
       request_node, result_node    the NUMA nodes of the pages of the request pool and of the result pool (mbind, MPOL_BIND, so
                                    automatic NUMA balancing leaves them alone)
       buffer    bytes of a request buffer, whole pages, at most 64 pages (default 16384)
       stride    the reader reads the word at every `stride` bytes: 8 is all of the buffer, 64 one word per cache line, 4096 one
                 word per page; 0 reads nothing (default 8)
       answer    none: the reader writes nothing (default)
                 inplace: the reader writes each answer into the request buffer, 8 bytes after the word it read (stride >= 16)
                 separate: the reader writes the answers one after the other into a buffer of the result pool
       pool      bytes of the request pool (default 1 GiB)
       depth     buffers that may wait for the reader (default 16)
       buffers   buffers to send (default 200000)
       move      none (default); reader: the writer moves the pages of a request buffer to result_node after it filled the buffer
                 (move_pages), and the pages stay there; both: it also moves them back to request_node before it fills the
                 buffer again. The request pool has no policy in these modes.

   Prints one JSON line: nanoseconds per buffer of wall time, and of the steps in the writer (fill, collect the answers, move)
   and in the reader (read, write the answers), and the pages the kernel migrated during the run for all processes of the
   machine (/proc/vmstat). The two processes run at the same time, so the wall time is not the sum of the steps. */

#define _GNU_SOURCE
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
#define PAGE 4096UL

struct Control
{
    _Alignas(64) atomic_ulong tail; /* buffers the writer filled */
    _Alignas(64) atomic_ulong head; /* buffers the reader handled */
    _Alignas(64) atomic_ulong started; /* both processes are pinned and ready */
    _Alignas(64) double readSeconds, writeSeconds;
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

/* A shared pool whose pages all exist and are on a node */
static char* createPool(size_t bytes, int node, int keepPolicy)
{
    const int fd = memfd_create("numa-handoff", 0);
    if (fd < 0 || ftruncate(fd, (off_t)bytes) != 0)
    {
        perror("memfd");
        exit(1);
    }
    char* pool = mmap(0, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    unsigned long mask[2] = {0, 0};
    mask[node / 64] = 1UL << (node % 64);
    if (pool == MAP_FAILED || syscall(SYS_mbind, pool, bytes, MPOL_BIND, mask, 129, 0) != 0)
    {
        perror("mmap or mbind");
        exit(1);
    }
    memset(pool, 1, bytes);
    if (!keepPolicy)
    {
        syscall(SYS_mbind, pool, bytes, 0 /* MPOL_DEFAULT */, 0, 0, 0);
    }
    return pool;
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

static const char* argument(int argc, char** argv, const char* name, const char* fallback)
{
    const size_t length = strlen(name);
    for (int index = 1; index < argc; ++index)
    {
        if (strncmp(argv[index], name, length) == 0 && argv[index][length] == '=')
        {
            return argv[index] + length + 1;
        }
    }
    return fallback;
}

int main(int argc, char** argv)
{
    const int writerCpu = atoi(argument(argc, argv, "writer_cpu", "2")), readerCpu = atoi(argument(argc, argv, "reader_cpu", "64"));
    const int requestNode = atoi(argument(argc, argv, "request_node", "0")), resultNode = atoi(argument(argc, argv, "result_node", "0"));
    const size_t bufferBytes = strtoull(argument(argc, argv, "buffer", "16384"), 0, 10);
    const size_t stride = strtoull(argument(argc, argv, "stride", "8"), 0, 10);
    const size_t poolBytes = strtoull(argument(argc, argv, "pool", "1073741824"), 0, 10);
    const unsigned long depth = strtoull(argument(argc, argv, "depth", "16"), 0, 10);
    const unsigned long buffers = strtoull(argument(argc, argv, "buffers", "200000"), 0, 10);
    const char* answer = argument(argc, argv, "answer", "none");
    const char* moveMode = argument(argc, argv, "move", "none");
    const int inplace = strcmp(answer, "inplace") == 0, separate = strcmp(answer, "separate") == 0;
    const int move = strcmp(moveMode, "none") != 0, back = strcmp(moveMode, "both") == 0;
    const size_t poolBuffers = poolBytes / bufferBytes, pages = bufferBytes / PAGE;
    const size_t reads = stride ? bufferBytes / stride : 0;
    /* A result buffer holds one answer per word read, in whole pages */
    const size_t resultBytes = separate ? (reads * 8 + PAGE - 1) / PAGE * PAGE : 0;
    if (bufferBytes % PAGE != 0 || pages > 64 || poolBuffers <= depth + 1 || (stride && stride % 8 != 0) || (inplace && stride < 16)
        || ((inplace || separate) && reads == 0))
    {
        fprintf(stderr, "bad arguments, see the comment at the start of numa_handoff.c\n");
        return 2;
    }

    struct Control* control = mmap(0, sizeof(struct Control), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    char* requests = createPool(poolBytes, requestNode, !move);
    char* results = separate ? createPool(poolBuffers * resultBytes, resultNode, 1) : 0;
    const int moveFlags = geteuid() == 0 ? MPOL_MF_MOVE_ALL : MPOL_MF_MOVE;

    const pid_t reader = fork();
    if (reader == 0)
    {
        pin(readerCpu);
        /* The reader has all pages mapped, as a UDF process that has seen the whole pool: for reading, and for writing where it
           writes answers */
        unsigned long sum = 0;
        for (size_t offset = 0; offset < poolBytes; offset += PAGE)
        {
            if (inplace)
            {
                volatile char* byte = requests + offset;
                *byte = *byte;
            }
            sum += (unsigned long)requests[offset];
        }
        for (size_t offset = 0; separate && offset < poolBuffers * resultBytes; offset += PAGE)
        {
            volatile char* byte = results + offset;
            *byte = *byte;
        }
        atomic_fetch_add(&control->started, 1);
        while (atomic_load(&control->started) < 2) { }
        double readSeconds = 0, writeSeconds = 0;
        static uint64_t values[32768];
        for (unsigned long sequence = 0; sequence < buffers; ++sequence)
        {
            while (atomic_load_explicit(&control->tail, memory_order_acquire) == sequence) { }
            char* buffer = requests + (sequence % poolBuffers) * bufferBytes;
            const double started = now();
            if (inplace)
            {
                /* Read a field and write the slot next to it, row by row, as a function over the rows of a buffer */
                for (size_t offset = 0; offset < bufferBytes; offset += stride)
                {
                    const uint64_t value = *(const uint64_t*)(buffer + offset);
                    sum += value;
                    *(uint64_t*)(buffer + offset + 8) = value + 1;
                }
                readSeconds += now() - started;
            }
            else
            {
                size_t count = 0;
                for (size_t offset = 0; stride && offset < bufferBytes; offset += stride)
                {
                    const uint64_t value = *(const uint64_t*)(buffer + offset);
                    sum += value;
                    values[count++] = value;
                }
                const double read = now();
                readSeconds += read - started;
                if (separate)
                {
                    uint64_t* result = (uint64_t*)(results + (sequence % poolBuffers) * resultBytes);
                    for (size_t index = 0; index < reads; ++index)
                    {
                        result[index] = values[index] + 1;
                    }
                    writeSeconds += now() - read;
                }
            }
            atomic_store_explicit(&control->head, sequence + 1, memory_order_release);
        }
        control->readSeconds = readSeconds;
        control->writeSeconds = writeSeconds;
        control->checksum = sum;
        _exit(0);
    }

    pin(writerCpu);
    atomic_fetch_add(&control->started, 1);
    while (atomic_load(&control->started) < 2) { }
    double fillSeconds = 0, moveSeconds = 0, collectSeconds = 0;
    unsigned long collected = 0, answers = 0;
    const unsigned long migratedBefore = vmstat("pgmigrate_success"), failedBefore = vmstat("pgmigrate_fail");
    const double begin = now();
    for (unsigned long sequence = 0; sequence <= buffers; ++sequence)
    {
        /* Wait for room, or at the end for the last answers, and read the answers of the buffers the reader is done with */
        for (;;)
        {
            const unsigned long head = atomic_load_explicit(&control->head, memory_order_acquire);
            if ((inplace || separate) && collected < head)
            {
                const double started = now();
                for (; collected < head; ++collected)
                {
                    const size_t slot = collected % poolBuffers;
                    if (separate)
                    {
                        const uint64_t* result = (const uint64_t*)(results + slot * resultBytes);
                        for (size_t index = 0; index < reads; ++index)
                        {
                            answers += result[index];
                        }
                    }
                    else
                    {
                        const char* buffer = requests + slot * bufferBytes;
                        for (size_t offset = 0; offset < bufferBytes; offset += stride)
                        {
                            answers += *(const uint64_t*)(buffer + offset + 8);
                        }
                    }
                }
                collectSeconds += now() - started;
            }
            if (sequence == buffers ? head == buffers : sequence - head < depth)
            {
                break;
            }
        }
        if (sequence == buffers)
        {
            break;
        }
        char* buffer = requests + (sequence % poolBuffers) * bufferBytes;
        if (back && sequence >= poolBuffers)
        {
            moveSeconds += movePages(buffer, pages, requestNode, moveFlags);
        }
        const double started = now();
        uint64_t* data = (uint64_t*)buffer;
        for (size_t word = 0; word < bufferBytes / 8; ++word)
        {
            data[word] = sequence + word;
        }
        fillSeconds += now() - started;
        if (move)
        {
            moveSeconds += movePages(buffer, pages, resultNode, moveFlags);
        }
        atomic_store_explicit(&control->tail, sequence + 1, memory_order_release);
    }
    const double wallSeconds = now() - begin;
    waitpid(reader, 0, 0);
    const double per = 1e9 / (double)buffers;
    printf("{\"writer_cpu\": %d, \"reader_cpu\": %d, \"request_node\": %d, \"result_node\": %d, \"buffer_bytes\": %zu, \"stride\": %zu, "
           "\"answer\": \"%s\", \"result_bytes\": %zu, \"pool_bytes\": %zu, \"depth\": %lu, \"buffers\": %lu, \"move\": \"%s\", \"root\": %s, "
           "\"wall_ns_per_buffer\": %.0f, \"fill_ns_per_buffer\": %.0f, \"collect_ns_per_buffer\": %.0f, \"move_ns_per_buffer\": %.0f, "
           "\"read_ns_per_buffer\": %.0f, \"write_ns_per_buffer\": %.0f, \"pages_migrated\": %lu, \"migrations_failed\": %lu, \"checksum\": %lu}\n",
           writerCpu, readerCpu, requestNode, resultNode, bufferBytes, stride, answer, resultBytes, poolBytes, depth, buffers, moveMode,
           geteuid() == 0 ? "true" : "false", wallSeconds * per, fillSeconds * per, collectSeconds * per, moveSeconds * per,
           control->readSeconds * per, control->writeSeconds * per, vmstat("pgmigrate_success") - migratedBefore,
           vmstat("pgmigrate_fail") - failedBefore, control->checksum + answers);
    return 0;
}

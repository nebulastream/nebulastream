//
// Created by Toso, Lorenzo on 2019-03-21.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <Tables.h>
#include "Limits.h"
#include "AbstractMap.h"

#define OVERFLOW_BUF_SIZE 1024
#define BUCKET_SIZE 2


class ETHHashmap : public AbstractMap {

private:
    struct Bucket {
        volatile char     latch;
        /* 3B hole */
        uint32_t          count;
        BuildTuple           tuples[BUCKET_SIZE];
        struct Bucket * next;
    };

    struct BucketBuffer {
        struct BucketBuffer * next;
        uint32_t count;
        Bucket buf[OVERFLOW_BUF_SIZE];
    };

private:
    Bucket * buckets;
    int32_t    num_buckets;
    uint32_t   hash_mask;
    uint32_t   skip_bits;
    std::vector<BucketBuffer*> bucket_buffers;
public:

    explicit ETHHashmap(size_t tuple_count);
    virtual ~ETHHashmap();


    void build(
            const BuildTable &buildTable,
            size_t start_index,
            size_t end_index,
            BucketBuffer** overflowbuf);

    void build_parallel(const BuildTable &buildTable, const Limits & build_limits, size_t threadCount = 0);
    std::vector<ResultTable> probe_parallel(const BuildTable &buildTable, const ProbeTable &probeTable, const Limits & probe_limits, size_t threadCount = 0) const;

    int64_t probe(
            const ProbeTable &probeTable,
            size_t start_index,
            size_t end_index,
            ResultTable &resultTable) const;

    void free_bucket_buffer(BucketBuffer * buf);

    BucketBuffer * init_bucket_buffer();


private:
    void allocate_hashtable(uint32_t nbuckets);
    void get_new_bucket(Bucket ** result, BucketBuffer ** buf);
    void destroy_hashtable();
};




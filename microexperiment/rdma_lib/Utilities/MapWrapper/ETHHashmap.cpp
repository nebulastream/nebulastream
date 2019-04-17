//
// Created by Toso, Lorenzo on 2019-03-21.
//

#include <ETHJoins/lock.h>
#include <cstring>
#include "ETHHashmap.h"
#include "Limits.h"
#include <omp.h>
#include <ETHJoins/tuple_buffer.h>


#define HASH(X, MASK, SKIP) (((X) & MASK) >> SKIP)
#ifndef NEXT_POW_2
/**
 *  compute the next number, greater than or equal to 32-bit unsigned v.
 *  taken from "bit twiddling hacks":
 *  http://graphics.stanford.edu/~seander/bithacks.html
 */
#define NEXT_POW_2(V)                           \
    do {                                        \
        V--;                                    \
        V |= V >> 1;                            \
        V |= V >> 2;                            \
        V |= V >> 4;                            \
        V |= V >> 8;                            \
        V |= V >> 16;                           \
        V++;                                    \
    } while(0)
#endif


void ETHHashmap::build(
        const BuildTable & buildTable,
        size_t start_index,
        size_t end_index,
        BucketBuffer** overflowbuf) {

#ifdef PREFETCH_NPJ
    size_t prefetch_index = PREFETCH_DISTANCE;
#endif

    for (size_t i=start_index; i < end_index; i++) {
        BuildTuple * dest;

#ifdef PREFETCH_NPJ
        if (prefetch_index < rel->num_tuples) {
            intkey_t idx_prefetch = HASH(rel->tuples[prefetch_index++].key,
                                         hashmask, skipbits);
			__builtin_prefetch(ht->buckets + idx_prefetch, 1, 1);
        }
#endif

        int32_t idx = HASH(buildTable.keys[i], hash_mask, skip_bits);
        /* copy the tuple to appropriate hash bucket */
        /* if full, follow nxt pointer to find correct place */
        Bucket * curr = buckets+idx;
        lock(&curr->latch);
        Bucket * nxt = curr->next;

        if(curr->count == BUCKET_SIZE) {
            if(!nxt || nxt->count == BUCKET_SIZE) {
                Bucket * b;
                /* b = (Bucket*) calloc(1, sizeof(Bucket)); */
                /* instead of calloc() everytime, we pre-allocate */
                get_new_bucket(&b, overflowbuf);
                curr->next = b;
                b->next    = nxt;
                b->count   = 1;
                dest       = b->tuples;
            }
            else {
                dest = nxt->tuples + nxt->count;
                nxt->count ++;
            }
        }
        else {
            dest = curr->tuples + curr->count;
            curr->count ++;
        }

        *dest = {buildTable.keys[i], buildTable.payloads[i]};
        unlock(&curr->latch);
    }
}

void ETHHashmap::get_new_bucket(Bucket** result, BucketBuffer** buf) {
    if((*buf)->count < OVERFLOW_BUF_SIZE) {
        *result = (*buf)->buf + (*buf)->count;
        (*buf)->count ++;
    }
    else {
        /* need to allocate new buffer */
        BucketBuffer * new_buf = (BucketBuffer*)
                malloc(sizeof(BucketBuffer));
        new_buf->count = 1;
        new_buf->next  = *buf;
        *buf    = new_buf;
        *result = new_buf->buf;
    }
}

ETHHashmap::BucketBuffer * ETHHashmap::init_bucket_buffer()
{
    BucketBuffer * b = new BucketBuffer();
    b->next = nullptr;
    b->count = 0;
    return b;
}

int64_t ETHHashmap::probe(
        const ProbeTable &probeTable,
        size_t start_index,
        size_t end_index,
        ResultTable &resultTable) const
{

#ifdef PREFETCH_NPJ
    size_t prefetch_index = PREFETCH_DISTANCE;
#endif

    int64_t matches = 0;

    for (size_t i = start_index; i < end_index; i++)
    {
#ifdef PREFETCH_NPJ
        if (prefetch_index < probeTable->num_tuples) {
			intkey_t idx_prefetch = HASH(probeTable->tuples[prefetch_index++].key, hashmask, skipbits);
			__builtin_prefetch(ht->buckets + idx_prefetch, 0, 1);
        }
#endif

        intkey_t idx = HASH(probeTable.keys[i], hash_mask, skip_bits);
        Bucket * b = buckets+idx;

        do {
            for(uint32_t j = 0; j < b->count; j++) {
                if(probeTable.keys[i] == b->tuples[j].key){
                    matches ++;
                    resultTable.push_back(probeTable.keys[i], b->tuples[j].payload, probeTable.payloads[i]);
                }
            }

            b = b->next;/* follow overflow pointer */
        } while(b);
    }

    return matches;
}

void ETHHashmap::free_bucket_buffer(BucketBuffer * buf)
{
    do {
        BucketBuffer * tmp = buf->next;
        free(buf);
        buf = tmp;
    } while(buf);
}

void ETHHashmap::allocate_hashtable(uint32_t nbuckets)
{

    num_buckets = nbuckets;
    NEXT_POW_2((num_buckets));

    /* allocate hashtable buckets cache line aligned */
    if (posix_memalign((void**)&buckets, CACHE_LINE_SIZE, num_buckets * sizeof(Bucket))){
        perror("Aligned allocation failed!\n");
        exit(EXIT_FAILURE);
    }

    memset(buckets, 0, num_buckets * sizeof(Bucket));
    skip_bits = 0; /* the default for modulo hash */
    hash_mask = (num_buckets - 1) << skip_bits;
}

void ETHHashmap::destroy_hashtable() {
    free(buckets);
}

ETHHashmap::ETHHashmap(size_t tuple_count) {
    uint32_t nbuckets = (tuple_count / BUCKET_SIZE);
    allocate_hashtable(nbuckets);
}

ETHHashmap::~ETHHashmap() {
    destroy_hashtable();

    for (auto & bucket_buffer : bucket_buffers)
        free_bucket_buffer(bucket_buffer);
}

void ETHHashmap::build_parallel(const BuildTable &buildTable, const Limits & build_limits, size_t threadCount)
{
    if (threadCount == 0)
        threadCount = omp_get_max_threads() / 2;

    if(!bucket_buffers.empty()){
        for (auto & bucket_buffer : bucket_buffers)
            free_bucket_buffer(bucket_buffer);
    }

    for(size_t i=0; i < threadCount;i++)
        bucket_buffers.push_back(init_bucket_buffer());

    omp_set_dynamic(0);

    const size_t buildRecordsPerThread = build_limits.size() / threadCount;
    const size_t buildRemainingRecords = build_limits.size() % threadCount;

    #pragma omp parallel num_threads(threadCount)
    {
        size_t threadId = omp_get_thread_num();
        size_t buildSliceSize = buildRecordsPerThread;
        size_t buildStartAt = omp_get_thread_num() * buildSliceSize + build_limits.start_index;

        if (threadId == threadCount - 1) {
            buildSliceSize += buildRemainingRecords;
        }

        build(buildTable, buildStartAt, buildStartAt + buildSliceSize, &bucket_buffers[threadId]);
    }
}

std::vector<ResultTable> ETHHashmap::probe_parallel(const BuildTable &buildTable, const ProbeTable &probeTable, const Limits & probe_limits, size_t threadCount) const {

    if (threadCount == 0)
        threadCount = omp_get_max_threads()/2;

    omp_set_dynamic(0);

    const size_t probeRecordsPerThread = probe_limits.size() / threadCount;
    const size_t probeRemainingRecords = probe_limits.size() % threadCount;


    std::vector<ResultTable> all_results(threadCount);

    #pragma omp parallel num_threads(threadCount)
    {
        ResultTable local_results;

        size_t threadId = omp_get_thread_num();

        size_t probeSliceSize = probeRecordsPerThread;
        size_t probeStartAt = omp_get_thread_num() * probeSliceSize + probe_limits.start_index;
        if (threadId == threadCount - 1) {
            probeSliceSize += probeRemainingRecords;
        }

        probe(probeTable, probeStartAt, probeStartAt + probeSliceSize, all_results[threadId]);
    }

    return all_results;

}

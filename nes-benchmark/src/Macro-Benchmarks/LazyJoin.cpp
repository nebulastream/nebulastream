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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/NesThread.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <folly/AtomicLinkedList.h>
#include <immintrin.h>
#include <random>
#include <sys/mman.h>
#include <tsl/robin_map.h>
#include <xmmintrin.h>

#ifndef min
#define min(A, B) ((A) < (B) ? (A) : (B))
#endif

struct alignas(16) LeftStream {
    uint64_t key;
    uint64_t timestamp;
};

struct alignas(16) RightStream {
    uint64_t key;
    uint64_t timestamp;
};

namespace Slash {
class abstract_zipfian_generator {
  public:
    virtual uint64_t operator()(std::mt19937& rng) = 0;
};

class ycsb_zipfian_generator : public abstract_zipfian_generator {
    static constexpr auto default_zipfian_factor = .99;

  public:
    explicit ycsb_zipfian_generator(uint64_t min_, uint64_t max_, double zipfian_factor = default_zipfian_factor)
        : ycsb_zipfian_generator(min_, max_, zipfian_factor, compute_zeta(0, max_ - min_ + 1, zipfian_factor, 0)) {
        // nop
    }

    explicit ycsb_zipfian_generator(uint64_t min_, uint64_t max_, double zipfianconstant_, double zetan_) : dist(0.0, 1.0) {
        num_items = max_ - min_ + 1;
        min = min_;
        theta = zipfianconstant = zipfianconstant_;

        theta = zipfianconstant;

        zeta2theta = zeta(0, 2, theta, 0);

        alpha = 1.0 / (1.0 - theta);
        zetan = zetan_;
        countforzeta = num_items;
        eta = (1 - std::pow(2.0 / num_items, 1 - theta)) / (1 - zeta2theta / zetan);
    }

    uint64_t operator()(std::mt19937& rng) override { return (*this)(rng, countforzeta); }

    uint64_t operator()(std::mt19937& rng, uint64_t new_item_count) {
        if (new_item_count > countforzeta) {
            // we have added more items. can compute zetan incrementally, which is cheaper
            num_items = new_item_count;
            zetan = zeta(countforzeta, num_items, theta, zetan);
            eta = (1 - std::pow(2.0 / num_items, 1 - theta)) / (1 - zeta2theta / zetan);
        }
        double u = dist(rng);
        double uz = u * zetan;
        if (uz < 1.0) {
            return min;
        }

        if (uz < 1.0 + std::pow(0.5, theta)) {
            return min + 1;
        }

        long ret = min + (long) ((num_items) *std::pow(eta * u - eta + 1, alpha));
        return ret;
    }

  private:
    double zeta(uint64_t st, uint64_t n, double thetaVal, double initialsum) {
        countforzeta = n;
        return compute_zeta(st, n, thetaVal, initialsum);
    }

    static double compute_zeta(uint64_t st, uint64_t n, double theta, double initialsum) {
        double sum = initialsum;
        for (auto i = st; i < n; i++) {
            sum += 1 / (std::pow(i + 1, theta));
        }
        return sum;
    }

  private:
    uint64_t num_items;
    uint64_t min;
    double zipfianconstant;
    double alpha, zetan, eta, theta, zeta2theta;
    uint64_t countforzeta;
    std::uniform_real_distribution<double> dist;
};
}// namespace Slash
namespace NES {
namespace detail {

int memcpy_uncached_store_avx(void* dest, const void* src, size_t n_bytes) {
    int ret = 0;
#ifdef __AVX__
    char* d = (char*) dest;
    uintptr_t d_int = (uintptr_t) d;
    const char* s = (const char*) src;
    uintptr_t s_int = (uintptr_t) s;
    size_t n = n_bytes;

    // align dest to 256-bits
    if (d_int & 0x1f) {
        size_t nh = min(0x20 - (d_int & 0x1f), n);
        memcpy(d, s, nh);
        d += nh;
        d_int += nh;
        s += nh;
        s_int += nh;
        n -= nh;
    }

    if (s_int & 0x1f) {// src is not aligned to 256-bits
        __m256d r0, r1, r2, r3;
        // unroll 4
        while (n >= 4 * sizeof(__m256d)) {
            r0 = _mm256_loadu_pd((double*) (s + 0 * sizeof(__m256d)));
            r1 = _mm256_loadu_pd((double*) (s + 1 * sizeof(__m256d)));
            r2 = _mm256_loadu_pd((double*) (s + 2 * sizeof(__m256d)));
            r3 = _mm256_loadu_pd((double*) (s + 3 * sizeof(__m256d)));
            _mm256_stream_pd((double*) (d + 0 * sizeof(__m256d)), r0);
            _mm256_stream_pd((double*) (d + 1 * sizeof(__m256d)), r1);
            _mm256_stream_pd((double*) (d + 2 * sizeof(__m256d)), r2);
            _mm256_stream_pd((double*) (d + 3 * sizeof(__m256d)), r3);
            s += 4 * sizeof(__m256d);
            d += 4 * sizeof(__m256d);
            n -= 4 * sizeof(__m256d);
        }
        while (n >= sizeof(__m256d)) {
            r0 = _mm256_loadu_pd((double*) (s));
            _mm256_stream_pd((double*) (d), r0);
            s += sizeof(__m256d);
            d += sizeof(__m256d);
            n -= sizeof(__m256d);
        }
    } else {// or it IS aligned
        __m256d r0, r1, r2, r3, r4, r5, r6, r7;
        // unroll 8
        while (n >= 8 * sizeof(__m256d)) {
            r0 = _mm256_load_pd((double*) (s + 0 * sizeof(__m256d)));
            r1 = _mm256_load_pd((double*) (s + 1 * sizeof(__m256d)));
            r2 = _mm256_load_pd((double*) (s + 2 * sizeof(__m256d)));
            r3 = _mm256_load_pd((double*) (s + 3 * sizeof(__m256d)));
            r4 = _mm256_load_pd((double*) (s + 4 * sizeof(__m256d)));
            r5 = _mm256_load_pd((double*) (s + 5 * sizeof(__m256d)));
            r6 = _mm256_load_pd((double*) (s + 6 * sizeof(__m256d)));
            r7 = _mm256_load_pd((double*) (s + 7 * sizeof(__m256d)));
            _mm256_stream_pd((double*) (d + 0 * sizeof(__m256d)), r0);
            _mm256_stream_pd((double*) (d + 1 * sizeof(__m256d)), r1);
            _mm256_stream_pd((double*) (d + 2 * sizeof(__m256d)), r2);
            _mm256_stream_pd((double*) (d + 3 * sizeof(__m256d)), r3);
            _mm256_stream_pd((double*) (d + 4 * sizeof(__m256d)), r4);
            _mm256_stream_pd((double*) (d + 5 * sizeof(__m256d)), r5);
            _mm256_stream_pd((double*) (d + 6 * sizeof(__m256d)), r6);
            _mm256_stream_pd((double*) (d + 7 * sizeof(__m256d)), r7);
            s += 8 * sizeof(__m256d);
            d += 8 * sizeof(__m256d);
            n -= 8 * sizeof(__m256d);
        }
        while (n >= sizeof(__m256d)) {
            r0 = _mm256_load_pd((double*) (s));
            _mm256_stream_pd((double*) (d), r0);
            s += sizeof(__m256d);
            d += sizeof(__m256d);
            n -= sizeof(__m256d);
        }
    }

    if (n)
        memcpy(d, s, n);

    // fencing is needed even for plain memcpy(), due to performance
    // being hit by delayed flushing of WC buffers
    _mm_sfence();

#else
#error "this file should be compiled with -mavx"
#endif
    return ret;
}

template<typename T = void>
T* allocAligned(size_t size, size_t alignment = 16) {
    void* tmp = nullptr;
    NES_ASSERT2_FMT(0 == posix_memalign(&tmp, alignment, sizeof(T) * size),
                    "Cannot allocate " << sizeof(T) * size << " bytes: " << strerror(errno));
    return reinterpret_cast<T*>(tmp);
}

template<typename T = void, size_t huge_page_size = 1 << 21>
T* allocHugePages(size_t size) {
    void* tmp = nullptr;
    NES_ASSERT2_FMT(0 == posix_memalign(&tmp, huge_page_size, sizeof(T) * size),
                    "Cannot allocate " << sizeof(T) * size << " bytes: " << strerror(errno));
    madvise(tmp, size * sizeof(T), MADV_HUGEPAGE);
    NES_ASSERT2_FMT(tmp != nullptr, "Cannot remap as huge pages");
    mlock(tmp, size * sizeof(T));
    //    uint8_t* ptr = reinterpret_cast<uint8_t*>(tmp);
    //    for (size_t i = 0, len = size * sizeof(T); i < len; i += huge_page_size) {
    //        *(ptr + i) = i;
    //    }
    return reinterpret_cast<T*>(tmp);
}

}// namespace detail

class ImmutableBloomFilter;

class alignas(64) BloomFilter {
    friend class ImmutableBloomFilter;

  private:
    uint32_t bits{0};     /* Number of bits in the bloom filter buffer */
    uint16_t hashes{0};   /* Number of hashes used per element added */
    uint8_t* bf{nullptr}; /* Location of the underlying bit field */
  public:
    explicit BloomFilter() {}

    explicit BloomFilter(uint64_t entries, double fp_rate) {
        double num, denom, bpe, dentries;
        uint64_t bytes;

        //        if (fp_rate >= 1 || fp_rate <= 0) {
        //            return 1;
        //        }

        /*
         * Initialization code inspired by:
         * https://github.com/jvirkki/libbloom/blob/master/bloom.c#L95
         */
        num = std::log(fp_rate);
        denom = 0.480453013918201;// ln(2)^2
        bpe = -(num / denom);

        dentries = (double) entries;
        bits = (uint_fast32_t) (dentries * bpe);

        if (bits % 8) {
            bytes = (bits / 8) + 1;
        } else {
            bytes = bits / 8;
        }

        hashes = (uint16_t) std::ceil(0.693147180559945 * bpe);// ln(2)
        bf = detail::allocAligned<uint8_t>(bytes, 64);
        memset(bf, 0, bytes);
    }

    ~BloomFilter() {
        if (bf) {
            delete[] bf;
        }
    }

    void add(uint64_t hash) {
        uint64_t hits;
        uint32_t a, b, x, i;
        a = hash & ((1ul << 32) - 1);
        b = hash >> 32;
        for (i = 0; i < hashes; i++) {
            x = (a + i * b) % bits;
            *(bf + (x >> 3)) |= (1 << (x & 7));
        }
    }
};

class alignas(64) ImmutableBloomFilter {

  private:
    uint32_t bits{0};     /* Number of bits in the bloom filter buffer */
    uint16_t hashes{0};   /* Number of hashes used per element added */
    uint8_t* bf{nullptr}; /* Location of the underlying bit field */

  public:
    explicit ImmutableBloomFilter() {}

    explicit ImmutableBloomFilter(BloomFilter& that) : bits(that.bits), hashes(that.hashes), bf(that.bf) {}

    bool check(uint64_t h) const {
        uint64_t hits = 0;
        uint32_t a, b, x, i;

        /* See bloom_add for comment on hash functions used */
        h ^= h >> 33;
        h *= UINT64_C(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h *= UINT64_C(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        a = h & ((1ul << 32) - 1);
        b = h >> 32;
        for (i = 0; i < hashes; i++) {
            x = (a + i * b) % bits;
            if (*(bf + (x >> 3)) & (1 << (x & 7))) {
                hits++;
            } else {
                return false;
            }
        }
        return hits == hashes;
    }
};

static_assert(sizeof(BloomFilter) == 64);
static_assert(sizeof(ImmutableBloomFilter) == 64);

template<typename>
class ImmutableFixedPage;

template<typename T>
class alignas(64) FixedPage {
    template<typename>
    friend class ImmutableFixedPage;
    static constexpr auto CHUNK_SIZE = 64 * 1024;
    static constexpr auto MAX_ITEM = CHUNK_SIZE / sizeof(T);

  public:
    explicit FixedPage(std::atomic<uint64_t>& tail, uint64_t overrunAddress) : bf(MAX_ITEM, 1e-2) {
        auto ptr = tail.fetch_add(CHUNK_SIZE);
        NES_ASSERT2_FMT(ptr < overrunAddress, "Invalid address " << ptr << " < " << overrunAddress);
        data = reinterpret_cast<T*>(ptr);
    }

    ~FixedPage() = default;

    bool append(const uint64_t hash, const T* element) {
        if constexpr (sizeof(T) >= 64) {
            //            detail::memcpy_uncached_store_avx(&data[pos++], element, sizeof(T));
        } else {
            data[pos++] = *element;
            bf.add(hash);
        }
        return pos < MAX_ITEM;
    }

    size_t size() const { return pos; }

    T& operator[](size_t index) const { return data[index]; }

  private:
    T* data{nullptr};
    size_t pos{0};
    BloomFilter bf;
};

template<typename T>
class ImmutableFixedPage {
  public:
    ImmutableFixedPage() = default;

    explicit ImmutableFixedPage(FixedPage<T>* page) : data(page->data), pos(page->pos), bf(page->bf) {}

    ImmutableFixedPage(const ImmutableFixedPage& that) : data(that.data), pos(that.pos), bf(that.bf) {}

    ImmutableFixedPage(ImmutableFixedPage&& that) : data(that.data), pos(that.pos), bf(that.bf) {
        that.data = nullptr;
        that.pos = 0;
        that.bf = BloomFilter();
    }

    ImmutableFixedPage& operator=(ImmutableFixedPage&& that) {
        if (this == std::addressof(that)) {
            return *this;
        }
        using std::swap;
        swap(*this, that);
        return *this;
    }

    inline friend void swap(ImmutableFixedPage& lhs, ImmutableFixedPage& rhs) noexcept {
        std::swap(lhs.data, rhs.data);
        std::swap(lhs.pos, rhs.pos);
        std::swap(lhs.bf, rhs.bf);
    }

    size_t size() const { return pos; }

    T const& operator[](size_t index) const { return data[index]; }

    bool bfCheck(uint64_t key) const { return bf.check(key); }

  private:
    T const* data{nullptr};
    size_t pos{0};
    ImmutableBloomFilter bf{};
};

template<typename T>
class alignas(64) FixedPagesLinkedList {
  public:
    explicit FixedPagesLinkedList(std::atomic<uint64_t>& tail, uint64_t overrunAddress)
        : tail(tail), overrunAddress(overrunAddress) {
        for (auto i = 0; i < 2; ++i) {
            pages.emplace_back(new FixedPage<T>(this->tail, overrunAddress));
        }
        currPage = pages[0];
    }

    ~FixedPagesLinkedList() {
        std::for_each(pages.begin(), pages.end(), [](FixedPage<T>* p) {
            delete p;
        });
    }

    void append(const uint64_t hash, const T* element) {
        if (!currPage->append(hash, element)) {
            if (++pos < pages.size()) {
                currPage = pages[pos];
            } else {
                pages.emplace_back(currPage = new FixedPage<T>(this->tail, overrunAddress));
            }
        }
        size++;
    }

  private:
    std::atomic<uint64_t>& tail;
    FixedPage<T>* currPage;
    size_t pos{0};
    uint64_t overrunAddress;

  public:
    std::vector<FixedPage<T>*> pages;
    size_t size;
};

template<typename T>
class alignas(64) AtomicFixedPagesLinkedList {
  private:
    class InternalNode {
      public:
        ImmutableFixedPage<T> data;
        std::atomic<InternalNode*> next;
    };

  public:
    ~AtomicFixedPagesLinkedList() {
        auto head = this->head.exchange(nullptr);
        while (head != nullptr) {
            auto* tmp = head;
            head = tmp->next;
            delete tmp;
        }
    }

    void append(FixedPagesLinkedList<T>& list) {
        for (auto* page : list.pages) {
            auto oldHead = head.load(std::memory_order::relaxed);
            auto node = new InternalNode{ImmutableFixedPage(page), oldHead};
            while (!head.compare_exchange_weak(oldHead, node, std::memory_order::release, std::memory_order::relaxed)) {
            }
            numItems.fetch_add(page->size(), std::memory_order::relaxed);
        }
        numPages.fetch_add(list.pages.size(), std::memory_order::relaxed);
    }

    size_t size() const { return numItems.load(std::memory_order::release); }

    size_t numOfPages() const { return numPages.load(std::memory_order::release); }

    std::vector<ImmutableFixedPage<T>> cache() {
        std::vector<ImmutableFixedPage<T>> ret(numOfPages());
        uint64_t i = 0;
        auto head = this->head.load();
        while (head != nullptr) {
            auto* tmp = head;
            ret[i++] = std::move(tmp->data);
            head = tmp->next;
            //delete tmp;
        }
        return ret;
    }

  private:
    std::atomic<InternalNode*> head{nullptr};
    std::atomic<size_t> numItems{0};
    std::atomic<size_t> numPages{0};
};

template<typename T>
concept TimestampedRecord = requires(T record) {
                                { record.timestamp } -> std::convertible_to<int64_t>;
                                { record.key } -> std::convertible_to<int64_t>;
                            };

static constexpr auto NUM_PARTITIONS = 8 * 1024;

template<typename TimestampedRecord>
class SharedJoinHashTable {
  public:
    std::array<AtomicFixedPagesLinkedList<TimestampedRecord>, NUM_PARTITIONS> ght;
};

class JoinControlBlock {
  public:
    JoinControlBlock& operator=(const uint64_t x) {
        control.store(x);
        return *this;
    }

    uint64_t fetch_sub(uint64_t x) { return control.fetch_sub(x); }

  private:
    std::atomic<uint64_t> control{0};
};

template<TimestampedRecord LHS, TimestampedRecord RHS>
class JoinSharedState {
  public:
    JoinControlBlock control;
    SharedJoinHashTable<LHS> ghtLeft;
    SharedJoinHashTable<RHS> ghtRight;
};

template<TimestampedRecord LHS, TimestampedRecord RHS>
class JoinTriggerPipeline : public Runtime::Execution::ExecutablePipelineStage {
    using JoinSharedStateType = JoinSharedState<LHS, RHS>;

  public:
    explicit JoinTriggerPipeline(JoinSharedStateType& sharedState)
        : Runtime::Execution::ExecutablePipelineStage(Unary), sharedState(sharedState) {}
    ExecutionResult
    execute(Runtime::TupleBuffer& buff, Runtime::Execution::PipelineExecutionContext&, Runtime::WorkerContext& wctx) override {
        auto ptr = buff.getBuffer();
        auto partitionId = reinterpret_cast<uint64_t>(ptr) - 1;

        auto leftChain = sharedState.ghtLeft.ght[partitionId].cache();
        auto rightChain = sharedState.ghtRight.ght[partitionId].cache();

        size_t leftChainTuples = sharedState.ghtLeft.ght[partitionId].size();
        size_t rightChainTuples = sharedState.ghtRight.ght[partitionId].size();

        NES_DEBUG("Worker " << wctx.getId() << " got " << partitionId << " size #tuples=" << leftChainTuples << " #tuples="
                            << rightChainTuples << " size #pages=" << leftChain.size() << " #pages=" << rightChain.size());

        size_t joinedTuples = 0;
        // we want a small probe side
        if (leftChainTuples && rightChainTuples) {
            if (leftChainTuples > rightChainTuples) {
                joinedTuples = executeJoin<RHS, LHS>(wctx, partitionId, std::move(rightChain), std::move(leftChain));
            } else {
                joinedTuples = executeJoin<LHS, RHS>(wctx, partitionId, std::move(leftChain), std::move(rightChain));
            }
        }

        if (joinedTuples) {
            NES_DEBUG("Worker " << wctx.getId() << " got " << partitionId << " joined #tuple=" << joinedTuples);
            NES_ASSERT2_FMT(joinedTuples <= (leftChainTuples * rightChainTuples),
                            "Something wrong #joinedTuples= " << joinedTuples << " upper bound "
                                                              << (leftChainTuples * rightChainTuples));
        }
        return ExecutionResult::Ok;
    }

  private:
    template<typename ProbeSideType, typename BuildSideType>
    size_t executeJoin(Runtime::WorkerContext& wctx,
                       uint64_t partitionId,
                       std::vector<ImmutableFixedPage<ProbeSideType>>&& probeSide,
                       std::vector<ImmutableFixedPage<BuildSideType>>&& buildSide) {
        size_t joinedTuples = 0;
        for (auto& lhsPage : probeSide) {
            auto lhsLen = lhsPage.size();
            for (auto i = 0ul; i < lhsLen; ++i) {
                auto& lhsRecord = lhsPage[i];
                auto lhsKey = lhsRecord.key;
                for (auto& rhsPage : buildSide) {
                    auto rhsLen = rhsPage.size();
                    if (rhsLen == 0 || !rhsPage.bfCheck(lhsKey)) {
                        continue;
                    }
                    for (auto j = 0ul; j < rhsLen; ++j) {
                        auto& rhsRecord = rhsPage[j];
                        if (rhsRecord.key == lhsKey) {
                            ++joinedTuples;
                            // call next operator or sink
                        }
                    }
                }
            }
        }
        ((void) wctx);
        ((void) partitionId);
        return joinedTuples;
    }

  private:
    JoinSharedStateType& sharedState;
};

template<typename TimestampedRecord, bool isLeftSize>
class JoinPipelineStage : public Runtime::Execution::ExecutablePipelineStage, public Runtime::BufferRecycler {
    static constexpr auto BUFFER_SIZE = 16ul * 1024 * 1024 * 1024;// 16GB
    static constexpr auto MASK = NUM_PARTITIONS - 1;

  public:
    void recyclePooledBuffer(Runtime::detail::MemorySegment*) override {}
    void recycleUnpooledBuffer(Runtime::detail::MemorySegment*) override {}

  public:
    explicit JoinPipelineStage(PipelineStageArity arity,
                               JoinControlBlock& controlBlock,
                               SharedJoinHashTable<TimestampedRecord>& sharedState,
                               Runtime::Execution::ExecutablePipelinePtr joinTrigger)
        : Runtime::Execution::ExecutablePipelineStage(arity), sharedState(sharedState), joinControlBlock(controlBlock),
          joinTrigger(joinTrigger) {
#if 0
        head = detail::allocAligned<uint8_t>(BUFFER_SIZE, 64);
#else
        head = detail::allocHugePages<uint8_t>(BUFFER_SIZE);
#endif
        tail.store(reinterpret_cast<uintptr_t>(head));
        overrunAddress = reinterpret_cast<uintptr_t>(head) + BUFFER_SIZE;
    }

    ~JoinPipelineStage() { std::free(head); }

    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& execCtx,
                            Runtime::WorkerContext& wctx) override {
        auto* input = buffer.getBuffer<TimestampedRecord>();
        auto numOfTuples = buffer.getNumberOfTuples();

        auto& ht = this->ht[wctx.getId()]->inner;

        for (auto i = 0ul, j = 0ul, len = numOfTuples / 4ul; i < len; ++i) {
            for (auto k = 0ul; k < 4ul; ++k, ++j) {
                auto* record = input + j;
                if PLACEHOLDER_LIKELY (record->timestamp) {
                    uint64_t h = record->key;
                    h ^= h >> 33;
                    h *= UINT64_C(0xff51afd7ed558ccd);
                    h ^= h >> 33;
                    h *= UINT64_C(0xc4ceb9fe1a85ec53);
                    h ^= h >> 33;
                    ht[h & MASK]->append(h, record);
                } else {
                    NES_INFO("Got 0 timestamp -> trigger window key " << record->key);
                    for (auto i = 0; i < NUM_PARTITIONS; ++i) {
                        sharedState.ght[i].append(*ht[i]);
                    }
                    if (joinControlBlock.fetch_sub(1) == 1) {
                        // let's trigger
                        auto queryManager = execCtx.getQueryManager();
                        for (auto i = 0; i < NUM_PARTITIONS; ++i) {
                            auto partitionId = i + 1;
                            auto buffer = Runtime::TupleBuffer::wrapMemory(reinterpret_cast<uint8_t*>(partitionId), 1, this);
                            queryManager->addWorkForNextPipeline(buffer, joinTrigger);
                        }
                    }
                }
            }
        }
        processedBytes[wctx.getId()].counter += (numOfTuples * 16);
        return ExecutionResult::Ok;
    }

    void reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) override {
        Reconfigurable::reconfigure(message, context);
        switch (message.getType()) {
            case Runtime::Initialize: {
                ht[context.getId()] = new HashTable;
                for (auto i = 0; i < NUM_PARTITIONS; ++i) {
                    ht[context.getId()]->inner[i] = new FixedPagesLinkedList<TimestampedRecord>(tail, overrunAddress);
                }
                processedBytes[context.getId()].counter = 0;
                timings[context.getId()].counter = std::chrono::high_resolution_clock::now().time_since_epoch().count();
                break;
            }
            case Runtime::SoftEndOfStream:
            case Runtime::HardEndOfStream:
            case Runtime::FailEndOfStream: {
                auto elapsedNs =
                    std::chrono::high_resolution_clock::now().time_since_epoch().count() - timings[context.getId()].counter;
                auto processedMb = processedBytes[context.getId()].counter / (1024ul * 1024ul);
                auto throughput = processedMb / (elapsedNs / 1'000'000'000.0);
                NES_DEBUG("Throughput worker=" << context.getId() << " is " << throughput);
                for (auto i = 0; i < NUM_PARTITIONS; ++i) {
                    //                    NES_INFO("Partition i=" << i << " has " << ht[context.getId()]->inner[i]->size);
                    delete ht[context.getId()]->inner[i];
                }
                delete ht[context.getId()];
            }
            default: {
                break;
            }
        }
    }

  private:
    struct alignas(64) HashTable {
        std::array<FixedPagesLinkedList<TimestampedRecord>*, NUM_PARTITIONS> inner;
    };

    template<typename T>
    struct alignas(64) Counter {
        T counter;
    };

    std::array<HashTable*, Runtime::NesThread::MaxNumThreads> ht;

    std::array<Counter<int64_t>, Runtime::NesThread::MaxNumThreads> timings;
    std::array<Counter<size_t>, Runtime::NesThread::MaxNumThreads> processedBytes;

    uint8_t* head;
    std::atomic<uint64_t> tail;
    uint64_t overrunAddress;

    SharedJoinHashTable<TimestampedRecord>& sharedState;
    JoinControlBlock& joinControlBlock;

    Runtime::Execution::ExecutablePipelinePtr joinTrigger;
};

class LeftJoinPipelineStage : public JoinPipelineStage<LeftStream, true> {
  public:
    explicit LeftJoinPipelineStage(JoinControlBlock& controlBlock,
                                   SharedJoinHashTable<LeftStream>& sharedState,
                                   Runtime::Execution::ExecutablePipelinePtr joinTrigger)
        : JoinPipelineStage(BinaryLeft, controlBlock, sharedState, joinTrigger) {}
};

class RightJoinPipelineStage : public JoinPipelineStage<RightStream, false> {
  public:
    explicit RightJoinPipelineStage(JoinControlBlock& controlBlock,
                                    SharedJoinHashTable<RightStream>& sharedState,
                                    Runtime::Execution::ExecutablePipelinePtr joinTrigger)
        : JoinPipelineStage(BinaryRight, controlBlock, sharedState, joinTrigger) {}
};

void createBenchmarkSources(std::vector<DataSourcePtr>& dataProducers,
                            Runtime::NodeEnginePtr nodeEngine,
                            SchemaPtr schema,
                            size_t partitionSizeBytes,
                            uint64_t numOfSources,
                            uint64_t startOffset,
                            Runtime::Execution::ExecutablePipelinePtr pipeline,
                            std::function<void(uint8_t*, size_t, size_t)> writer) {
    auto ofs = dataProducers.size() + startOffset;
    auto bufferSize = nodeEngine->getBufferManager()->getBufferSize();
    size_t numOfBuffers = partitionSizeBytes / bufferSize;
    for (auto i = 0ul; i < numOfSources; ++i) {
        auto start = std::chrono::system_clock::now();
        size_t dataSegmentSize = bufferSize * numOfBuffers;
        NES_INFO("Creating BenchmarkSource #" << i);
        auto* data = detail::allocHugePages<uint8_t>(dataSegmentSize);// new uint8_t[dataSegmentSize];
        writer(data, dataSegmentSize, reinterpret_cast<uintptr_t>(data));
        std::shared_ptr<uint8_t> ptr(data, [](uint8_t* ptr) {
            //delete[] ptr;
            std::free(ptr);
        });
        std::vector<Runtime::Execution::SuccessorExecutablePipeline> pipelines;
        pipelines.emplace_back(pipeline);
        auto source = std::make_shared<BenchmarkSource>(schema,
                                                        std::move(ptr),
                                                        dataSegmentSize,
                                                        nodeEngine->getBufferManager(),
                                                        nodeEngine->getQueryManager(),
                                                        numOfBuffers,
                                                        0ul,
                                                        i + ofs,
                                                        0,
                                                        64ul,
                                                        GatheringMode::INGESTION_RATE_MODE,
                                                        SourceMode::WRAP_BUFFER,
                                                        i,
                                                        0ul,
                                                        pipelines);
        dataProducers.emplace_back(source);
        auto diff = std::chrono::system_clock::now() - start;
        NES_INFO("Created BenchmarkSource #" << i << " in "
                                             << std::chrono::duration_cast<std::chrono::milliseconds>(diff).count());
    }
}

class DummyQueryListener : public AbstractQueryStatusListener {
  public:
    virtual ~DummyQueryListener() {}

    bool canTriggerEndOfStream(QueryId, QuerySubPlanId, OperatorId, Runtime::QueryTerminationType) override { return true; }
    bool notifySourceTermination(QueryId, QuerySubPlanId, OperatorId, Runtime::QueryTerminationType) override { return true; }
    bool notifyQueryFailure(QueryId, QuerySubPlanId, std::string) override { return true; }
    bool notifyQueryStatusChange(QueryId, QuerySubPlanId, Runtime::Execution::ExecutableQueryPlanStatus) override { return true; }
    bool notifyEpochTermination(uint64_t, uint64_t) override { return false; }
};

}// namespace NES
int main(int, char**) {
    using namespace NES;
    NES::Logger::setupLogging("LazyJoin.log", NES::LogLevel::LOG_INFO);
    auto lhsStreamSchema = Schema::create()
                               ->addField("key", DataTypeFactory::createUInt64())
                               ->addField("timestamp", DataTypeFactory::createUInt64());

    auto rhsStreamSchema = Schema::create()
                               ->addField("key", DataTypeFactory::createUInt64())
                               ->addField("timestamp", DataTypeFactory::createUInt64());

    std::vector<PhysicalSourcePtr> physicalSources;

    auto leftStreamSize = 32 * 1024 * 1024;
    auto rightStreamSize = 32 * 1024 * 1024;

    auto workerConfig = std::make_shared<WorkerConfiguration>();
    workerConfig->bufferSizeInBytes = 1024 * 1024;
    workerConfig->numberOfBuffersInGlobalBufferManager = 4 * 1024;
    workerConfig->numberOfBuffersPerWorker = 64;
    workerConfig->numberOfBuffersInSourceLocalBufferPool = 64;
    workerConfig->numWorkerThreads = 16;

    auto fakeWorker = std::make_shared<DummyQueryListener>();
    auto engine = Runtime::NodeEngineBuilder::create(workerConfig).setQueryStatusListener(fakeWorker).build();
    Exceptions::installGlobalErrorListener(engine);

    auto sink = createNullOutputSink(0, 0, engine, 1);

    auto numSourcesLeft = 4;
    auto numSourcesRight = 4;

    JoinSharedState<LeftStream, RightStream> joinSharedState;
    joinSharedState.control = numSourcesLeft + numSourcesRight;

    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;

    //    auto joinStateLeft = std::make_shared<JoinState>(engine->getHardwareManager(), 1024, 1024 * 1024 * 1024);
    //    auto joinStateRight = std::make_shared<JoinState>(engine->getHardwareManager(), 1024, 1024 * 1024 * 1024);

    auto executionContextLeft = std::make_shared<Runtime::Execution::PipelineExecutionContext>(
        0,
        engine->getQueryManager(),
        [sink](Runtime::TupleBuffer& buffer, Runtime::WorkerContext& wctx) {
            sink->writeData(buffer, wctx);
        },
        [](Runtime::TupleBuffer&) {
        },
        operatorHandlers);

    auto executionContextRight = std::make_shared<Runtime::Execution::PipelineExecutionContext>(
        0,
        engine->getQueryManager(),
        [sink](Runtime::TupleBuffer& buffer, Runtime::WorkerContext& wctx) {
            sink->writeData(buffer, wctx);
        },
        [](Runtime::TupleBuffer&) {
        },
        operatorHandlers);

    auto executableTrigger = std::make_shared<JoinTriggerPipeline<LeftStream, RightStream>>(joinSharedState);
    auto pipelineTrigger =
        Runtime::Execution::ExecutablePipeline::create(2, 0, 0, executionContextRight, executableTrigger, 2, {sink});

    auto executableLeft =
        std::make_shared<LeftJoinPipelineStage>(joinSharedState.control, joinSharedState.ghtLeft, pipelineTrigger);
    auto executableRight =
        std::make_shared<RightJoinPipelineStage>(joinSharedState.control, joinSharedState.ghtRight, pipelineTrigger);

    auto pipelineLeft = Runtime::Execution::ExecutablePipeline::create(0,
                                                                       0,
                                                                       0,
                                                                       executionContextLeft,
                                                                       executableLeft,
                                                                       numSourcesLeft,
                                                                       {pipelineTrigger});
    auto pipelineRight = Runtime::Execution::ExecutablePipeline::create(1,
                                                                        0,
                                                                        0,
                                                                        executionContextRight,
                                                                        executableRight,
                                                                        numSourcesRight,
                                                                        {pipelineTrigger});

    std::vector<DataSourcePtr> dataProducers;

    createBenchmarkSources(dataProducers,
                           engine,
                           lhsStreamSchema,
                           leftStreamSize,
                           numSourcesLeft,
                           1,
                           pipelineLeft,
                           [](uint8_t* data, size_t length, uint64_t seed) {
                               auto* ptr = reinterpret_cast<LeftStream*>(data);
                               auto numOfTuples = length / sizeof(LeftStream);
                               std::mt19937 engine(seed * std::chrono::high_resolution_clock::now().time_since_epoch().count());
                               Slash::ycsb_zipfian_generator generator(0, 10'000, 0.2);

                               for (size_t i = 0; i < numOfTuples; ++i) {
                                   ptr[i].key = generator(engine);
                                   ptr[i].timestamp = 1;
                               }
                               ptr[numOfTuples - 1].timestamp = 0;
                           });

    createBenchmarkSources(dataProducers,
                           engine,
                           rhsStreamSchema,
                           rightStreamSize,
                           numSourcesRight,
                           1,
                           pipelineRight,
                           [](uint8_t* data, size_t length, uint64_t seed) {
                               auto* ptr = reinterpret_cast<RightStream*>(data);
                               auto numOfTuples = length / sizeof(RightStream);
                               std::mt19937 engine(seed * std::chrono::high_resolution_clock::now().time_since_epoch().count());
                               Slash::ycsb_zipfian_generator generator(0, 10'000, 0.2);

                               for (size_t i = 0; i < numOfTuples; ++i) {
                                   ptr[i].key = generator(engine);
                                   ptr[i].timestamp = 1;
                               }
                               ptr[numOfTuples - 1].timestamp = 0;
                           });

    auto executionPlan = Runtime::Execution::ExecutableQueryPlan::create(0,
                                                                         0,
                                                                         dataProducers,
                                                                         {sink},
                                                                         {pipelineLeft, pipelineRight, pipelineTrigger},
                                                                         engine->getQueryManager(),
                                                                         engine->getBufferManager());

    NES_ASSERT(engine->registerQueryInNodeEngine(executionPlan), "Cannot register query");

    auto startTs = std::chrono::high_resolution_clock::now();

    NES_ASSERT(engine->startQuery(executionPlan->getQueryId()), "Cannot start query");

    while (engine->getQueryStatus(executionPlan->getQueryId()) == Runtime::Execution::ExecutableQueryPlanStatus::Running) {
        _mm_pause();
    }

    auto statistics = engine->getQueryManager()->getQueryStatistics(executionPlan->getQuerySubPlanId());
    auto nowTs = std::chrono::high_resolution_clock::now();
    auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(nowTs - startTs);
    double elapsedSec = elapsedNs.count() / 1'000'000'000.0;
    double MTuples = statistics->getProcessedTuple() / 1'000'000.0;
    auto throughputTuples = MTuples / elapsedSec;
    auto throughputMBytes = ((16ul * MTuples * 1'000'000.0) / elapsedSec) / (1024ul * 1024ul);
    NES_INFO("Processed: " << MTuples << " MTuples :: " << (16ul * MTuples * 1'000'000.0 / (1024ul * 1024ul)) << " MB :: "
                           << elapsedSec << " s :: " << throughputTuples << " MTuple/sec :: " << throughputMBytes << " MB/sec");

    NES_ASSERT(engine->undeployQuery(executionPlan->getQueryId()), "Cannot undeploy query");
    NES_ASSERT(engine->stop(), "Cannot stop query");

    //    auto query = Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000))));
    //
    //    TestHarness testHarness = TestHarness(query, restPort, rpcPort)
    //                                  .addLogicalSource("window1", windowSchema)
    //                                  .addLogicalSource("window2", window2Schema)
    //                                  .attachWorkerWithCSVSourceToCoordinator("window1", csvSourceType1)
    //                                  .attachWorkerWithCSVSourceToCoordinator("window1", csvSourceType1)
    //                                  .attachWorkerWithCSVSourceToCoordinator("window2", csvSourceType2)
    //                                  .attachWorkerWithCSVSourceToCoordinator("window2", csvSourceType2)
    //                                  .validate()
    //                                  .setupTopology();

    return 0;
}
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

#ifndef TESTSOURCE_HPP
#define TESTSOURCE_HPP
#include <variant>
namespace NES::Unikernel {

// helper type for the visitor #4
template<class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
// explicit deduction guide (not needed as of C++20)
template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

template<typename Config>
struct TestSourceImpl final {
    using QueueValueType = std::variant<TupleBufferHolder, bool>;
    using SPSCQueue = folly::DynamicBoundedQueue<QueueValueType, true, true, true>;
    using BufferType = SchemaBuffer<typename Config::Schema, 8192>;
    constexpr static NES::SourceType SourceType = NES::SourceType::TEST_SOURCE;
    constexpr static bool NeedsBuffer = Config::CopyBuffer;
    SPSCQueue queue{100};
    std::atomic_bool opened = false;
    std::atomic_bool closed = false;
    std::atomic_bool stop_request = false;

    TestSourceImpl();

    void emit(NES::Runtime::TupleBuffer& tb) { queue.enqueue(tb); }

    void eof() { queue.enqueue(true); }

    bool open() {
        NES_ASSERT(!opened.exchange(true), "Open called multiple times");
        return true;
    }

    bool close(Runtime::QueryTerminationType) {
        NES_ASSERT(!closed.exchange(true), "Close called multiple times");
        return true;
    }

    std::optional<NES::Runtime::TupleBuffer> receiveData(const std::stop_token& stoken, BufferType buffer) {
        QueueValueType item;
        while (!(queue.try_dequeue_for(item, 20ms) || stoken.stop_requested())) {
        }

        if (stoken.stop_requested()) {
            NES_ASSERT(!stop_request.exchange(true), "stop was requested multiple times");
            return std::nullopt;
        }

        if constexpr (Config::CopyBuffer) {
            return std::visit(overloaded{[&buffer](const TupleBufferHolder& tb) -> std::optional<NES::Runtime::TupleBuffer> {
                                             NES_ASSERT(tb.bufferToHold.getBufferSize() == 8192, "Expected Buffer Size");
                                             std::memcpy(buffer.getBuffer().getBuffer(), tb.bufferToHold.getBuffer(), 8192);
                                             return buffer.getBuffer();
                                         },
                                         [](const bool& /*bool*/) -> std::optional<NES::Runtime::TupleBuffer> {
                                             return std::nullopt;
                                         }},
                              item);
        } else {
            return std::visit(overloaded{[](const TupleBufferHolder& tb) -> std::optional<NES::Runtime::TupleBuffer> {
                                             return tb.bufferToHold;
                                         },
                                         [](const bool& /*bool*/) -> std::optional<NES::Runtime::TupleBuffer> {
                                             return std::nullopt;
                                         }},
                              item);
        }
    }
};

template<typename Config>
TestSourceImpl<Config>* SourceHandle = nullptr;

template<typename Config>
TestSourceImpl<Config>::TestSourceImpl() {
    SourceHandle<Config> = this;
}

struct TestSinkImpl {
    explicit TestSinkImpl(OperatorId operator_id)
        : operatorId(operator_id), tuples_received(0), setup_time(std::chrono::high_resolution_clock::now()) {}
    OperatorId operatorId;
    std::atomic<size_t> tuples_received;
    std::chrono::high_resolution_clock::time_point setup_time;
    std::chrono::high_resolution_clock::time_point first_tuple_time;
    std::atomic<std::chrono::high_resolution_clock::time_point> last_tuple_time;
    void writeData(NES::Runtime::TupleBuffer& buffer, NES::Runtime::WorkerContext&) {
        auto expected = last_tuple_time.load();
        while (!last_tuple_time.compare_exchange_weak(expected,
                                                      std::chrono::high_resolution_clock::now(),
                                                      std::memory_order_release,
                                                      std::memory_order_relaxed))
            ;
        auto t = tuples_received.fetch_add(buffer.getNumberOfTuples());
        if (t == 0)
            first_tuple_time = last_tuple_time.load();
    }

    void shutdown() {
        std::chrono::duration<double> duration = last_tuple_time.load() - first_tuple_time;
        NES_INFO("TPS: {}",
                 static_cast<double>(tuples_received.load())
                     / std::chrono::duration_cast<std::chrono::seconds>(duration).count());
    }
};
}// namespace NES::Unikernel

#endif//TESTSOURCE_HPP

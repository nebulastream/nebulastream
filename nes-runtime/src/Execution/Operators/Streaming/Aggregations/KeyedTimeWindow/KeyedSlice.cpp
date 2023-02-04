#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>

namespace NES::Runtime::Execution::Operators {

KeyedSlice::KeyedSlice(std::unique_ptr<Nautilus::Interface::ChainedHashMap> hashMap, uint64_t start, uint64_t end)
    : start(start), end(end), state(std::move(hashMap)) {}

}// namespace NES::Runtime::Execution::Operators
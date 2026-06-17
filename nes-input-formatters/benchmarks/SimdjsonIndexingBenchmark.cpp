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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>

#include <simdjson.h>

/// Reference benchmark: how fast does the vendored simdjson (the mature, gold-standard portable SIMD
/// parser NES already uses for JSON) process equivalent data on this machine, measured the same way as
/// the CSV indexer bench (same container, -O3, GB/s, whole buffer vs engine-sized chunks).
///
/// NOTE: this is NOT identical work to CSV delimiter-indexing. simdjson here does a FULL on-demand
/// parse (structural indexing + object navigation + integer value materialization) of NDJSON, which is
/// strictly more work than finding CSV field/row delimiters. It is a "what does the best portable SIMD
/// parser achieve" reference, not an apples-to-apples CSV number.
/// Build:  cmake --build {build} --target simdjson-indexing-benchmark
namespace
{
constexpr uint64_t numFields = 5;

/// ~64 MiB of NDJSON with the same logical data as the CSV bench: one object per line, 5 integer fields.
std::string makeNdjson()
{
    std::string json;
    json.reserve(size_t{96} << 20);
    uint32_t counter = 0;
    char line[160];
    while (json.size() < (size_t{64} << 20))
    {
        const int len = std::snprintf(
            line,
            sizeof(line),
            R"({"f0":%u,"f1":%u,"f2":%u,"f3":%u,"f4":%u})"
            "\n",
            counter % 1000000U,
            (counter * 2U) % 1000000U,
            (counter * 3U) % 1000000U,
            (counter * 4U) % 1000000U,
            (counter * 5U) % 1000000U);
        json.append(line, static_cast<size_t>(len));
        ++counter;
    }
    return json;
}

/// On-demand structural pass over one batch covering [data, data+len): iterate_many forces simdjson's
/// stage-1 structural indexing over the whole batch; advancing the stream drives it across all bytes.
/// We count documents (closest analog to CSV structural delimiter-indexing) rather than materializing
/// values, which is also how NES's indexer uses simdjson (it reads raw_json tokens, not get_uint64).
uint64_t parseBatch(simdjson::ondemand::parser& parser, const char* data, size_t len, size_t capacity)
{
    uint64_t count = 0;
    const simdjson::padded_string_view view{data, len, capacity};
    simdjson::ondemand::document_stream stream = parser.iterate_many(view, len + simdjson::SIMDJSON_PADDING);
    for (auto doc : stream)
    {
        (void)doc;
        ++count;
    }
    return count;
}

template <typename Fn>
double measure(Fn&& fn, const std::string& json, const int iterations, uint64_t& checksum)
{
    checksum += fn(); /// warm up
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i)
    {
        checksum += fn();
    }
    const auto end = std::chrono::steady_clock::now();
    const auto seconds = std::chrono::duration<double>(end - start).count();
    return static_cast<double>(json.size()) * iterations / seconds / 1e9;
}
}

int main()
{
    const std::string json = makeNdjson();
    const simdjson::padded_string padded{json}; /// copies + pads by SIMDJSON_PADDING
    const char* base = padded.data();
    const size_t total = json.size();
    const size_t capacity = total + simdjson::SIMDJSON_PADDING;
    constexpr int iterations = 20;
    uint64_t checksum = 0;

    simdjson::ondemand::parser wholeParser;
    const auto whole = [&] { return parseBatch(wholeParser, base, total, capacity); };

    /// Engine-like: re-parse each fixed-size chunk independently (mirrors indexing one TupleBuffer).
    const auto chunked = [&](size_t chunkSize)
    {
        return [&, chunkSize]
        {
            simdjson::ondemand::parser parser;
            uint64_t sum = 0;
            for (size_t off = 0; off < total; off += chunkSize)
            {
                const size_t len = std::min(chunkSize, total - off);
                sum += parseBatch(parser, base + off, len, capacity - off);
            }
            return sum;
        };
    };

    const double wholeGbps = measure(whole, json, iterations, checksum);
    const double gbps64k = measure(chunked(65536), json, iterations, checksum);
    const double gbps4k = measure(chunked(4096), json, iterations, checksum);

    std::printf(
        "simdjson %s on-demand structural pass / stage1 (%zu MiB NDJSON, %d iterations, %d fields/obj)\n",
        SIMDJSON_VERSION,
        total >> 20,
        iterations,
        static_cast<int>(numFields));
    std::printf("  %-26s %8.2f GB/s\n", "whole buffer (1 MiB batches)", wholeGbps);
    std::printf("  %-26s %8.2f GB/s\n", "64 KiB chunks", gbps64k);
    std::printf("  %-26s %8.2f GB/s   (engine default buffer size)\n", "4 KiB chunks", gbps4k);
    std::printf("active implementation: %s\n", simdjson::get_active_implementation()->name().c_str());
    std::printf("(checksum %llu)\n", static_cast<unsigned long long>(checksum));
    return 0;
}

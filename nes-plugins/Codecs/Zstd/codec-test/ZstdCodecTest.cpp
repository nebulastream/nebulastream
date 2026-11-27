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

#include <cstddef>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include <CodecHarness/Scenarios.hpp>
#include <ErrorHandling.hpp>
/// For ZSTD_writeSkippableFrame.
#define ZSTD_STATIC_LINKING_ONLY /// NOLINT(cppcoreguidelines-macro-usage)
#include <zstd.h>

namespace NES::CodecTest
{

/// The one-shot API, which shares no code path with the streaming ZstdDecoder.
std::vector<std::byte> referenceEncode(const std::span<const std::byte> input)
{
    const std::unique_ptr<ZSTD_CCtx, decltype(&ZSTD_freeCCtx)> context{ZSTD_createCCtx(), &ZSTD_freeCCtx};
    /// The checksum lets the decoder detect corrupted data that still parses as a frame.
    if (context == nullptr || ZSTD_isError(ZSTD_CCtx_setParameter(context.get(), ZSTD_c_checksumFlag, 1)) != 0U)
    {
        throw TestException("Cannot set up the zstd reference encoder");
    }
    std::vector<std::byte> frame(ZSTD_compressBound(input.size()));
    const auto size = ZSTD_compress2(context.get(), frame.data(), frame.size(), input.data(), input.size());
    if (ZSTD_isError(size) != 0U)
    {
        throw TestException("zstd reference encoding failed: {}", ZSTD_getErrorName(size));
    }
    frame.resize(size);
    return frame;
}

std::vector<Scenario> scenarios()
{
    auto scenarios = decompressionScenarios();
    /// A skippable frame, e.g. producer metadata, decodes to nothing.
    scenarios.push_back(decodesStream(
        "AfterSkippableFrame",
        [](const std::span<const std::byte> input)
        {
            constexpr std::string_view metadata = "metadata";
            std::vector<std::byte> stream(ZSTD_SKIPPABLEHEADERSIZE + metadata.size());
            const auto size = ZSTD_writeSkippableFrame(stream.data(), stream.size(), metadata.data(), metadata.size(), 0);
            if (ZSTD_isError(size) != 0U)
            {
                throw TestException("Cannot write a zstd skippable frame: {}", ZSTD_getErrorName(size));
            }
            stream.resize(size);
            const auto encoded = referenceEncode(input);
            stream.insert(stream.end(), encoded.begin(), encoded.end());
            return stream;
        }));
    return scenarios;
}

}

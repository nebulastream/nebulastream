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
#include <span>
#include <vector>

#include <CodecHarness/Scenarios.hpp>
#include <ErrorHandling.hpp>
#include <lz4frame.h>

namespace NES::CodecTest
{

/// The one-shot frame API, which shares no code path with the streaming LZ4Decoder.
std::vector<std::byte> referenceEncode(const std::span<const std::byte> input)
{
    LZ4F_preferences_t preferences{};
    /// The checksums let the decoder detect corrupted data that still parses as a frame.
    preferences.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    preferences.frameInfo.blockChecksumFlag = LZ4F_blockChecksumEnabled;
    std::vector<std::byte> frame(LZ4F_compressFrameBound(input.size(), &preferences));
    const auto size = LZ4F_compressFrame(frame.data(), frame.size(), input.data(), input.size(), &preferences);
    if (LZ4F_isError(size) != 0U)
    {
        throw TestException("LZ4 reference encoding failed: {}", LZ4F_getErrorName(size));
    }
    frame.resize(size);
    return frame;
}

std::vector<Scenario> scenarios()
{
    return decompressionScenarios();
}

}

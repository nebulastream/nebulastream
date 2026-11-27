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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <ErrorHandling.hpp>

/// The codec harness tests one codec plugin's decoder through a lowered File source, as ConnTest tests a connector.
///
/// A plugin calls add_codec_test(PLUGIN <Name> LIBRARIES <libraries>) and defines in codec-test/<Name>CodecTest.cpp:
///
///     std::vector<std::byte> referenceEncode(std::span<const std::byte> input) { ... }
///     std::vector<Scenario> scenarios() { return decompressionScenarios(); }
///
/// Without a config, a scenario binds CREATE DECODER codec_under_test TYPE <Name>;
/// codecs with options add .nesql files with one CREATE DECODER statement to codec-test/configs/.
///
/// The inputs are uncompressed files of nes-systests/testdata plus generated edge cases.
/// No encoded data is stored: every encoded stream comes from referenceEncode at runtime.
///
/// The harness runs every scenario in six phases:
///  1. encodeStreams: reference-encode the inputs into test streams.
///  2. defineDecoderAndSource: bind CREATE DECODER, register a File source that references it, create the buffer pool.
///  3. lowerSource, per test stream: write the encoded file and lower the source with its decoder.
///  4. runSource: until end of stream, failure, or the stop the scenario requests.
///  5. compareOutput: check the source's output against the test stream.
///  6. releaseSource: release the source and remove the file, also after a failed comparison.

namespace NES::CodecTest
{

struct TestInput
{
    std::string name;
    std::vector<std::byte> bytes;
};

struct TestStream
{
    std::string name;
    std::vector<std::byte> encoded;
    std::vector<std::byte> expected;
};

enum class RunMode : uint8_t
{
    UntilEndOfStream,
    StopAfterFirstData,
};

struct SourceOutput
{
    std::vector<std::byte> bytes;
    bool endOfStream = false;
    bool timedOut = false;
    std::optional<ErrorCode> errorCode;
    std::optional<std::string> error;
    bool stoppedInTime = false;
};

/// Holds the output of one decode call at the smallest buffer size, since decode returns all of it at once.
constexpr size_t DEFAULT_POOL_MEMORY_IN_BYTES = 256UL * 1024 * 1024;

struct Scenario
{
    std::string name;
    /// A file in codec-test/configs/; empty for the generated default config.
    std::string config;
    std::function<std::vector<TestStream>(std::span<const TestInput>)> encodeStreams;
    std::function<void(const TestStream&, const SourceOutput&)> compareOutput;
    std::optional<uint32_t> fixedBufferSize = std::nullopt;
    size_t poolMemoryInBytes = DEFAULT_POOL_MEMORY_IN_BYTES;
    RunMode runMode = RunMode::UntilEndOfStream;
    std::optional<std::string> knownFailure = std::nullopt;
};

Scenario decodesReference(std::string config = "");

Scenario damagedStreamFails(ErrorCode error, std::string config = "");

/// For codecs without integrity checks: a damaged stream fails or decodes to different bytes.
Scenario damagedStreamDecodesDifferently(std::string config = "");

/// For codecs whose encoded streams may follow each other, e.g. compression frames.
Scenario decodesConcatenatedEncodings(std::string config = "");

/// Every test input, encoded by the plugin's function, e.g. with bytes the reference never writes.
Scenario decodesStream(std::string name, std::function<std::vector<std::byte>(std::span<const std::byte>)> encode, std::string config = "");

Scenario stopWhileDecoding(std::string config = "");

/// TODO #2188: skipped as a known failure until the buffer passing between decoders is decided.
Scenario poolTooSmallForOneDecodeCall(std::string config = "");

/// The scenarios of a decompression codec whose reference encoding has an integrity check.
std::vector<Scenario> decompressionScenarios(const std::string& config = "");

/// Defined by the plugin with an implementation that shares no code with the decoder, e.g. the library's one-shot API.
std::vector<std::byte> referenceEncode(std::span<const std::byte> input);

/// Defined by the plugin.
std::vector<Scenario> scenarios();

}

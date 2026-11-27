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

/// Runs the scenarios of one codec plugin's decoder in the phases that CodecHarness/Scenarios.hpp describes.

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <random>
#include <ranges>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <unistd.h>
#include <CodecHarness/Scenarios.hpp>
#include <Codecs/Decoder.hpp>
#include <Codecs/DecoderCatalog.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Schema/Schema.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
using CodecTest::RunMode;
using CodecTest::Scenario;
using CodecTest::SourceOutput;
using CodecTest::TestInput;
using CodecTest::TestStream;

constexpr auto DECODING_TIMEOUT = std::chrono::seconds(60);
constexpr size_t INFLIGHT_BUFFER_LIMIT = 64;
/// Small buffers make even small inputs cross many buffer boundaries.
constexpr std::array<uint32_t, 2> BUFFER_SIZES{64, 4096};
/// The size of a generated input whose output ends exactly at a full buffer.
constexpr size_t MULTIPLE_OF_EVERY_BUFFER_SIZE = 8UL * 4096;
static_assert(std::ranges::all_of(BUFFER_SIZES, [](const uint32_t size) { return MULTIPLE_OF_EVERY_BUFFER_SIZE % size == 0; }));

std::string environment(const char* name)
{
    const char* value = std::getenv(name); /// NOLINT(concurrency-mt-unsafe)
    if (value == nullptr)
    {
        throw InvalidConfigParameter("{} is not set. ctest sets it per codec plugin, see add_codec_test.", name);
    }
    return value;
}

std::vector<std::byte> readFile(const std::filesystem::path& path)
{
    std::ifstream file(path, std::ios::binary);
    if (not file)
    {
        throw InvalidConfigParameter("Cannot open codec harness test file {}", path.string());
    }
    std::vector<char> content{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
    std::vector<std::byte> bytes(content.size());
    std::ranges::transform(content, bytes.begin(), [](const char value) { return static_cast<std::byte>(value); });
    return bytes;
}

std::vector<std::byte> randomBytes(const size_t size, const uint64_t seed)
{
    /// NOLINTNEXTLINE(bugprone-random-generator-seed) deterministic test input
    std::mt19937_64 random{seed};
    std::vector<std::byte> bytes(size);
    std::ranges::generate(bytes, [&random] { return static_cast<std::byte>(random()); });
    return bytes;
}

std::vector<TestInput> testInputs(const std::string& commaSeparatedFiles)
{
    std::vector<TestInput> inputs;
    inputs.push_back({.name = "Empty", .bytes = {}});
    inputs.push_back({.name = "SingleByte", .bytes = {std::byte{'x'}}});
    std::vector<std::byte> allByteValues;
    for (size_t repetition = 0; repetition < 64; ++repetition)
    {
        for (size_t value = 0; value < 256; ++value)
        {
            allByteValues.push_back(static_cast<std::byte>(value));
        }
    }
    inputs.push_back({.name = "AllByteValues", .bytes = std::move(allByteValues)});
    /// Compresses extremely well: few encoded bytes expand to many output buffers.
    inputs.push_back({.name = "FourMiBOfOneByte", .bytes = std::vector<std::byte>(4UL * 1024 * 1024, std::byte{'a'})});
    /// Does not compress: the encoded stream is larger than the input.
    inputs.push_back({.name = "RandomBytes", .bytes = randomBytes(256UL * 1024, 42)});
    std::vector<std::byte> bufferMultiple;
    for (size_t line = 0; bufferMultiple.size() < MULTIPLE_OF_EVERY_BUFFER_SIZE; ++line)
    {
        const auto text = fmt::format("{},{},sensor_{}\n", line, line * 7919 % 1000003, line % 13);
        const auto bytes = std::as_bytes(std::span{text});
        bufferMultiple.insert(bufferMultiple.end(), bytes.begin(), bytes.end());
    }
    bufferMultiple.resize(MULTIPLE_OF_EVERY_BUFFER_SIZE);
    inputs.push_back({.name = "MultipleOfEveryBufferSize", .bytes = std::move(bufferMultiple)});
    /// In the plugin's order: the last one is the input of the damaged streams.
    for (const auto file : std::views::split(commaSeparatedFiles, ','))
    {
        const std::filesystem::path path{std::string_view(file.begin(), file.end())};
        if (not path.empty())
        {
            inputs.push_back({.name = path.filename().string(), .bytes = readFile(path)});
        }
    }
    return inputs;
}

const std::vector<TestInput>& inputs()
{
    static const auto inputs = testInputs(environment("CODECTEST_INPUTS"));
    return inputs;
}

std::string formatBytesAround(const std::span<const std::byte> bytes, const size_t position)
{
    constexpr size_t CONTEXT = 16;
    const auto begin = position - std::min(position, CONTEXT);
    const auto end = std::min(bytes.size(), position + CONTEXT);
    std::string hex;
    std::string text;
    for (size_t i = begin; i < end; ++i)
    {
        const auto value = static_cast<unsigned char>(bytes[i]);
        std::string_view separator = " ";
        if (i == position)
        {
            separator = "[";
        }
        else if (i == begin)
        {
            separator = "";
        }
        hex += fmt::format("{}{:02x}", separator, value);
        if (i == position)
        {
            hex += "]";
        }
        text += (value >= 0x20 && value < 0x7f) ? static_cast<char>(value) : '.';
    }
    return fmt::format("bytes {}..{}: {} |{}|", begin, end, hex, text);
}

::testing::AssertionResult sameBytes(const std::span<const std::byte> expected, const std::span<const std::byte> decoded)
{
    const auto [expectedEnd, decodedEnd] = std::ranges::mismatch(expected, decoded);
    if (expectedEnd == expected.end() && decodedEnd == decoded.end())
    {
        return ::testing::AssertionSuccess();
    }
    const auto position = static_cast<size_t>(std::distance(expected.begin(), expectedEnd));
    auto failure = ::testing::AssertionFailure();
    failure << fmt::format("decoded {} bytes, expected {}. ", decoded.size(), expected.size());
    if (position == std::min(expected.size(), decoded.size()))
    {
        failure << fmt::format(
            "The decoded bytes are a {} of the expected ones.", decoded.size() < expected.size() ? "prefix" : "extension");
    }
    else
    {
        failure << fmt::format("First difference at byte {}.", position);
    }
    failure << fmt::format("\n  expected {}\n  decoded  {}", formatBytesAround(expected, position), formatBytesAround(decoded, position));
    return failure;
}

std::vector<TestStream>
encodeEach(const std::span<const TestInput> testInputs, const std::function<std::vector<std::byte>(std::span<const std::byte>)>& encode)
{
    std::vector<TestStream> streams;
    for (const auto& input : testInputs)
    {
        streams.push_back({.name = input.name, .encoded = encode(input.bytes), .expected = input.bytes});
    }
    return streams;
}

/// The reference encoding of the test input with the given name.
auto referenceEncodingOf(std::string name)
{
    return [name = std::move(name)](const std::span<const TestInput> testInputs)
    {
        const auto input = std::ranges::find(testInputs, name, &TestInput::name);
        if (input == testInputs.end())
        {
            throw TestException("The codec harness has no test input {}", name);
        }
        return encodeEach(std::span<const TestInput>{&*input, 1}, CodecTest::referenceEncode);
    };
}

struct DamagedStream
{
    std::string name;
    std::vector<std::byte> bytes;
};

std::vector<DamagedStream> damagedStreams(const std::vector<std::byte>& encoded)
{
    std::vector<DamagedStream> streams;
    const auto truncated = [&encoded](const size_t size)
    { return std::vector<std::byte>(encoded.begin(), encoded.begin() + static_cast<std::ptrdiff_t>(size)); };
    streams.push_back({.name = "TruncatedAfterOneByte", .bytes = truncated(1)});
    streams.push_back({.name = "TruncatedInTheMiddle", .bytes = truncated(encoded.size() / 2)});
    streams.push_back({.name = "WithoutTheLastByte", .bytes = truncated(encoded.size() - 1)});
    for (const auto position : {size_t{0}, encoded.size() / 3, encoded.size() / 2, (encoded.size() * 2) / 3, encoded.size() - 1})
    {
        auto flipped = encoded;
        flipped[position] ^= std::byte{0xFF};
        streams.push_back({.name = fmt::format("ByteFlippedAt{}", position), .bytes = std::move(flipped)});
    }
    streams.push_back({.name = "RandomBytes", .bytes = randomBytes(64UL * 1024, 7)});
    return streams;
}

std::vector<TestStream> encodeDamaged(const std::span<const TestInput> testInputs)
{
    const auto& original = testInputs.back();
    std::vector<TestStream> streams;
    for (auto& damaged : damagedStreams(CodecTest::referenceEncode(original.bytes)))
    {
        streams.push_back(
            {.name = fmt::format("{}_{}", original.name, damaged.name), .encoded = std::move(damaged.bytes), .expected = original.bytes});
    }
    return streams;
}

struct BoundConfig
{
    std::shared_ptr<DecoderCatalog> decoders;
    Identifier decoderName;
};

BoundConfig bindConfig(const std::string& config)
{
    std::string sql = fmt::format("CREATE DECODER codec_under_test TYPE {}", environment("CODECTEST_CODEC"));
    if (not config.empty())
    {
        const auto bytes = readFile(std::filesystem::path(environment("CODECTEST_CONFIGS_DIR")) / config);
        sql.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size()); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    }
    auto statements = AntlrSQLQueryParser::ManagedAntlrParser::create(sql)->parseMultiple();
    if (not statements.has_value())
    {
        throw std::move(statements).error();
    }
    const StatementBinder binder(
        std::make_shared<SourceCatalog>(),
        [](auto&& query) { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(query)>(query)); });
    auto decoders = std::make_shared<DecoderCatalog>();
    std::optional<Identifier> decoderName;
    for (const auto& statement : *statements)
    {
        auto bound = binder.bind(statement.get());
        if (not bound.has_value())
        {
            throw std::move(bound).error();
        }
        const auto* createDecoder = std::get_if<CreateDecoderStatement>(&*bound);
        if (createDecoder == nullptr || decoderName.has_value())
        {
            throw InvalidConfigParameter("The codec harness config {} must contain exactly one CREATE DECODER statement", config);
        }
        if (auto added = decoders->addDecoder(createDecoder->name, createDecoder->decoderType, createDecoder->decoderConfig);
            not added.has_value())
        {
            throw std::move(added).error();
        }
        decoderName = createDecoder->name;
    }
    if (not decoderName.has_value())
    {
        throw InvalidConfigParameter("The codec harness config {} contains no CREATE DECODER statement", config);
    }
    return {.decoders = std::move(decoders), .decoderName = *decoderName};
}

struct DefinedSource
{
    SourceDescriptor sourceDescriptor;
    std::filesystem::path encodedFile;
    std::shared_ptr<BufferManager> bufferPool;
};

DefinedSource defineDecoderAndSource(const Scenario& scenario, const uint32_t bufferSize)
{
    const auto [decoders, decoderName] = bindConfig(scenario.config);
    auto encodedFile
        = std::filesystem::temp_directory_path() / fmt::format("codec-harness-{}-{}", environment("CODECTEST_CODEC"), getpid());
    SourceCatalog catalog{decoders};
    const auto logicalSource = catalog.addLogicalSource(
        Identifier::parse("input"),
        Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::INT32}});
    if (not logicalSource.has_value())
    {
        throw TestException("Cannot create the logical source of the codec harness");
    }
    auto sourceDescriptor = catalog.addPhysicalSource(
        *logicalSource,
        Identifier::parse("FILE"),
        Host{"localhost:8080"},
        {{Identifier::parse("FILE_PATH"), fmt::format("'{}'", encodedFile.string())},
         {Identifier::parse("DECODERS"), fmt::format("'{}'", decoderName.getOriginalString())}},
        {{Identifier::parse("TYPE"), "CSV"}});
    if (not sourceDescriptor.has_value())
    {
        throw std::move(sourceDescriptor).error();
    }
    return {
        .sourceDescriptor = *std::move(sourceDescriptor),
        .encodedFile = std::move(encodedFile),
        .bufferPool = BufferManager::create(
            scenario.poolMemoryInBytes, 0.1, BufferAlignment{64}, bufferSize, std::make_shared<NesDefaultMemoryAllocator>())};
}

struct Collected
{
    std::mutex mutex;
    SourceOutput output;
    std::promise<void> terminated;
    bool hasTerminated = false;
    std::promise<void> firstData;
    bool hasData = false;
};

/// The source handle is declared last, so that it is destroyed first: its thread uses the other members.
struct LoweredSource
{
    BackpressureController backpressureController;
    std::unique_ptr<Collected> collected = std::make_unique<Collected>();
    std::unique_ptr<SourceHandle> sourceHandle = nullptr;
};

void writeFile(const std::filesystem::path& path, const std::vector<std::byte>& bytes)
{
    std::ofstream file(path, std::ios::binary);
    file.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size())); /// NOLINT
    if (not file)
    {
        throw TestException("Cannot write the encoded file {}", path.string());
    }
}

std::unique_ptr<LoweredSource> lowerSource(const DefinedSource& definedSource, const TestStream& stream)
{
    writeFile(definedSource.encodedFile, stream.encoded);
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    auto lowered = std::make_unique<LoweredSource>(LoweredSource{.backpressureController = std::move(backpressureController)});
    lowered->sourceHandle = SourceProvider(INFLIGHT_BUFFER_LIMIT, definedSource.bufferPool)
                                .lower(INITIAL<OriginId>, std::move(backpressureListener), definedSource.sourceDescriptor);
    return lowered;
}

auto collectInto(Collected& collected)
{
    return [&collected](OriginId, SourceReturnType::SourceReturnType emitted, const std::stop_token&)
    {
        const std::scoped_lock lock(collected.mutex);
        const auto terminate = [&collected]
        {
            if (not std::exchange(collected.hasTerminated, true))
            {
                collected.terminated.set_value();
            }
        };
        if (auto* data = std::get_if<SourceReturnType::Data>(&emitted))
        {
            const auto bytes = data->buffer.getAvailableMemoryArea<std::byte>().first(data->buffer.getNumberOfTuples());
            collected.output.bytes.insert(collected.output.bytes.end(), bytes.begin(), bytes.end());
            if (not std::exchange(collected.hasData, true))
            {
                collected.firstData.set_value();
            }
        }
        else if (auto* error = std::get_if<SourceReturnType::Error>(&emitted))
        {
            collected.output.errorCode = error->ex.code();
            collected.output.error = error->ex.what();
            terminate();
        }
        else
        {
            collected.output.endOfStream = true;
            terminate();
        }
        return SourceReturnType::EmitResult::SUCCESS;
    };
}

/// Returns once the source thread has stopped.
SourceOutput runSource(LoweredSource& lowered, const RunMode runMode)
{
    auto& collected = *lowered.collected;
    lowered.sourceHandle->start(collectInto(collected));
    auto& awaited = runMode == RunMode::UntilEndOfStream ? collected.terminated : collected.firstData;
    const bool arrived = awaited.get_future().wait_for(DECODING_TIMEOUT) == std::future_status::ready;
    bool stoppedInTime = true;
    if (arrived && runMode == RunMode::StopAfterFirstData)
    {
        auto stopped = std::async(std::launch::async, [&lowered] { lowered.sourceHandle->stop(); });
        stoppedInTime = stopped.wait_for(DECODING_TIMEOUT) == std::future_status::ready;
    }
    else
    {
        lowered.sourceHandle->stop();
    }
    const std::scoped_lock lock(collected.mutex);
    collected.output.timedOut = not arrived;
    collected.output.stoppedInTime = stoppedInTime;
    return collected.output;
}

void releaseSource(std::unique_ptr<LoweredSource> lowered, const DefinedSource& definedSource)
{
    lowered.reset();
    std::filesystem::remove(definedSource.encodedFile);
}

void runScenario(const Scenario& scenario, const uint32_t bufferSize)
{
    if (scenario.knownFailure.has_value())
    {
        GTEST_SKIP() << *scenario.knownFailure;
    }
    const auto trace = fmt::format("codec {}, buffer size {}", environment("CODECTEST_CODEC"), bufferSize);

    const auto streams = scenario.encodeStreams(inputs());
    ASSERT_FALSE(streams.empty()) << "The scenario encoded no test stream";

    const auto definedSource = defineDecoderAndSource(scenario, bufferSize);

    for (const auto& stream : streams)
    {
        SCOPED_TRACE(fmt::format("{}, stream {}", trace, stream.name));
        auto lowered = lowerSource(definedSource, stream);
        const auto output = runSource(*lowered, scenario.runMode);
        /// No stream may block the source.
        if (output.timedOut)
        {
            ADD_FAILURE() << "The source did not end within " << DECODING_TIMEOUT;
        }
        else
        {
            scenario.compareOutput(stream, output);
        }
        releaseSource(std::move(lowered), definedSource);
    }
}

void expectDecodes(const TestStream& stream, const SourceOutput& output)
{
    EXPECT_FALSE(output.error.has_value()) << "The source failed: " << output.error.value_or("");
    EXPECT_TRUE(output.endOfStream);
    EXPECT_TRUE(sameBytes(stream.expected, output.bytes));
}

}

namespace CodecTest
{

Scenario decodesReference(std::string config)
{
    return {
        .name = "DecodesReference",
        .config = std::move(config),
        .encodeStreams = [](const std::span<const TestInput> testInputs) { return encodeEach(testInputs, referenceEncode); },
        .compareOutput = expectDecodes};
}

Scenario damagedStreamFails(const ErrorCode error, std::string config)
{
    return {
        .name = "DamagedStreamFails",
        .config = std::move(config),
        .encodeStreams = encodeDamaged,
        .compareOutput = [error](const TestStream&, const SourceOutput& output)
        {
            ASSERT_TRUE(output.errorCode.has_value())
                << "The damaged stream decoded without an error to " << output.bytes.size() << " bytes";
            EXPECT_EQ(*output.errorCode, error) << output.error.value_or("");
        }};
}

Scenario damagedStreamDecodesDifferently(std::string config)
{
    return {
        .name = "DamagedStreamDecodesDifferently",
        .config = std::move(config),
        .encodeStreams = encodeDamaged,
        .compareOutput = [](const TestStream& stream, const SourceOutput& output)
        {
            if (not output.errorCode.has_value())
            {
                EXPECT_FALSE(sameBytes(stream.expected, output.bytes)) << "The damaged stream decoded to the original bytes";
            }
        }};
}

Scenario decodesConcatenatedEncodings(std::string config)
{
    return {
        .name = "DecodesConcatenatedEncodings",
        .config = std::move(config),
        .encodeStreams =
            [](const std::span<const TestInput> testInputs)
        {
            TestStream stream{.name = "AllInputs", .encoded = {}, .expected = {}};
            for (const auto& input : testInputs)
            {
                const auto encoded = referenceEncode(input.bytes);
                stream.encoded.insert(stream.encoded.end(), encoded.begin(), encoded.end());
                stream.expected.insert(stream.expected.end(), input.bytes.begin(), input.bytes.end());
            }
            return std::vector{std::move(stream)};
        },
        .compareOutput = expectDecodes};
}

Scenario decodesStream(std::string name, std::function<std::vector<std::byte>(std::span<const std::byte>)> encode, std::string config)
{
    return {
        .name = std::move(name),
        .config = std::move(config),
        .encodeStreams
        = [encode = std::move(encode)](const std::span<const TestInput> testInputs) { return encodeEach(testInputs, encode); },
        .compareOutput = expectDecodes};
}

Scenario stopWhileDecoding(std::string config)
{
    return {
        .name = "StopWhileDecoding",
        .config = std::move(config),
        .encodeStreams = referenceEncodingOf("RandomBytes"),
        .compareOutput
        = [](const TestStream&, const SourceOutput& output) { EXPECT_TRUE(output.stoppedInTime) << "Stopping the source did not end it"; },
        /// Small buffers, so that decoding takes many iterations of the source thread.
        .fixedBufferSize = 64,
        .runMode = RunMode::StopAfterFirstData};
}

Scenario poolTooSmallForOneDecodeCall(std::string config)
{
    return {
        .name = "PoolTooSmallForOneDecodeCall",
        .config = std::move(config),
        .encodeStreams = referenceEncodingOf("FourMiBOfOneByte"),
        .compareOutput = expectDecodes,
        .fixedBufferSize = 4096,
        /// Smaller than the output of one decode call, so the decoder waits for buffers until the allocation timeout.
        .poolMemoryInBytes = 1024UL * 1024,
        .knownFailure = "Known failure until #2188 decides how buffers are passed on"};
}

std::vector<Scenario> decompressionScenarios(const std::string& config)
{
    return {
        decodesReference(config),
        damagedStreamFails(ErrorCode::CannotDecodeSourceData, config),
        decodesConcatenatedEncodings(config),
        stopWhileDecoding(config),
        poolTooSmallForOneDecodeCall(config),
    };
}

}

namespace
{

struct ScenarioCase
{
    CodecTest::Scenario scenario;
    uint32_t bufferSize;
};

std::vector<ScenarioCase> scenarioCases()
{
    std::vector<ScenarioCase> cases;
    for (const auto& scenario : CodecTest::scenarios())
    {
        if (scenario.fixedBufferSize.has_value())
        {
            cases.push_back({.scenario = scenario, .bufferSize = *scenario.fixedBufferSize});
            continue;
        }
        for (const auto bufferSize : BUFFER_SIZES)
        {
            cases.push_back({.scenario = scenario, .bufferSize = bufferSize});
        }
    }
    return cases;
}

std::string testName(std::string name)
{
    std::ranges::replace_if(name, [](const char character) { return std::isalnum(static_cast<unsigned char>(character)) == 0; }, '_');
    return name;
}

}

/// NOLINTNEXTLINE(misc-use-internal-linkage) gtest fixture
class CodecScenario : public Testing::BaseUnitTest, public ::testing::WithParamInterface<ScenarioCase>
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("CodecHarness.log", LogLevel::LOG_DEBUG); }
};

TEST_P(CodecScenario, Runs)
{
    runScenario(GetParam().scenario, GetParam().bufferSize);
}

INSTANTIATE_TEST_SUITE_P(
    Scenarios,
    CodecScenario,
    ::testing::ValuesIn(scenarioCases()),
    [](const auto& info)
    {
        /// No structured binding: its comma would split the macro argument.
        const auto& scenario = info.param.scenario;
        const auto config = scenario.config.empty() ? std::string{"default"} : std::filesystem::path(scenario.config).stem().string();
        if (scenario.fixedBufferSize.has_value())
        {
            return testName(fmt::format("{}_{}", scenario.name, config));
        }
        return testName(fmt::format("{}_{}_{}ByteBuffers", scenario.name, config, info.param.bufferSize));
    });

}

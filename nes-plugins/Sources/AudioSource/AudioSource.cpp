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

#include <AudioSource.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Ranges.hpp>
#include <alsa/error.h>
#include <alsa/pcm.h>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{
namespace
{
struct AudioTuple
{
    double sample;
    uint64_t timestamp;
};

static_assert(sizeof(AudioTuple) == 16);
static_assert(std::is_trivially_copyable_v<AudioTuple>);

struct AudioField
{
    std::string_view name;
    DataType::Type type;
    std::string_view typeName;
};

constexpr std::array audioFields{
    AudioField{.name = "SAMPLE", .type = DataType::Type::FLOAT64, .typeName = "FLOAT64"},
    AudioField{.name = "TIMESTAMP", .type = DataType::Type::UINT64, .typeName = "UINT64"},
};

constexpr size_t MAX_FRAMES_PER_READ = 1024;

/// A read whose capture time differs from where the sample timeline expects it by more than this is not a continuation
/// of that timeline: the stream restarted, samples were dropped (the ring buffer overran while the source was blocked),
/// or the device clock drifted away from the system clock. The timeline is then anchored again.
constexpr uint64_t TIMELINE_TOLERANCE_NS = 5'000'000;

constexpr uint64_t nanosecondsOf(const snd_htimestamp_t& timestamp)
{
    return (static_cast<uint64_t>(timestamp.tv_sec) * 1'000'000'000ULL) + static_cast<uint64_t>(timestamp.tv_nsec);
}

void validateSchema(const SourceDescriptor& sourceDescriptor)
{
    const auto schema = sourceDescriptor.getLogicalSource().getSchema();
    if (schema->size() != audioFields.size())
    {
        throw CannotOpenSource("Audio source expects {} non-nullable fields, but got {}", audioFields.size(), schema->size());
    }

    for (const auto [index, expected] : audioFields | views::enumerate)
    {
        const auto actual = (*schema)[index];
        INVARIANT(actual.has_value(), "Audio source schema field {} is missing", index);
        const auto& actualName = static_cast<const Identifier&>(actual->getFullyQualifiedName());
        if (actualName != Identifier::parse(std::string{expected.name}) || actual->getDataType().type != expected.type
            || actual->getDataType().nullable)
        {
            throw CannotOpenSource(
                "Audio source field {} must be non-nullable {} in tuple position {}, but got {}",
                expected.name,
                expected.typeName,
                index,
                actual->getFullyQualifiedName());
        }
    }
}

constexpr uint64_t timestampFor(uint64_t startTimestampNs, uint64_t sampleIndex, uint32_t sampleRate)
{
    return startTimestampNs + ((sampleIndex / sampleRate) * 1'000'000'000ULL)
        + (((sampleIndex % sampleRate) * 1'000'000'000ULL) / sampleRate);
}

static_assert(timestampFor(0, 48'000, 48'000) == 1'000'000'000ULL);
}

AudioSource::AudioSource(const SourceDescriptor& sourceDescriptor)
    : device(sourceDescriptor.getFromConfig(ConfigParametersAudio::DEVICE))
    , sampleRate(sourceDescriptor.getFromConfig(ConfigParametersAudio::SAMPLE_RATE))
    , realTimestamps(sourceDescriptor.getFromConfig(ConfigParametersAudio::REAL_TIMESTAMP))
{
    validateSchema(sourceDescriptor);
}

AudioSource::~AudioSource()
{
    close();
}

void AudioSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    const auto result = snd_pcm_open(&pcm, device.c_str(), SND_PCM_STREAM_CAPTURE, SND_PCM_NONBLOCK);
    if (result < 0)
    {
        throw CannotOpenSource("Could not open ALSA capture device '{}': {}", device, snd_strerror(result));
    }

    const auto setupResult = snd_pcm_set_params(pcm, SND_PCM_FORMAT_S16_LE, SND_PCM_ACCESS_RW_INTERLEAVED, 1, sampleRate, 1, 100'000);
    if (setupResult < 0)
    {
        close();
        throw CannotOpenSource("Could not configure ALSA capture device '{}': {}", device, snd_strerror(setupResult));
    }

    /// Let ALSA timestamp its ring buffer with the system (realtime) clock, the clock all other timestamps use.
    hardwareTimestamps = false;
    snd_pcm_sw_params_t* swParams = nullptr;
    snd_pcm_sw_params_alloca(&swParams);
    if (snd_pcm_sw_params_current(pcm, swParams) == 0 && snd_pcm_sw_params_set_tstamp_mode(pcm, swParams, SND_PCM_TSTAMP_ENABLE) == 0
        && snd_pcm_sw_params_set_tstamp_type(pcm, swParams, SND_PCM_TSTAMP_TYPE_GETTIMEOFDAY) == 0 && snd_pcm_sw_params(pcm, swParams) == 0)
    {
        hardwareTimestamps = true;
    }

    startTimestampNs = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    samplesCaptured = 0;
    timelineAnchored = false;
}

uint64_t AudioSource::captureTimeOfOldestUnreadFrameNs() const
{
    /// The newest frame was captured at the timestamp, and `available` frames precede it that have not been read yet.
    snd_pcm_uframes_t available = 0;
    snd_htimestamp_t timestamp{};
    if (hardwareTimestamps && snd_pcm_htimestamp(pcm, &available, &timestamp) == 0 && (timestamp.tv_sec != 0 || timestamp.tv_nsec != 0))
    {
        const uint64_t backlogNs = (static_cast<uint64_t>(available) * 1'000'000'000ULL) / sampleRate;
        const auto captured = nanosecondsOf(timestamp);
        return captured > backlogNs ? captured - backlogNs : captured;
    }

    /// No device timestamp: the frames queued right now were captured before the current time.
    const auto queued = snd_pcm_avail(pcm);
    const uint64_t backlogNs = queued > 0 ? (static_cast<uint64_t>(queued) * 1'000'000'000ULL) / sampleRate : 0;
    const uint64_t now
        = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return now > backlogNs ? now - backlogNs : now;
}

Source::FillTupleBufferResult AudioSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    PRECONDITION(pcm, "Audio source was not opened");
    const auto tupleCapacity = tupleBuffer.getBufferSize() / sizeof(AudioTuple);
    PRECONDITION(tupleCapacity > 0, "Audio tuple does not fit into tuple buffer");

    size_t tuplesWritten = 0;
    auto output = tupleBuffer.getAvailableMemoryArea();
    while (tuplesWritten < tupleCapacity && !stopToken.stop_requested())
    {
        const auto framesToRead = std::min(tupleCapacity - tuplesWritten, MAX_FRAMES_PER_READ);
        samples.resize(framesToRead);
        /// The read starts at the oldest unread frame, so its capture time is the one to stamp it with, however long the
        /// source was blocked before this call. The continuous sample timeline is only re-anchored when this capture time
        /// disagrees with it.
        uint64_t captureTimeNs = 0;
        if (realTimestamps)
        {
            captureTimeNs = captureTimeOfOldestUnreadFrameNs();
        }
        const auto framesRead = snd_pcm_readi(pcm, samples.data(), framesToRead);
        if (framesRead == -EAGAIN)
        {
            const auto waitResult = snd_pcm_wait(pcm, 100);
            if (waitResult < 0 && waitResult != -EAGAIN && snd_pcm_recover(pcm, waitResult, 1) < 0)
            {
                throw CannotOpenSource("ALSA capture device '{}' stopped: {}", device, snd_strerror(waitResult));
            }
            continue;
        }
        if (framesRead < 0)
        {
            if (snd_pcm_recover(pcm, static_cast<int>(framesRead), 1) < 0)
            {
                throw CannotOpenSource("Could not read from ALSA capture device '{}': {}", device, snd_strerror(framesRead));
            }
            /// Recovering restarts the stream, so the next frames do not continue the sample timeline.
            timelineAnchored = false;
            continue;
        }

        if (realTimestamps)
        {
            const auto expectedNs = timestampFor(startTimestampNs, samplesCaptured, sampleRate);
            const auto deviationNs = captureTimeNs > expectedNs ? captureTimeNs - expectedNs : expectedNs - captureTimeNs;
            if (!timelineAnchored || deviationNs > TIMELINE_TOLERANCE_NS)
            {
                startTimestampNs = captureTimeNs;
                samplesCaptured = 0;
                timelineAnchored = true;
            }
        }

        for (snd_pcm_sframes_t index = 0; index < framesRead; ++index)
        {
            const AudioTuple tuple{
                .sample = static_cast<double>(samples[index]) / 32'768.0,
                .timestamp = timestampFor(startTimestampNs, samplesCaptured++, sampleRate)};
            std::memcpy(output.data() + (tuplesWritten++ * sizeof(AudioTuple)), &tuple, sizeof(tuple));
        }
    }

    /// Native sources report tuples rather than raw bytes; no input formatter follows this source.
    return stopToken.stop_requested() ? FillTupleBufferResult::eos() : FillTupleBufferResult::withBytes(tuplesWritten);
}

void AudioSource::close()
{
    if (pcm != nullptr)
    {
        snd_pcm_close(pcm);
        pcm = nullptr;
    }
}

DescriptorConfig::Config AudioSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersAudio>(std::move(config), NAME);
}

std::ostream& AudioSource::toString(std::ostream& stream) const
{
    return stream << "AudioSource(device=" << device << ", sampleRate=" << sampleRate << ')';
}


}

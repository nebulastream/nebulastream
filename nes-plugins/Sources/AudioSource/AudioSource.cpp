/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
*/

#include <AudioSource.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <Util/Ranges.hpp>

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
    AudioField{"SAMPLE", DataType::Type::FLOAT64, "FLOAT64"},
    AudioField{"TIMESTAMP", DataType::Type::UINT64, "UINT64"},
};

constexpr size_t MAX_FRAMES_PER_READ = 1024;

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
        + ((sampleIndex % sampleRate) * 1'000'000'000ULL) / sampleRate;
}

static_assert(timestampFor(0, 48'000, 48'000) == 1'000'000'000ULL);
}

AudioSource::AudioSource(const SourceDescriptor& sourceDescriptor)
    : device(sourceDescriptor.getFromConfig(ConfigParametersAudio::DEVICE))
    , sampleRate(sourceDescriptor.getFromConfig(ConfigParametersAudio::SAMPLE_RATE))
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

    startTimestampNs = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    samplesCaptured = 0;
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
            continue;
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
    if (pcm)
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

SourceValidationRegistryReturnType RegisterAudioSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return AudioSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterAudioSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<AudioSource>(sourceRegistryArguments.sourceDescriptor);
}
}

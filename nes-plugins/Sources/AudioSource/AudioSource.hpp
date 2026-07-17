/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
*/

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <alsa/asoundlib.h>

namespace NES
{

struct ConfigParametersAudio
{
    static inline const DescriptorConfig::ConfigParameter<std::string> DEVICE{
        "DEVICE",
        "default",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(DEVICE, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> SAMPLE_RATE{
        "SAMPLE_RATE",
        48'000,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto sampleRate = DescriptorConfig::tryGet(SAMPLE_RATE, config);
            return sampleRate && *sampleRate > 0 ? sampleRate : std::nullopt;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, DEVICE, SAMPLE_RATE);
};

/// Captures mono signed 16-bit PCM through ALSA and produces (SAMPLE FLOAT64, TIMESTAMP UINT64) tuples.
class AudioSource final : public Source
{
public:
    static constexpr std::string_view NAME = "AUDIO";

    explicit AudioSource(const SourceDescriptor& sourceDescriptor);
    ~AudioSource() override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& stream) const override;

private:
    std::string device;
    uint32_t sampleRate;
    uint64_t startTimestampNs = 0;
    uint64_t samplesCaptured = 0;
    snd_pcm_t* pcm = nullptr;
    std::vector<int16_t> samples;
};

}

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

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <alsa/asoundlib.h> ///NOLINT(misc-include-cleaner)
#include <alsa/pcm.h>
#include <ErrorHandling.hpp>

namespace NES
{

struct AudioSourceConfig
{
    bool realTimestamp;
    std::string device;
    uint32_t sampleRate;

    static std::expected<AudioSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Captures mono signed 16-bit PCM through ALSA and produces (SAMPLE FLOAT64, TIMESTAMP UINT64) tuples.
class AudioSource final : public Source
{
public:
    static constexpr std::string_view NAME = "AUDIO";

    AudioSource(const AudioSourceConfig& config, const SourceDescriptor& sourceDescriptor);
    ~AudioSource() override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void close() override;

    [[nodiscard]] std::string_view getType() const override { return NAME; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& stream) const override;

private:
    std::string device;
    uint32_t sampleRate;
    uint64_t startTimestampNs = 0;
    uint64_t samplesCaptured = 0;
    snd_pcm_t* pcm = nullptr;

    /// Capture time of the oldest frame that is still unread in the ALSA ring buffer.
    [[nodiscard]] uint64_t captureTimeOfOldestUnreadFrameNs() const;
    std::vector<int16_t> samples;
    /// REAL_TIMESTAMP: stamp samples with the time the device captured them (ALSA hardware timestamps) instead of a
    /// timeline that starts when the device was opened.
    bool realTimestamps = false;
    /// The device delivers hardware timestamps (otherwise the capture time is estimated from the current time).
    bool hardwareTimestamps = false;
    /// The sample timeline (startTimestampNs, samplesCaptured) is anchored to a capture time. Cleared whenever the
    /// stream restarts, so that the next read anchors it again.
    bool timelineAnchored = false;
};

}

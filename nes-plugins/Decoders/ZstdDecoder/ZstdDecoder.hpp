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
#include <expected>
#include <span>
#include <string_view>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Decoders/DecoderStage.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <ErrorHandling.hpp>

using ZSTD_DCtx = struct ZSTD_DCtx_s;

namespace NES
{

/// Zstd has no options yet; the struct exists for the config registries.
struct ZstdDecoderConfig
{
    static std::expected<ZstdDecoderConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Decompresses the Zstandard frame format (what the zstd CLI and ZSTD_compressStream* produce), incrementally:
/// a frame may span any number of input buffers, and several frames (also skippable frames) may follow each other.
///
/// Output is written into buffers from the provider; a buffer is passed on when it is full or when the current
/// input is used up. Corrupt input and a stream that ends inside a frame raise CannotDecodeSourceData.
class ZstdDecoder final : public DecoderStage
{
public:
    static constexpr std::string_view NAME = "ZSTD";

    explicit ZstdDecoder(const ZstdDecoderConfig& config);
    ~ZstdDecoder() override;
    ZstdDecoder(const ZstdDecoder&) = delete;
    ZstdDecoder& operator=(const ZstdDecoder&) = delete;
    ZstdDecoder(ZstdDecoder&&) = delete;
    ZstdDecoder& operator=(ZstdDecoder&&) = delete;

    void process(std::span<const std::byte> input, const BufferProvider& provideBuffer, std::vector<TupleBuffer>& output) override;
    void flush(const BufferProvider& provideBuffer, std::vector<TupleBuffer>& output) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

private:
    ZSTD_DCtx* context = nullptr;
    /// True between the first byte of a frame and its end: input that stops here is truncated.
    bool insideFrame = false;
};

}

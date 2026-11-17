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
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Decoders/DecoderStage.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <ErrorHandling.hpp>

using LZ4F_dctx = struct LZ4F_dctx_s;

namespace NES
{

/// LZ4 has no options yet; the struct exists for the config registries.
struct LZ4DecoderConfig
{
    static std::expected<LZ4DecoderConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Decompresses the LZ4 frame format (what the lz4 CLI and LZ4F_compress* produce), incrementally: a frame
/// may span any number of input buffers, and several frames may follow each other in one stream.
///
/// Output is written into buffers from the provider; a buffer is passed on when it is full or when the
/// current input is used up, so decoded data never waits for the next read. Content and block checksums are
/// verified if the frame carries them. Corrupt input and a stream that ends inside a frame raise
/// CannotDecodeSourceData.
class LZ4Decoder final : public DecoderStage
{
public:
    static constexpr std::string_view NAME = "LZ4";

    explicit LZ4Decoder(const LZ4DecoderConfig& config);
    ~LZ4Decoder() override;
    LZ4Decoder(const LZ4Decoder&) = delete;
    LZ4Decoder& operator=(const LZ4Decoder&) = delete;
    LZ4Decoder(LZ4Decoder&&) = delete;
    LZ4Decoder& operator=(LZ4Decoder&&) = delete;

    void process(std::span<const std::byte> input, const BufferProvider& provideBuffer, std::vector<TupleBuffer>& output) override;
    void flush(const BufferProvider& provideBuffer, std::vector<TupleBuffer>& output) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

private:
    LZ4F_dctx* context = nullptr;
    /// True between the first byte of a frame and its end: input that stops here is truncated.
    bool insideFrame = false;
};

}

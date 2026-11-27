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

#include <expected>
#include <vector>

#include <Codecs/Decoder.hpp>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <ErrorHandling.hpp>

using LZ4F_dctx = struct LZ4F_dctx_s;

namespace NES
{

struct LZ4DecoderConfig
{
    static std::expected<LZ4DecoderConfig, Exception> fromConfig(const InstantiatedConfig&) { return LZ4DecoderConfig{}; }
};

/// Decompresses LZ4 frames, which may span buffers and follow each other.
/// Corrupt input and a stream that ends inside a frame result in CannotDecodeSourceData.
class LZ4Decoder final : public Decoder
{
public:
    explicit LZ4Decoder(const LZ4DecoderConfig& config);
    ~LZ4Decoder() override;
    LZ4Decoder(const LZ4Decoder&) = delete;
    LZ4Decoder& operator=(const LZ4Decoder&) = delete;
    LZ4Decoder(LZ4Decoder&&) = delete;
    LZ4Decoder& operator=(LZ4Decoder&&) = delete;

    std::expected<std::vector<TupleBuffer>, Exception> decode(const TupleBuffer& encoded, AbstractBufferProvider& bufferProvider) override;
    std::expected<std::vector<TupleBuffer>, Exception> flush(AbstractBufferProvider& bufferProvider) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

private:
    LZ4F_dctx* context = nullptr;
    /// True between the first byte of a frame and its end: a stream that ends here is truncated.
    bool insideFrame = false;
};

}

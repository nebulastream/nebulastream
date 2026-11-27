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

using ZSTD_DCtx = struct ZSTD_DCtx_s;

namespace NES
{

struct ZstdDecoderConfig
{
    static std::expected<ZstdDecoderConfig, Exception> fromConfig(const InstantiatedConfig&) { return ZstdDecoderConfig{}; }
};

/// Decompresses zstd frames, which may span buffers and follow each other.
/// Corrupt input and a stream that ends inside a frame result in CannotDecodeSourceData.
class ZstdDecoder final : public Decoder
{
public:
    explicit ZstdDecoder(const ZstdDecoderConfig& config);
    ~ZstdDecoder() override;
    ZstdDecoder(const ZstdDecoder&) = delete;
    ZstdDecoder& operator=(const ZstdDecoder&) = delete;
    ZstdDecoder(ZstdDecoder&&) = delete;
    ZstdDecoder& operator=(ZstdDecoder&&) = delete;

    std::expected<std::vector<TupleBuffer>, Exception> decode(const TupleBuffer& encoded, AbstractBufferProvider& bufferProvider) override;
    std::expected<std::vector<TupleBuffer>, Exception> flush(AbstractBufferProvider& bufferProvider) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

private:
    ZSTD_DCtx* context;
    /// True between the first byte of a frame and its end: a stream that ends here is truncated.
    bool insideFrame = false;
};

}

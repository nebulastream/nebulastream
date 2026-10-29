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
#include <ErrorHandling.hpp>

namespace NES
{

struct V4L2SourceConfig
{
    std::string device;
    uint32_t width;
    uint32_t height;
    uint32_t frameRate;
    std::string pixelFormat;
    uint32_t bufferCount;
    uint32_t pollTimeoutMs;

    static std::expected<V4L2SourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

class V4L2Source final : public Source
{
public:
    static constexpr std::string_view NAME = "V4L2";

    V4L2Source(const V4L2SourceConfig& config, const SourceDescriptor& sourceDescriptor);
    ~V4L2Source() override;

    V4L2Source(const V4L2Source&) = delete;
    V4L2Source& operator=(const V4L2Source&) = delete;
    V4L2Source(V4L2Source&&) = delete;
    V4L2Source& operator=(V4L2Source&&) = delete;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void close() override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    [[nodiscard]] std::string_view getType() const override {return "V4LS";}
protected:
    [[nodiscard]] std::ostream& toString(std::ostream& stream) const override;

private:
    struct MappedBuffer
    {
        void* data;
        size_t size;
    };

    std::string devicePath;
    uint32_t requestedWidth;
    uint32_t requestedHeight;
    uint32_t requestedFrameRate;
    uint32_t requestedPixelFormat;
    uint32_t requestedBufferCount;
    uint32_t pollTimeoutMs;

    uint32_t negotiatedWidth{0};
    uint32_t negotiatedHeight{0};
    uint32_t negotiatedPixelFormat{0};
    int device{-1};
    bool streaming{false};
    std::vector<MappedBuffer> mappedBuffers;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
};

}

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
#include <string_view>

#include <arv.h>
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

struct VideoSourceConfig
{
    uint32_t sourceSelector;

    static std::expected<VideoSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

class VideoSource final : public Source
{
public:
    static constexpr std::string_view NAME = "VIDEO";

    VideoSource(const VideoSourceConfig& config, const SourceDescriptor& sourceDescriptor);
    ~VideoSource() override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void close() override;

    [[nodiscard]] std::string_view getType() const override { return NAME; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& stream) const override;

private:
    struct GObjectDeleter
    {
        template <typename T>
        void operator()(T* object) const
        {
            if (object)
            {
                g_object_unref(object);
            }
        }
    };

    uint32_t sourceSelector;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::unique_ptr<ArvCamera, GObjectDeleter> camera;
    std::unique_ptr<ArvStream, GObjectDeleter> stream;
};

}

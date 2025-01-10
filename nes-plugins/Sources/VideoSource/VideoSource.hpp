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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <arv.h>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <sys/socket.h> /// For socket functions
#include <sys/types.h>
#include "Configurations/Descriptor.hpp"
#include "Sources/SourceDescriptor.hpp"

namespace NES::Sources
{

template <typename T>
struct g_clear
{
    void operator()(T* ptr) const { g_clear_object(&ptr); }
};

template <typename T>
using g_unique_ptr = std::unique_ptr<T, g_clear<T>>;

struct Context
{
    g_unique_ptr<ArvCamera> camera;
    g_unique_ptr<ArvStream> stream;
    std::atomic_bool requireReconnect;
};

class VideoSource : public Source
{
public:
    static constexpr std::string_view NAME = "Video";

    explicit VideoSource(const SourceDescriptor& sourceDescriptor);
    ~VideoSource() override;

    VideoSource(const VideoSource&) = delete;
    VideoSource& operator=(const VideoSource&) = delete;
    VideoSource(VideoSource&&) = delete;
    VideoSource& operator=(VideoSource&&) = delete;

    size_t fillTupleBuffer(
        NES::Memory::TupleBuffer& tupleBuffer, Memory::AbstractBufferProvider& bufferProvider, const std::stop_token& stopToken) override;

    /// Open TCP connection.
    void open() override;
    /// Close TCP connection.
    void close() override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::unique_ptr<Context> context;
    uint32_t sourceSelector;
};


struct ConfigParametersVideo
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<uint32_t> SOURCE_SELECTOR{
        "sourceSelector",
        0,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto source_selector = Util::from_chars<uint32_t>(config.at(SOURCE_SELECTOR));
            if (!source_selector)
            {
                NES_ERROR("Source selector is not a valid positive integer");
                return std::nullopt;
            }
            if (source_selector.value() > 1)
            {
                NES_ERROR("VideoSource Source selector {} is invalid (valid options are 0 for RGB and 1 for IR)", source_selector.value());
                return std::nullopt;
            }
            return source_selector;
        }};
    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(SOURCE_SELECTOR);
};

}

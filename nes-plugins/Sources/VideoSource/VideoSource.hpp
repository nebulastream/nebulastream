/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
*/

#pragma once

#include <cstdint>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <arv.h>

namespace NES
{

struct ConfigParametersVideo
{
    static inline const DescriptorConfig::ConfigParameter<uint32_t> SOURCE_SELECTOR{
        "sourceSelector",
        0,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto selector = DescriptorConfig::tryGet(SOURCE_SELECTOR, config);
            return selector && *selector <= 3 ? selector : std::nullopt;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, SOURCE_SELECTOR);
};

class VideoSource final : public Source
{
public:
    static constexpr std::string_view NAME = "Video";

    explicit VideoSource(const SourceDescriptor& sourceDescriptor);
    ~VideoSource() override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

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

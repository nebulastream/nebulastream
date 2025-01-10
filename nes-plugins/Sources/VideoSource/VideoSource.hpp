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

#include "VideoSourceService.hpp"


#include <chrono>
#include <memory>
#include <string_view>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/AsyncSource.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

template <typename Service>
class VideoSource final : public AsyncSource
{
public:
    static constexpr std::string_view NAME = "Video";

    explicit VideoSource(const SourceDescriptor& sourceDescriptor);
    ~VideoSource() override = default;

    void open(std::shared_ptr<AbstractBufferProvider>, AsyncSourceEmit&& emitFunction) override;
    void close() override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::optional<typename Service::Handle> handle;
    std::shared_ptr<Service> service;
    SourceDescriptor sourceDescriptor;
};

}

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

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <BackpressureChannel.hpp>

namespace NES
{

using SinkRegistryReturnType = std::unique_ptr<Sink>;

struct SinkRegistryArguments
{
    BackpressureController backpressureController;
    SinkDescriptor sinkDescriptor;
};

using SinkFactoryFn = std::function<SinkRegistryReturnType(SinkRegistryArguments)>;

/// Creates the registry entry for a sink implementation. Sinks are constructed from the
/// backpressure controller and their descriptor; the entry expression in
/// cmake/RuntimeRegistrationUtil.cmake instantiates this per plugin type.
template <typename SinkImpl>
SinkFactoryFn makeSinkFactory()
{
    return [](SinkRegistryArguments arguments) -> SinkRegistryReturnType
    { return std::make_unique<SinkImpl>(std::move(arguments.backpressureController), arguments.sinkDescriptor); };
}

class SinkRegistry : public RuntimeRegistry<SinkRegistry, std::string, SinkFactoryFn, /*CaseSensitive*/ false>
{
public:
    static SinkRegistry& instance();
};

}

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

#include "TokioSource.hpp"


#include <Sources/SourceReturnType.hpp>
#include <nes-source-runtime-bindings/lib.h>
#include <rfl/json/Parser.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <IORuntime.hpp>
#include <TokioSource.hpp>

/// Serialize a DescriptorConfig::Config as flat {"key":"value",...} JSON
/// for the Rust FFI, which expects HashMap<String, String>.
static std::string configToFlatJson(const NES::DescriptorConfig::Config& config)
{
    std::unordered_map<std::string, std::string> flat;
    for (const auto& [key, value] : config)
    {
        flat[key] = std::get<std::string>(value);
    }
    return rfl::json::write(flat);
}

namespace NES
{
struct Context
{
    std::optional<size_t> stopEpoch;
    std::atomic_size_t epochCounter;
    std::shared_ptr<SourceContext> context;
    rust::Box<SourceHandle> handle;

    Context(
        const SourceDescriptor& descriptor,
        std::shared_ptr<AbstractBufferProvider> bufferProvider,
        SourceReturnType::AsyncEmitFunction emitFunction,
        OriginId originId)
        : epochCounter(0)
        , context(
              std::make_shared<SourceContext>(SourceContext{
                  .onData = std::function(
                      [originId, emit = std::move(emitFunction)](
                          TupleBuffer buffer,
                          rust::Fn<void(rust::Box<AsyncCompletionContext>, AsyncCompletionResult)> done,
                          rust::Box<AsyncCompletionContext> completionContext)
                      {
                          NES_INFO("Sink Failed: {}");
                          emit(
                              originId,
                              NES::SourceReturnType::Data{.buffer = std::move(buffer), .onComplete = {}},
                              SourceReturnType::AsyncOperationCallback([done, completionContext](auto result)
                                                                       { done(completionContext, result); }));
                      }),
                  .onError = std::function([](std::string_view error) { NES_ERROR("Sink Failed: {}", error); }),
                  .onEoS = std::function([] { NES_INFO("End of Stream"); })}))
        , handle(create_handle(
              descriptor.getSourceType(),
              configToFlatJson(descriptor.getConfig()),
              NES::IORuntime::get().id(),
              context,
              std::move(bufferProvider)))
    {
    }

    ~Context() { NES_DEBUG("TokioSource::~Context()"); }
};
}

NES::TokioSource::TokioSource(BackpressureListener listener, SourceDescriptor descriptor, OriginId originId)
    : context(nullptr), originId(originId), descriptor(descriptor), backpressureHandler(listener)
{
}

NES::TokioSource::~TokioSource()
{
}

bool NES::TokioSource::start(
    NES::SourceReturnType::AsyncEmitFunction&& emitFunction, std::shared_ptr<AbstractBufferProvider> buffer_provider) const
{
    std::make_unique<Context>(descriptor, buffer_provider, emitFunction, originId);
    return true;
}

NES::OriginId NES::TokioSource::getOriginId()
{
    return originId;
}

void NES::TokioSource::stop() const
{
    ::stop(*context->handle);
}

std::ostream& NES::operator<<(std::ostream& out, const TokioSource&)
{
    return out << "TokioSource::toString()";
}
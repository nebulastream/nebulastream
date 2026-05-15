#include <iostream>


#include <nes-io-runtime-bindings/lib.h>
#include <nlohmann/json.hpp>
#include <IORuntime.hpp>
#include <IORuntimeBindings.hpp>
#include <Thread.hpp>

namespace NES
{

IORuntime::IORuntime() : handle(init_io_runtime(2, ThreadInitializationContext::fromCurrentThreadsContext()))
{
}

IORuntime::~IORuntime() = default;

void IORuntime::attachConfig(std::string_view serviceName, const nlohmann::json& config)
{
    attach_config(*handle, rust::Str(std::string(serviceName)), rust::Str(config.dump()));
}

} // namespace NES

extern "C" IORuntimeHandle* current_io_runtime_internal()
{
    if (auto instance = NES::IORuntime::tryInstance())
    {
        return std::addressof(*instance->handle);
    }
    NES_ERROR("No IORuntime instance found.");
    return nullptr;
}

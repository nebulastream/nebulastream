#pragma once

#ifndef INCLUDED_FROM_TOKIO_SINK_REGISTRY
#    error "This file should not be included directly! Include TokioSinkRegistry.hpp instead"
#endif

#include <string>
#include <vector>
#include <Util/Registry.hpp>

namespace NES
{
TokioSinkRegistryReturnType createTokioSinkFromRegistry(std::string name, TokioSinkRegistryArguments args);
std::vector<std::string> getRegisteredTokioSinkNames();

template <>
inline void
Registrar<TokioSinkRegistry, std::string, TokioSinkRegistryReturnType, TokioSinkRegistryArguments>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    for (const auto& n : getRegisteredTokioSinkNames())
    {
        registry.addEntry(n, [n](TokioSinkRegistryArguments args) {
            return createTokioSinkFromRegistry(n, std::move(args));
        });
    }
}
}

#pragma once

#ifndef INCLUDED_FROM_TOKIO_SOURCE_REGISTRY
#    error "This file should not be included directly! Include TokioSourceRegistry.hpp instead"
#endif

#include <string>
#include <vector>
#include <Util/Registry.hpp>

namespace NES
{
TokioSourceRegistryReturnType createTokioSourceFromRegistry(std::string name, TokioSourceRegistryArguments args);
std::vector<std::string> getRegisteredTokioSourceNames();

template <>
inline void
Registrar<TokioSourceRegistry, std::string, TokioSourceRegistryReturnType, TokioSourceRegistryArguments>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    for (const auto& n : getRegisteredTokioSourceNames())
    {
        registry.addEntry(n, [n](TokioSourceRegistryArguments args) {
            return createTokioSourceFromRegistry(n, std::move(args));
        });
    }
}
}

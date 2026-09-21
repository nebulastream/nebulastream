// Licensed under the Apache License, Version 2.0 (the "License");
#include <NESCodonPlugin/Plugin.hpp>
#include <NESCodonPlugin/Registry.hpp>

#include <dlfcn.h>

#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace NES
{
namespace
{
struct Registry
{
    std::mutex mutex;
    std::unordered_map<std::string, std::string_view> modules;
    std::unordered_map<std::string, void*> nativeSymbols;
    std::unordered_set<std::string> loadedPaths;
    std::vector<void*> handles;
};

Registry& registry()
{
    static Registry instance;
    return instance;
}

std::string dlError(const std::string_view operation, const std::filesystem::path& path)
{
    const auto* error = ::dlerror();
    return std::string{operation} + " '" + path.string() + "': " + (error == nullptr ? "unknown dynamic-loader error" : error);
}
}

void loadCodonPlugin(const std::filesystem::path& path)
{
    const auto normalizedPath = std::filesystem::absolute(path).lexically_normal().string();
    auto& state = registry();
    const std::scoped_lock lock{state.mutex};
    if (state.loadedPaths.contains(normalizedPath))
    {
        return;
    }

    ::dlerror();
    void* handle = ::dlopen(normalizedPath.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr)
    {
        throw std::runtime_error(dlError("Could not load Codon plugin", normalizedPath));
    }

    ::dlerror();
    const auto entryPoint = reinterpret_cast<NESCodonPluginEntryPointV1>(::dlsym(handle, NES_CODON_PLUGIN_ENTRY_POINT));
    if (entryPoint == nullptr)
    {
        const auto message = dlError("Codon plugin does not export " + std::string{NES_CODON_PLUGIN_ENTRY_POINT} + " in", normalizedPath);
        ::dlclose(handle);
        throw std::runtime_error(message);
    }
    const auto* plugin = entryPoint();
    if (plugin == nullptr || plugin->abiVersion != NES_CODON_PLUGIN_ABI_VERSION || plugin->name == nullptr)
    {
        ::dlclose(handle);
        throw std::runtime_error("Codon plugin '" + normalizedPath + "' has an incompatible descriptor");
    }

    for (size_t index = 0; index < plugin->moduleCount; ++index)
    {
        const auto& module = plugin->modules[index];
        if (module.path == nullptr || module.source == nullptr)
        {
            ::dlclose(handle);
            throw std::runtime_error("Codon plugin '" + normalizedPath + "' declares an invalid module");
        }
        if (state.modules.contains(module.path))
        {
            ::dlclose(handle);
            throw std::runtime_error("Codon module '" + std::string{module.path} + "' is registered more than once");
        }
    }
    for (size_t index = 0; index < plugin->nativeSymbolCount; ++index)
    {
        const auto& symbol = plugin->nativeSymbols[index];
        if (symbol.name == nullptr || symbol.address == nullptr)
        {
            ::dlclose(handle);
            throw std::runtime_error("Codon plugin '" + normalizedPath + "' declares an invalid native symbol");
        }
        if (state.nativeSymbols.contains(symbol.name))
        {
            ::dlclose(handle);
            throw std::runtime_error("Codon native symbol '" + std::string{symbol.name} + "' is registered more than once");
        }
    }

    for (size_t index = 0; index < plugin->moduleCount; ++index)
    {
        const auto& module = plugin->modules[index];
        state.modules.emplace(module.path, std::string_view{module.source, module.sourceSize});
    }
    for (size_t index = 0; index < plugin->nativeSymbolCount; ++index)
    {
        const auto& symbol = plugin->nativeSymbols[index];
        state.nativeSymbols.emplace(symbol.name, symbol.address);
    }
    state.loadedPaths.emplace(normalizedPath);
    state.handles.emplace_back(handle);
}

std::string_view findCodonPluginModule(const std::string_view path)
{
    auto& state = registry();
    const std::scoped_lock lock{state.mutex};
    const auto module = state.modules.find(std::string{path});
    if (module != state.modules.end())
    {
        return module->second;
    }
    for (const auto& [modulePath, source] : state.modules)
    {
        if (path.size() > modulePath.size() && path.ends_with(modulePath) && path[path.size() - modulePath.size() - 1] == '/')
        {
            return source;
        }
    }
    return {};
}

std::unordered_map<std::string, void*> getCodonPluginNativeSymbols()
{
    auto& state = registry();
    const std::scoped_lock lock{state.mutex};
    return state.nativeSymbols;
}
}

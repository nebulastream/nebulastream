// Licensed under the Apache License, Version 2.0 (the "License");
#pragma once

#include <cstddef>
#include <cstdint>

#if defined(__GNUC__) || defined(__clang__)
    #define NES_CODON_PLUGIN_EXPORT __attribute__((visibility("default")))
#else
    #define NES_CODON_PLUGIN_EXPORT
#endif

extern "C" {
inline constexpr uint32_t NES_CODON_PLUGIN_ABI_VERSION = 1;
inline constexpr const char* NES_CODON_PLUGIN_ENTRY_POINT = "nes_codon_plugin_v1";

struct NESCodonModuleV1
{
    /// Relative embedded resource path, including its .codon or .py extension.
    const char* path;
    const char* source;
    size_t sourceSize;
};

struct NESCodonNativeSymbolV1
{
    const char* name;
    void* address;
};

struct NESCodonPluginV1
{
    uint32_t abiVersion;
    const char* name;
    const NESCodonModuleV1* modules;
    size_t moduleCount;
    const NESCodonNativeSymbolV1* nativeSymbols;
    size_t nativeSymbolCount;
};

using NESCodonPluginEntryPointV1 = const NESCodonPluginV1* (*)();
}

// Licensed under the Apache License, Version 2.0 (the "License");
#pragma once

#include <cstdint>
#include <span>
#include <string_view>

namespace NES
{
struct Arena;

struct UdfErrorHolder
{
    int8_t* errorPtr;
    uint64_t errorSize;
};

/// Fixed ABI used by the small compiled trampoline when the surrounding pipeline runs in interpreter mode.
struct PythonUdfAbiValue
{
    int8_t* data;
    uint64_t size;
    uint8_t isNull;
};

struct PythonUdfRuntimeSymbol
{
    std::string_view name;
    void* address;
};

void activatePythonUdfArena(Arena* arena);
std::span<const PythonUdfRuntimeSymbol> getPythonUdfRuntimeSymbols();
bool isPythonUdfRuntimeSymbol(std::string_view name);
}

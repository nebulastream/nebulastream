// Licensed under the Apache License, Version 2.0 (the "License");
#pragma once

#include <filesystem>
#include <string_view>
#include <unordered_map>

namespace NES
{
void loadCodonPlugin(const std::filesystem::path& path);
std::string_view findCodonPluginModule(std::string_view path);
std::unordered_map<std::string, void*> getCodonPluginNativeSymbols();
}

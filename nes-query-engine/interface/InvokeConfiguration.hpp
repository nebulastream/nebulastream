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

// InvokeConfig.hpp
#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <unordered_map>
// #include <yaml-cpp/yaml.h>

#include <nautilus/common/FunctionAttributes.hpp>

namespace NES
{

enum class InvokeMode
{
    Default,
    FnAttr,
    Inline
};

class InvokeConfig
{
public:
    static InvokeConfig& instance();

    /// Load from a YAML file path. If path is empty or doesn't exist, resets to defaults.
    bool loadFromFile(const std::string& path);

    /// Resolution: per-tag override wins, then global.
    InvokeMode resolveMode(const std::string& tag) const;
    nautilus::FunctionAttributes resolveFnAttr(const std::string& tag) const;

    /// Manual setters (useful for tests)
    void setGlobalMode(InvokeMode mode) { globalMode_ = mode; }

    void setGlobalFnAttr(nautilus::FunctionAttributes attr) { globalAttr_ = attr; }

    void setMode(const std::string& tag, InvokeMode mode) { modes_[tag] = mode; }

    void setFnAttr(const std::string& tag, nautilus::FunctionAttributes attr) { attrs_[tag] = attr; }

    void reset();

private:
    InvokeConfig() = default;

    static InvokeMode parseModeString(const std::string& s);
    static nautilus::ModRefInfo parseModRefInfo(const std::string& s);
    // static nautilus::FunctionAttributes parseFnAttrNode(const YAML::Node& node);

    InvokeMode globalMode_ = InvokeMode::Default;
    std::optional<nautilus::FunctionAttributes> globalAttr_;

    std::unordered_map<std::string, InvokeMode> modes_;
    std::unordered_map<std::string, nautilus::FunctionAttributes> attrs_;
};

} // namespace NES

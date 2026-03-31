// InvokeConfig.cpp
#include <InvokeConfiguration.hpp>


#include <filesystem>
#include <stdexcept>
#include <yaml-cpp/yaml.h>

#include <Util/Logger/Logger.hpp>

namespace NES
{

void InvokeConfig::reset()
{
    globalMode_ = InvokeMode::Default;
    globalAttr_.reset();
    modes_.clear();
    attrs_.clear();
}

InvokeMode InvokeConfig::parseModeString(const std::string& s)
{
    if (s == "inline")
        return InvokeMode::Inline;
    if (s == "fnattr")
        return InvokeMode::FnAttr;
    if (s == "default")
        return InvokeMode::Default;
    throw std::runtime_error("Unknown invoke mode: " + s);
}

nautilus::ModRefInfo parseModRefInfo(const std::string& s)
{
    if (s == "ref")
        return nautilus::ModRefInfo::Ref;
    if (s == "mod")
        return nautilus::ModRefInfo::Mod;
    if (s == "modref")
        return nautilus::ModRefInfo::ModRef;
    if (s == "nomodref")
        return nautilus::ModRefInfo::NoModRef;
    throw std::runtime_error("Unknown ModRefInfo: " + s);
}

nautilus::FunctionAttributes parseFnAttrNode(const YAML::Node& node)
{
    nautilus::FunctionAttributes attr{};
    if (node["mod_ref_info"])
        attr.modRefInfo = parseModRefInfo(node["mod_ref_info"].as<std::string>());
    if (node["will_return"])
        attr.willReturn = node["will_return"].as<bool>();
    if (node["no_unwind"])
        attr.noUnwind = node["no_unwind"].as<bool>();
    return attr;
}

InvokeConfig& InvokeConfig::instance()
{
    static InvokeConfig cfg;
    return cfg;
}

bool InvokeConfig::loadFromFile(const std::string& path)
{
    // No path specified — intentional default, not an error.
    if (path.empty())
    {
        NES_INFO("InvokeConfig: no config file specified. Using global_mode=default (no attributes, no inlining).");
        return true;
    }

    // Path specified but file not accessible.
    if (!std::filesystem::exists(path))
    {
        NES_ERROR("InvokeConfig: config file '{}' does not exist or is not accessible.", path);
        return false;
    }

    NES_INFO("InvokeConfig: loading from '{}'", path);

    YAML::Node root;
    try
    {
        root = YAML::LoadFile(path);
    }
    catch (const YAML::Exception& e)
    {
        NES_ERROR("InvokeConfig: failed to parse '{}': {}", path, e.what());
        return false;
    }

    try
    {
        // --- global mode ---
        if (root["global_mode"])
        {
            globalMode_ = parseModeString(root["global_mode"].as<std::string>());
        }

        // --- global function attributes ---
        if (root["global_fn_attr"])
        {
            globalAttr_ = parseFnAttrNode(root["global_fn_attr"]);
        }

        // --- per-function overrides ---
        if (root["overrides"])
        {
            for (auto it = root["overrides"].begin(); it != root["overrides"].end(); ++it)
            {
                const std::string tag = it->first.as<std::string>();
                const YAML::Node& entry = it->second;

                if (entry["mode"])
                {
                    modes_[tag] = parseModeString(entry["mode"].as<std::string>());
                }

                if (entry["fn_attr"])
                {
                    attrs_[tag] = parseFnAttrNode(entry["fn_attr"]);
                }
            }
        }
    }
    catch (const std::exception& e)
    {
        NES_ERROR("InvokeConfig: error processing '{}': {}", path, e.what());
        reset(); // leave in a clean default state rather than half-loaded
        return false;
    }

    NES_INFO(
        "InvokeConfig: loaded global_mode={}, {} overrides",
        root["global_mode"] ? root["global_mode"].as<std::string>() : "default",
        modes_.size());
    // setInstance({*this});
    return true;
}

InvokeMode InvokeConfig::resolveMode(const std::string& tag) const
{
    auto it = modes_.find(tag);
    if (it != modes_.end())
        return it->second;
    return globalMode_;
}

nautilus::FunctionAttributes InvokeConfig::resolveFnAttr(const std::string& tag) const
{
    auto it = attrs_.find(tag);
    if (it != attrs_.end())
        return it->second;
    if (globalAttr_.has_value())
        return globalAttr_.value();
    // sensible fallback
    return nautilus::FunctionAttributes{.modRefInfo = nautilus::ModRefInfo::Ref, .willReturn = true, .noUnwind = true};
}

} // namespace NES
# On Extensibility, Plugins, and Registries in NebulaStream
We strive for NebulaStream to be as extensible as possible, where feasible.
This is known as the open-closed-principle, where the system should be open for extension but closed for modification.
Extensibility applies to all components that conform to an interface, and multiple implementations are possible without requiring an adaptation of a shared core.
Examples of such components in NebulaStream include:
- `Sources`
- `InputFormatters`
- `DataTypes`
- `Functions`
- `Operators`
- `RewriteRules`
- `Sinks`
- ...

We strive to make the addition of each of these components as easy as possible by providing a uniform way to extend them via **plugins** and **registries**.

## Plugins
Plugins are concrete implementations of one of these extensible components.
Currently, they are organized in two tiers:
1. Optional plugins, live in `nes-plugins` and are deactivated by default
2. Internal plugins, live in the core directories (`nes-sources` or `nes-input-formatters`) and are activated in every build

### Optional Plugins
To enable an optional plugin you would like to use, go to `nes-plugins/CMakeLists.txt`, and change the property of the plugin like this: `activate_optional_plugin("Sources/TCPSource" ON)`.
This will add the plugin to the NebulaStream build.
Optional plugins can be added as a library in the following way:
```cmake
add_plugin_as_library(<PLUGIN_NAME> <COMPONENT_NAME> <REGISTRY_NAME> <LIBRARY_NAME> <SOURCE_FILES>)
target_link_libraries(<LIBRARY_NAME> PRIVATE <DEPENDS_ON_LIBRARY>) # <-- optional, set if plugin lib depends on additional libraries
```
For a `TCPSource`, this might then look like this:
```cmake
add_plugin_as_library(TCP Source nes-sources-registry tcp_source_plugin_library TCPSource.cpp)
```
- `TCP` is the unique identifier that will be used to create an instance from the registry
- `Source` is the name of the component that the plugin is a part of
- `nes-sources-registry` is the name of the library that contains the specific registry for the component
- `tcp_source_plugin_library` is the name of the library that will be the result of the `add_plugin_as_library` call
- `TCPSource.cpp` is a list of source files that will be part of the plugin library

Plugins may specify additional dependencies that will **only** be a part of the plugin library.
These can be added e.g., via FetchContent within the `CMakeLists.txt` of the root directory of the plugin.

**If you want to add a plugin, first add it to `nes-plugins` under the appropriate prefix**.
For example, if you want to add support for the XML format, this plugin would go under `nes-plugins/InputFormatters/XmlInputFormatter`.
When a plugin is used extensively and tested sufficiently, it may be promoted to an internal plugin.

### Internal Plugins
Internal plugins live directly in the source tree of the respective component, e.g., in `nes-physical-operators/src/Functions/ArithmeticalFunctions/AddPhysicalFunction.cpp`.
In the `CMakeLists.txt` of the source directory where the arithmetical functions are defined, the internal plugins are added:
```cmake
add_plugin(Add PhysicalFunction nes-physical-operators AddPhysicalFunction.cpp)
add_plugin(Div PhysicalFunction nes-physical-operators DivPhysicalFunction.cpp)
add_plugin(Mod PhysicalFunction nes-physical-operators ModPhysicalFunction.cpp)
add_plugin(Mul PhysicalFunction nes-physical-operators MulPhysicalFunction.cpp)
add_plugin(Sub PhysicalFunction nes-physical-operators SubPhysicalFunction.cpp)
```

Notice how the cmake function is slightly different from how an optional plugin is added.
While an optional plugin will be built as a library that is linked with the component library (`nes-physical-operators`), an internal plugin will be part of the component library, because it is **always** active.

# Registries
Registries are libraries that act as a factory for plugins that have been registered in them.
They are linked against the component library and vice versa, such that they:
- Have access to the appropriate types defined in the component
- Can be used by the component to create the types registered

At compile time, we need to specify the plugins that should be part of a registry.
This includes all internal plugins and the optional plugins that are activated in the respective `CMakeLists.txt` files of the plugins.

If we look into one of the extensible components of the system, we find a `registry` folder in them.
Within this folder, the `include` directory contains registries, e.g., the `SourceRegistry.hpp`:
```c++
namespace NES::Sources
{

using SourceRegistryReturnType = std::unique_ptr<Source>; /// <-- this type will be returned by the registry
struct SourceRegistryArguments /// <-- this will be passed to the creation function to construct the appropriate type
{
    SourceDescriptor sourceDescriptor;
};

class SourceRegistry : public BaseRegistry<SourceRegistry, std::string, SourceRegistryReturnType, SourceRegistryArguments>
{
};

}

#define INCLUDED_FROM_SOURCE_REGISTRY
#include <SourceGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SOURCE_REGISTRY
```

This defines a registry for the source component and inherits from `BaseRegistry`.
It defines the return type of the registry, a `unique_ptr` to an implementation of the `Source` interface.
Then, it defines a struct that contains all necessary arguments to create an object of the type, which in this case is a `SourceDescriptor`.
The return type and arguments help you reason about what type your plugin will produce and what types it will receive as input to be constructed.
Finally, at the bottom, we include a `Registrar`.
Registrars are generated by CMake at build time. They register all enabled plugins of a component into its registry, making them available for use at runtime.
They implement the `registerAll(Registry<Registrar>& registry)` interface and are derived from template files located under `registry/templates`.
For example, the registrar template for the source component, `SourceGeneratedRegistrar.inc.in`, looks like this:
```c++
namespace NES::Sources::SourceGeneratedRegistrar
{

/// declaration of register functions for 'Sources'
@REGISTER_FUNCTION_DECLARATIONS@
}

namespace NES
{
template <>
inline void
Registrar<Sources::SourceRegistry, std::string, Sources::SourceRegistryReturnType, Sources::SourceRegistryArguments>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{

    using namespace NES::Sources::SourceGeneratedRegistrar;
    /// the SourceRegistry calls registerAll and thereby all the below functions that register Sources in the SourceRegistry
    @REGISTER_ALL_FUNCTION_CALLS@
}
}
```
This template is used by CMake to generate the **declarations** and **invocations** of registration functions within the `Registrar` type. The resulting code may look like this:
```c++
namespace NES::Sources::SourceGeneratedRegistrar
{

/// declaration of register functions for 'Sources'
SourceRegistryReturnType RegisterTCPSource(SourceRegistryArguments);
SourceRegistryReturnType RegisterFileSource(SourceRegistryArguments);

}

namespace NES
{
template <>
inline void
Registrar<Sources::SourceRegistry, std::string, Sources::SourceRegistryReturnType, Sources::SourceRegistryArguments>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{

    using namespace NES::Sources::SourceGeneratedRegistrar;
    /// the SourceRegistry calls registerAll and thereby all the below functions that register Sources in the SourceRegistry
    registry.addEntry("TCP", RegisterTCPSource);
    registry.addEntry("File", RegisterFileSource);
}
}
```
Note that this header file is directly generated into CMake's build folder.
The specific contents of the file depend on the plugins that are activated in the current build.
Finally, the missing piece of the puzzle is the **definitions** of the register functions.
These are located in the plugins source files, e.g., `TCPSource.cpp` because only the plugins themselves are capable of producing an instance of their type.
Usually, the register function will simply call the constructor and forward the arguments to it, optionally wrapping the object into a smart pointer.
```c++
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterTCPSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<TCPSource>(sourceRegistryArguments.sourceDescriptor);
}
```

For you to develop a new plugin, only this function needs to be implemented, the rest will be handled automatically in the build process.
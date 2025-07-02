# On Extensibility, Plugins, and Registries in NebulaStream
At NebulaStream, we aim to make the system as extensible as reasonably possible.
This approach follows the open-closed principle, meaning the system should be open to extension, but closed to modification.
Extensibility applies to all components that adhere to an interface, allowing for multiple implementations without requiring changes to a shared core.
In NebulaStream, examples of such components include:
- `Sources`
- `InputFormatters`
- `DataTypes` (limited)
- `Functions`
- `Operators`
- `RewriteRules`
- `Sinks`
- ...

**Plugins** and **registries** offer a uniform way to extend these components without the need for detailed knowledge about the core system.

## Plugins
Plugins are concrete implementations of extensible components.
Currently, they are organized into two tiers:
1. Optional plugins, located in nes-plugins, which are deactivated by default.
2. Internal plugins, located in the core nes-* directories, and enabled in every build.

### Optional Plugins
To enable an optional plugin, open nes-plugins/CMakeLists.txt and set the desired plugin’s property like this:
```cmake
activate_optional_plugin("Sources/TCPSource" ON)
```
This includes the plugin in the NebulaStream build.
Optional plugins can be added as libraries using the following structure:
```cmake
add_plugin_as_library(<PLUGIN_NAME> <COMPONENT_NAME> <REGISTRY_NAME> <LIBRARY_NAME> <SOURCE_FILES>)
target_link_libraries(<LIBRARY_NAME> PRIVATE <DEPENDS_ON_LIBRARY>) # <-- optional, set if plugin lib depends on additional libraries
```
For instance, a `TCPSource` plugin might look like this:
```cmake
add_plugin_as_library(TCP Source nes-sources-registry tcp_source_plugin_library TCPSource.cpp)
```
Where:
- `TCP` is the unique identifier used to instantiate the plugin from the registry.
- `Source` is the name of the registry the plugin belongs to.
- `nes-sources-registry` is the library that holds the specific component’s registry.
- `tcp_source_plugin_library` is the resulting library from the `add_plugin_as_library` command.
- TCPSource.cpp lists the source files that make up the plugin library.

Plugins may declare additional dependencies, which will be exclusive to the plugin library.
These can be added, for example, using `FetchContent` in the plugin's root `CMakeLists.txt`.

**When creating a new plugin, add it to nes-plugins under the correct prefix.**
For example, if you’re introducing XML format support, place it under: `nes-plugins/InputFormatters/XmlInputFormatter`.
Once a plugin is widely used and well-tested, it may be promoted to an internal plugin.

### Internal Plugins
Internal plugins reside directly within the source directory of their corresponding components.
For instance:
```
nes-physical-operators/src/Functions/ArithmeticalFunctions/AddPhysicalFunction.cpp
```
In the source directory’s `CMakeLists.txt`, internal plugins are added like this:
```cmake
add_plugin(Add PhysicalFunction nes-physical-operators AddPhysicalFunction.cpp)
add_plugin(Div PhysicalFunction nes-physical-operators DivPhysicalFunction.cpp)
add_plugin(Mod PhysicalFunction nes-physical-operators ModPhysicalFunction.cpp)
add_plugin(Mul PhysicalFunction nes-physical-operators MulPhysicalFunction.cpp)
add_plugin(Sub PhysicalFunction nes-physical-operators SubPhysicalFunction.cpp)
```
Notice the slight difference in the CMake function used here compared to optional plugins.
While `add_plugin_as_library` creates a standalone library that is then linked into the component’s library (e.g., `nes-physical-operators`),
`add_plugin` integrates the plugin directly into the component’s library, making it active in every build.

# Registries
Registries are libraries that act as factories for creating registered plugins.
They are linked with the component libraries (e.g., `nes-sources`, `nes-input-formatters`, etc.) and vice versa, so that they:
- Have access to the appropriate header files defined in the component
- Can be used by the corresponding component to access the registered plugins

During the build process, it’s necessary to specify which plugins should be part of a registry.
This includes both internal plugins and the optional ones that have been activated in their respective `CMakeLists.txt` files.
Within each extensible component, you’ll find a registry directory.
Inside it, the include directory contains the registries — e.g., `SourceRegistry.hpp`:
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

This defines the registry for the source component, which inherits from `BaseRegistry`.
It specifies:
- The return type, a `unique_ptr` to a `Source` implementation.
- A struct for all required arguments, in this case a `SourceDescriptor`.

These declarations help reason about the output type your plugin should produce and the input required to construct it.
At the end of the file, the `Registrar` is included.
Registrars are auto-generated by CMake during the build.
They register all enabled plugins into the registry, making them accessible at runtime.
Registrars implement the `registerAll(Registry<Registrar>& registry)` interface and are based on templates found under `registry/templates`.

For example, the registrar template for sources, `SourceGeneratedRegistrar.inc.in`, looks like:
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
CMake uses this template to generate the actual declarations and calls for the registration functions.
The resulting generated code might look like:
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
This header is directly generated into CMake’s build folder.
Its content depends on which plugins are active in the current build.
The final piece of the puzzle is the definition of the register functions.
These are implemented in the plugin’s source file, such as `TCPSource.cpp`, since only the plugin has the logic to construct an instance of its type.
Typically, the register function simply constructs the object, forwarding the necessary arguments, and wraps it in a smart pointer:
```c++
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterTCPSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<TCPSource>(sourceRegistryArguments.sourceDescriptor);
}
```

To develop a new plugin, only this function interacting with the registry needs to be implemented. 
The rest is handled automatically during the build process.

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
- `LoweringRules`
- `Sinks`
- ...

**Plugins** and **registries** offer a uniform way to extend these components without the need for detailed knowledge about the core system.

## Plugins
Plugins are concrete implementations of extensible components.
Currently, they are organized into two tiers:
1. Optional plugins, located in nes-plugins, whose sources can be included or omitted from the build.
2. Internal plugins, located in the core nes-* directories, and enabled in every build.

### Optional Plugins
To enable an optional plugin, add its directory in `nes-plugins/CMakeLists.txt`:
```cmake
add_subdirectory(Sources/TCPSource)
```
Remove that line to exclude the plugin. The plugin directory adds its implementation directly to the component that owns it:
```cmake
target_sources(<COMPONENT_TARGET> PRIVATE <SOURCE_FILES>)
target_link_libraries(<COMPONENT_TARGET> PRIVATE <DEPENDS_ON_LIBRARY>) # optional
```
For instance, a `TCPSource` plugin might look like this:
```cmake
target_sources(nes-sources PRIVATE TCPSource.cpp)
```
Where:
- `nes-sources` is the component that owns the source implementation.
- `TCPSource.cpp` contains the plugin hook and implementation.

Plugins may add additional dependencies to their component target.

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
add_source_files(nes-physical-operators
        AddPhysicalFunction.cpp
        DivPhysicalFunction.cpp
        ModPhysicalFunction.cpp
        MulPhysicalFunction.cpp
        SubPhysicalFunction.cpp)
```
Built-in implementations are ordinary component sources. Their `ADD_PLUGIN` hooks handle registration.

# Registries
Registries are factories for creating registered plugins. A registry defines its key, return type, and construction arguments:

```c++
namespace NES
{
using SourceRegistryReturnType = std::unique_ptr<Source>;
struct SourceRegistryArguments
{
    SourceDescriptor sourceDescriptor;
};

class SourceRegistry : public Registry<SourceRegistry, std::string, SourceRegistryReturnType, SourceRegistryArguments>
{
};
}
```

Each registry entry provides one init hook. The linker collects these hooks, and `initializePlugins()` calls them in link order during startup.

```c++
SourceRegistryReturnType RegisterTCPSource(SourceRegistryArguments arguments)
{
    return std::make_unique<TCPSource>(arguments.sourceDescriptor);
}

ADD_PLUGIN(SourceRegistry, "TCP", RegisterTCPSource);
```

Registries are ordinary singletons and know nothing about the linker. Only the parameterless init hooks are stored in the `nes_plugin_init` linker section.

Plugin-bearing component archives are retained with `WHOLE_ARCHIVE`, so one implementation can register with multiple registries:

```cmake
target_sources(nes-sources PRIVATE TCPSource.cpp TCPDataServer.cpp)
```

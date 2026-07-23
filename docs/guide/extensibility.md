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
1. Optional plugins, located in nes-plugins, which are deactivated by default.
2. Internal plugins, located in the core nes-* directories, and enabled in every build.

### Optional Plugins
To enable an optional plugin, open nes-plugins/CMakeLists.txt and add the plugin's directory like this:
```cmake
add_plugin("Sources/TCPSource")
```
This includes the plugin in the NebulaStream build.
Optional plugins build one implementation library and register entries with the runtime
registries (see cmake/RuntimeRegistrationUtil.cmake):
```cmake
add_library(<LIBRARY_NAME> STATIC <SOURCE_FILES>)
target_link_libraries(<LIBRARY_NAME> PRIVATE <OWNING_COMPONENT>)
link_plugin_library(<OWNING_COMPONENT> <LIBRARY_NAME>)
add_registry_entry(<REGISTRY_NAME> <PLUGIN_NAME> [KEY <key>] [OPTIONAL])
```
For instance, the `TCPSource` plugin looks like this:
```cmake
add_library(tcp_source_plugin_library STATIC TCPSource.cpp TCPDataServer.cpp)
target_link_libraries(tcp_source_plugin_library PRIVATE nes-sources)
link_plugin_library(nes-sources tcp_source_plugin_library)
add_registry_entry(Source TCP)
add_registry_entry(SourceValidation TCP)
```
Where:
- `TCP` is the unique identifier used to instantiate the plugin from the registry (the entry
  expression is defined by the registry's `create_runtime_registry` declaration and references
  the plugin's type, e.g. `TCPSource`).
- `Source`/`SourceValidation` are the registries the plugin contributes to.

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
In the source directory’s `CMakeLists.txt`, internal plugins register their entries like this:
```cmake
add_registry_entry(PhysicalFunction Add)
add_registry_entry(PhysicalFunction Div)
add_registry_entry(PhysicalFunction Mod)
add_registry_entry(PhysicalFunction Mul)
add_registry_entry(PhysicalFunction Sub)
```
Internal plugins compile their sources directly into the component's library (via
`add_source_files`) and only differ from optional plugins in where the sources live.

# Registries
Registries are runtime factories for registered plugins (see `nes-common/include/Util/RuntimeRegistry.hpp`
and `cmake/RuntimeRegistrationUtil.cmake`). Each extensible component has a `registry/include`
directory with the registry headers — e.g., `SourceRegistry.hpp`:
```c++
namespace NES
{

using SourceRegistryReturnType = std::unique_ptr<Source>; /// <-- this type is produced by the registry entries
struct SourceRegistryArguments /// <-- passed to an entry to construct the appropriate type
{
    SourceDescriptor sourceDescriptor;
};

using SourceFactoryFn = std::function<SourceRegistryReturnType(SourceRegistryArguments)>;

/// Creates the registry entry for a source implementation.
template <typename SourceImpl>
SourceFactoryFn makeSourceFactory()
{
    return [](SourceRegistryArguments arguments) -> SourceRegistryReturnType
    { return std::make_unique<SourceImpl>(arguments.sourceDescriptor); };
}

class SourceRegistry : public RuntimeRegistry<SourceRegistry, std::string, SourceFactoryFn, /*CaseSensitive*/ false>
{
public:
    static SourceRegistry& instance();
};

}
```
This specifies the entry type (a factory `std::function`), the arguments an entry receives, and
how an entry is expressed for a plugin type — either a factory template like `makeSourceFactory`
(when construction is uniform over the plugin type) or a static member on the plugin class (when
per-plugin logic is needed, e.g. `&AddLogicalFunction::createAdd`).

The component declares the registry once in its `CMakeLists.txt`:
```cmake
create_runtime_registry(Source nes-sources
        ENTRY_TEMPLATE "makeSourceFactory<${PLUGIN_NAME}Source>()"
        HEADER_TEMPLATE "${PLUGIN_NAME}Source.hpp")
```
and every `add_registry_entry(Source <name>)` generates a small glue translation unit that
describes the entry (an `EntryProvision`, see `nes-common/include/Plugins/PluginDescriptor.hpp`).
Plugins only DESCRIBE what they provide; the actual registration — including duplicate and
missing-registry policies and the `OPTIONAL` entry rule — is performed centrally by the
`PluginLoader` when the host calls `loadBuiltinPlugins()` at startup, or when the `PluginCatalog`
loads a dynamically built plugin (`.so`) at runtime. Built-in and dynamically loaded plugins
share this protocol; only the transport differs (collected function pointers vs. the plugin's
generated `nes_plugin_describe` entry point).

To develop a new plugin, implement the plugin type (and, where the registry uses static members,
the corresponding `create*` member) and add the `add_registry_entry` line — the rest is generated
during the build.

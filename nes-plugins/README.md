## Plugins:

NebulaStream provides a plugin system to load specific components dynamically at runtime.
To this end, plugins implement interfaces and provide additional components, e.g., new data sources/sinks or operators.
Plugins can either build in-tree, e.g., under nes-plugins, or out-of-tree in an separate repository.

### Plugin Definition:

Each plugin implements a specific interface (see [DataSourcePlugin](nes-core/include/Sources/DataSourcePlugin.hpp)).
Furthermore, it uses the associated PluginRegistry to register its plugin in a central singleton. 
Using these singletons a plugin user can invoke each plugin at runtime.

```C++
// Creates a plugin registry that accepts plugins that implement the DataSourcePlugin interface.
using SourcePluginRegistry = Util::PluginRegistry<DataSourcePlugin>;

/// in ArrowSourcePlugin.cpp
// Define implementation of the DataSourcePlugin
class ArrowSourcePlugin : public DataSourcePlugin {
  ....
};

// Register source plugin
[[maybe_unused]] static SourcePluginRegistry::Add<ArrowSourcePlugin> arrowSourcePlugin;

```

### Plugin Loading:

NebulaStream uses a plugin-loader that loads specific plugins at runtime.
In the default case NebulaStream loads all plugins that are located in the plugin directory.
This is either `$BuildDir/nes-plugins` or `bin/nes-plugins` depending on the current environment.

When a plugin was loaded, NebulaStream prints the following message to the console.

```

```

### Current Plugins:

- Arrow: The arrow plugin extends the standard file source and sink with support for the arrow ipc format.
- Tensorflow: The tensorflow plugin, provides a implementation for the model inference operator for tensoflow models.



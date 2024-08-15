# The Problem

The current implementation of configuration in NES is very ambiguous. In general, we define the following problems:

**P1:** Existing configurations include partially implemented/non-implemented features. For instance, enableMonitoring, nodeSpatialType, numberOfBuffersPerEpoch, etc.

**P2:** Cumbersome architecture that supports ambiguous methods (e.g. setValue() and overloaded "=" operator, equals() and overloaded == operator), ambiguous classes (e.g. ConfigurationOption partially repeats functionality of TypedBaseOption) and an outdated file (ConfigurationsNames.hpp) that should contain all possible system configurations, however some are missing. 

**P3:** Existing architecture of configurations include the following hierarchy:

```mermaid
A[BaseOption ] --> B
A --> C
B[TypedOption<T>] -->  E[ScalarOption<T>]
B[TypedOption<T>] -->  F[EnumOption<T>]
B[TypedOption<T>] -->  G[WrapOption<T, Factory>]
C[SequenceOption<T>]
```

_BaseOption:_ A base class for all configuration types.
_TypedOption<T>:_ A template class that provides a generic interface for handling different types of options.
_ScalarOption<T>:_ A specific type of TypedOption<T> that deals with scalar values (e.g., integers, strings, or booleans, i.e. primitive configuration types).
_EnumOption<T>:_ Another specialization of TypedOption<T>, designed to handle enumeration types, allowing for a set of predefined options.
_WrapOption<T, Factory>:_ A specialized TypedOption<T> that encapsulates a complex configuration option. It uses a factory to create or manage instances of the option. For example, worker coordinates, which is a spatial data type in a form of "<long, lat>". 
_SequenceOption<T>:_ A list of configuration options of the same type, typically used together with the WrapOption.

This hierarchy implements the parsing functionality that is not intuitive. This results in developers interpreting it differently (e.g. WrapOption requires a factory to create a composite type, that resulted in PhysicalSourceFactoryPluging and PhysicalSourceTypeFactory)

**P4:** Some configuration options in our test suite are not fully validated in the configuration-related unit test. While we do check certain configurations for type and default value assignment, many configurations are either not tested or only partially tested, lacking comprehensive semantic checks. Additionally, configurations are not included into integration tests, that should make sure that a worker spawns with different configuration scenarios.

**P5:** Unstructured module. The module's structure includes subdirectories: worker, coordinator, enums and validation on the same level. Logically, these subdirectories should not be on the same granularity as they are semantically different. What is worse is that enums are not the only complex type of configuration. We have WrapOption configurations, where each type requires a factory, but they are split between worker and coordinator subdirectories.

# Goals

**G1:** Remove irrelevant configurations (P1).

**G2:** Introduce intuitive configuration architecture (P2 and P5).

**G3:** Support intuitive creation of new configuration options (P3).

**G4:** Ensure comprehensive testing of all configuration options by validating them in both unit and integration tests, including type validation, semantic checks, and scenario-based testing to confirm correct worker behavior under different configurations (P4).

# Non-Goals

NG1: Runtime reconfigurations of a worker, i.e. changing configurations after the launch of the worker. We do not propose a solution for runtime configurations in this document, however, we keep it in mind for the nearest future.
NG2: Configurations for the distributed setup or related to the coordinator.

# Our Proposed Solution

## Remove irrelevant configurations
As part of the first Milestone targeting a stable worker, I propose removing everything unrelated to the single worker and adding the rest later on. To address P1, I propose to leave the following configs (I further mark with "+" configs that stay and with "-" configs that go with an explanation)

WorkerConfiguration.hpp
- workerId (-) // Related to the distributed setup
- localWorkerHost (+) // IP address or hostname of the worker, required for establishing communication with the worker
- coordinatorHost (-) // No coordinator        
- rpcPort (+) // RPC server port of the worker, required for establishing communication with the worker
- dataPort (-) // No ZMQ Server
- coordinatorPort (-) // No coordinator
- numberOfSlots (-) // Related to the heuristics-based placement strategy in the query optimizer
- bandwidth (-) // Related to the distributed setup
- latency (-) // Related to the distributed setup
- numWorkerThreads (+) // Number of threads allocated for parallel processing in the worker
- numberOfBuffersInGlobalBufferManager (+) // Number of buffers allocated in the global buffer pool, determining the memory capacity of the worker
- numberOfBuffersPerWorker (+) // Number of buffers allocated in the local task buffer pool, determining the number of buffers from numberOfBuffersInGlobalBufferManager within a specific task
- numberOfBuffersInSourceLocalBufferPool (+) // Number of buffers allocated in the local source buffer pool, determining the number of buffers from numberOfBuffersInGlobalBufferManager that can be utilized by a source to produce new data
- bufferSizeInBytes (+) // Size of each buffer, in bytes, that is used to pass data through the system
- parentId (-) // Not sure about this config. At the stage of configuring the yaml the only id that is certain is the coordinator id (1). The rest is determined during the launch. For instance, even if you write 3 here and then launch 2 other workers with the parentId 1, it will depend on the order when the workers got launched who gets the id 3. In case one uses docker or Kubernetes to launch workers, this order is undefined. It also depends if the worker gets restarted. It gets even worse with deeper hierarchies. Individual workers have to get started manually to keep the order and assume that none of the workers will get restarted. To address these issues, typically, we use the REST calls "addParent" and "removeParent" to adjust the topology once all nodes are launched. 
- logLevel (+) // The logging level for the system (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)
- sourcePinList (-) // Related to the new implementation of sources
- workerPinList (-) // Related to the distributed setup
- numaAwareness (-) // Can be addressed at the later stages
- enableMonitoring (-) // Partially supported
- monitoringWaitTime (-) // Partially supported
- queryCompiler (-) // Only one compiler currently
- physicalSourceTypes (-) // Related to the new implementation of sources
- locationCoordinates (-) // Partially supported
- nodeSpatialType (-) // Partially supported
- mobilityConfiguration (-) // Partially supported
- numberOfQueues (-) // Part of the optimization
- queuePinList (-) // Related to the numberOfQueues
- numberOfThreadsPerQueue (-) // Related to the numberOfQueues
- numberOfBuffersPerEpoch (-) // Not supported feature
- queryManagerMode (-) // Does not make sense if we do not support numberOfQueues
- enableSourceSharing (-) // Not implemented yet
- workerHealthCheckWaitTime (-) // Related to the distributed setup
- configPath (+) // Path to worker.yaml
- connectSinksAsync (-) // Related to the new implementation of sources
- connectSourceEventChannelsAsync (-) // Related to the new implementation of sources

In terms of the remaining files, it's either all (-) or all (+):

Coordinator: 
- CoordinatorConfiguration (-) // We focus on the worker for right now
- ElegantConfigurations (-) // We focus on the worker for right now
- OptimizerConfiguration (-) // We focus on the worker for right now
- LogicalSourceType (-) // Related to the new implementation of sources
- LogicalSourceType (-) // Related to the new implementation of sources
- SchemaType (-) // Related to the new implementation of sources (Used in LogicalSourceType)

Enum: 
- CompilationStrategy (+) // Indicates the optimization strategy for the query compiler [FAST|DEBUG|OPTIMIZE]
- DistributedJoinOptimizationMode (-) // Related to the distributed setup
- DumpMode (+) // Specifies the mode for dumping intermediate representations during processing [NONE|CONSOLE|FILE|FILE_AND_CONSOLE]
- EnumOption (+) // General enum class 
- EnumOptionDetails (-) // Can be just added to EnumOption.hpp
- MemoryLayoutPolicy (-) // Part of the query optimizer
- NautilusBackend (+) // // Specifies the backend used by the Nautilus system [INTERPRETER|MLIR_COMPILER_BACKEND]
- OutputBufferOptimizationLevel (-) // Part of a partially implemented feature
- PipeliningStrategy (-) // Part of a partially implemented feature
- PlacementAmendmentMode (-) // Part of the query optimizer
- QueryCompilerType (-) // Only Nautilus for now
- QueryExecutionMode (-) // Part of the optimization
- QueryMergerRule (-) // Part of the optimization
- StorageHandlerType (-) // Coordinator config
- WindowingStrategy (-) // Only slicing for now

Validation: All (+) // subdirectory that includes validation classes for primitive types, e.g. int, string, etc.

Worker:
PhysicalSourceTypes: All (-) // Related to the new implementation of sources
- GeoLocationFactory (-) // Partially implemented feature
- PhysicalSourceFactoryPlugin (-) // Related to the new implementation of sources
- PhysicalSourceTypeFactory (-) // Related to the new implementation of sources
- QueryCompilerConfiguration (-) // Will be merged to a file with individual component configurations 
- WorkerConfiguration (-) // Will be split to individual components
- WorkerMobilityConfiguration (-) // Partially implemented feature

The rest of the files without a specific type are related to the overall configuration hierarchy: 
- BaseConfiguration (-) // Not needed due to the proposed new architecture for configurations
- BaseOption (-) // Not needed due to the proposed new architecture for configurations
- ConfigOptions.md (-) // Can be moved to the documentation
- ConfigurationException (-) // Related to the new exception handling
- ConfigurationOption (-) // Can be merged with the TypedBaseOption. They basically do the same, except that ConfigurationOption handles magic_enum.
- ConfigurationNames (+) // Adjust to the names of configurations that are left, remove the note
- ScalarOption (-) // Not needed due to the proposed new architecture for configurations
- SequenceOption (+) // Potentially is not used by the configurations that will be currently left in the system, however, is a valid type for future configurations
- TypedBaseOption (-) // Not needed due to the proposed new architecture for configurations
- WorkerConfigurationKeys.hpp (-) // Include TENSOR_FLOW, JAVA_UDF, MOBILITY, SPATIAL_TYPE, OPENCL_DEVICES
- WorkerPropertyKeys (-) // SLOTS, LOCATION, MAINTENANCE are irrelevant for the current milestone, DATA_PORT, GRPC_PORT can be moved to ConfigurationNames.hpp. I am not sure what is ADDRESS (it is never used)
- WrapOption (+) // It is currently used only for Sources and Spatial things. However, it's a good code basis for the future complex configuration types

The "+" and "-" are related to both hpp and cpp files.

## Proposed architecture for configurations

Our proposed solution implements decentralized configuration architecture similar to [ClickHouse](https://clickhouse.com/docs/en/operations/configuration-files). 
A worker stores all configurations, and components proactively pull configurations from a specialized structure that is created after validation of individual configuration options.
To this end, we propose:

1) a singleton ConfigurationManager class that reads, validates and stores all the configurations at the startup.

```c++

using ConfigValue = std::variant<std::string, int, bool, std::shared_ptr<std::unordered_map<std::string, ConfigValue>>>;

class ConfigurationManager {
	public:
   		static ConfigurationManager& getInstance();
   		
   		/// loadConfigurations calls loadYaml first to fill configValues
	    /// and then rewrites some of them with the command line input
   		void loadConfigurations(int argc, char* argv[]);
   		
   		template <typename T>
    	T getValue(const std::string& key, const T& defaulValue) const;
    	
	private:
		/// parsing can be done using YAML::LoadFile from the yaml-cpp library
		loadYaml(const std::string& filename);
		std::unordered_map<std::string, ConfigValue> configValues;
```

Having one file that is responsible for reading, validating and storing configuration options is intuitive (P2, P5) in contrast to having approx. 40 classes as we have currently. 
Additionally, with physical sources being registered as part of queries, there is no need in complex types of configuration options. Therefore, there is no need in a complicated hierarchy of configuration types (P3). 
Moreover, this approach can be nicely adapted to the centralized runtime reconfiguration approaches (e.g. query-based, where new configurations are submitted via queries), as it stores all options centrally.

2) Use individual components classes to store configuration.

```c++
class NetworkConfig {
public:
    void loadConfig() {
        auto& configManager = ConfigurationManager::getInstance();
        hostName = configManager.getValue<std::string>("hostName", "localhost");
        dataPort = configManager.getValue<uint64_t>("dataPort", 3001);
        
        /// and getters
    }
private:
    std::string hostName;
    uint64_t dataPort;
};

class QueryManagerConfig {
public:
    void loadConfig() {
        auto& configManager = ConfigurationManager::getInstance();
        numberOfBuffersPerWorker = configManager.getValue<uint64_t>("numberOfBuffersPerWorker", 128);
        numberOfBuffersInSourceLocalBufferPool = configManager.getValue<uint64_t>("numberOfBuffersInSourceLocalBufferPool", 64);
        
        /// and getters
    }
private:
    uint64_t numberOfBuffersPerWorker;
    uint64_t numberOfBuffersInSourceLocalBufferPool;
};
```

This approach makes intuitive creation of new configuration options (P3), as they logically distributed on per-component basis. 
Additionally, these classes can include further semantic-based validation. 

Each component can then load the necessary configurations.

```c++
class NetworkComponent {
public:
NetworkComponent(const NetworkConfig& config) : config(config) {}

    void initialize() {
        uint64_t hostName = config.getHostName();
        uint64_t dataPort = config.getDataPort();
        
        /// Spawn a ZQM server with a given hostName and port
    }

private:
NetworkConfig config;
};
```

This implementation is a bit not straightforward as it can be simplified by removing the intermediate component configuration classes and requires synchronisation with global configurations in case of updates. 
However,  having these classes allows us to keep track of existing configuration options and avoid duplicates in the future.

## Configuration testing

### Unit Tests

We update the current unit test to include all system configurations. We then provide the following tests:

- **Default Configuration:** Test that the system correctly loads and applies default configuration values when no configuration is provided
- **Custom Configuration:** Test that the system correctly loads and applies custom configurations from a yaml file or command-line
- **Override Behavior:** Test scenarios where command-line arguments override configuration file options
- **Type Validation:** Test how the system handles invalid or malformed configurations, such as incorrect data types, missing required fields, or unsupported values
- **Semantic Validation:** Test the behavior of the system when specific configuration options set to minimum, maximum, or dependent values
- **Interaction with Components:** Ensure that components correctly read and react to configuration changes

### Integration Tests

- **Worker Initialization with Configurations:** Test that the system successfully spawns a worker using various configurations (default, custom, and overridden with yaml/command line).

# Alternatives

## Existing Solutions

As part of existing solutions, we investigated MongoDB, RockDB, ClickHouse, MySQL, and FoundationDB. Below is a description of how these systems handle configurations classified by centralized, hybrid, and decentralized solutions.

### Centralized

**MongoDB**
In terms of a single MongoDB instance, the configuration loading, parsing, and validation process are centralized within that instance before settings are applied to their respective components like storage, network, and others.

1. Configuration Loading:
- When a MongoDB instance (mongod or mongos) starts up, it first loads the configuration from the specified sources. These can be a configuration file (typically YAML), command-line arguments, or environment variables.
2. Parsing and Validation:
- The loaded configurations are parsed and validated in a centralized manner. This means that all configurations are read and checked for correctness and validity (such as data types, range constraints, and dependencies among settings) before any operational components (like the network or storage engines) start up.
- This step ensures that only valid configurations are applied to the system components, preventing runtime failures due to misconfigurations.
3. Application of Settings:
- After validation, the settings are then distributed internally to the various components of the MongoDB instance:
4. Dynamic Reconfiguration:
- Some settings can be changed at runtime without restarting the instance. These changes are typically made via administrative commands that apply the new settings immediately to the relevant system component. Even these changes follow a centralized validation process within the instance before they are applied.

**RockDB**

1. Configuration Loading:
- When a RocksDB instance is initialized, it first loads configuration settings from the application code where the database is embedded. Configurations are typically set up through various C++ structs such as Options, DBOptions, and ColumnFamilyOptions.
- These configuration objects can be manually set in the code or loaded from an external source like a configuration file processed by the application, but RocksDB itself does not natively parse files or environment variables.
2. Parsing and Validation:
- Configuration objects like Options are filled with settings programmatically. Each setting is applied to the object properties directly in the code.
- As the configuration settings are applied, RocksDB internally validates these settings. For example, write_buffer_size must be within a practical and permissible range to ensure it's operational.
3. Application of Settings:
- Once validated, the settings contained within these configuration objects are applied to the respective components of the RocksDB instance.
4. Dynamic Reconfiguration:
- Some options in RocksDB can be adjusted dynamically at runtime using methods like SetOptions(). This allows developers to modify certain parameters without needing to restart the database or application.
- Dynamic reconfigurations are validated in a similar manner to initial configurations to ensure they are within allowable ranges and do not conflict with other operational parameters.


**MYSQL**
1. Configuration Loading:
- When a MySQL instance (mysqld) starts up, it first loads the configuration from specified sources. These sources typically include configuration files (my.cnf or my.ini), which are the primary method for setting configuration options. Additionally, command-line arguments can be used to override settings in the configuration files, and in some cases, environment variables might influence settings.
- Configuration files can exist in multiple standard locations, and MySQL reads these files in a specific order, allowing settings in files read later to override those read earlier.
2. Parsing and Validation:
- The loaded configurations are parsed in a centralized manner as MySQL starts. This process involves reading/parsing the settings specified in the configuration files and command-line arguments, and integrating these into the server's operational parameters.
- Each configuration setting is validated for correctness and applicability. This includes checking data types (ensuring string or numeric values are appropriate), range constraints (ensuring numeric values fall within acceptable limits), and dependencies among settings (e.g., buffer pool size relative to available system memory).
3. Application of Settings:
- The application of settings is managed through a centralized mechanism where, after validation, the configuration values are stored in internal data structures. These structures are accessible by various components of the MySQL server, such as the storage engines or query processor. Each component retrieves the necessary settings from these centralized data structures and applies them internally.
4. Dynamic Reconfiguration:
- MySQL supports dynamic changes to certain settings while the server is running. These changes can be made through SQL commands, such as SET GLOBAL or SET SESSION, allowing administrators to adjust operational parameters without restarting the server.
- Dynamic changes are validated in the same thorough manner as at startup, ensuring that adjustments do not compromise the stability or security of the server.

### Hybrid

**ClickHouse**

ClickHouse uses XML files (config.xml and users.xml) for configuration, which is somewhat less common in modern DBMSs that often prefer simpler formats like YAML or JSON. XML allows for a more structured and hierarchical configuration, which can be particularly advantageous for complex setups with many nested options.

1. Configuration Loading:
- All configuration settings for ClickHouse are defined in XML files, such as config.xml for system settings and users.xml for user and access management settings. The configurations are stored in objects that can be accessed globally within the ClickHouse server process.
- Each component within ClickHouse — whether it's the server itself, various storage engines, or network interfaces — accesses the configuration settings it requires from this centralized data structure as needed. This approach is somewhat like a "pull" model, where each component retrieves its specific configuration parameters.
- The components do not receive configurations passively; instead, they actively query the central configuration for the parameters relevant to their operational context.
2. Parsing and Validation:
- At startup, ClickHouse parses these XML files, loading settings into memory. During this initial load, some level of validation and parsing converts XML data into internal data structures that are more efficiently accessed by the server components.
3. Application of Settings:
- Server Configuration: Settings related to server operation, such as port numbers, memory limits, and path configurations, are applied to the server. This step configures how the server behaves in terms of network communication, data storage, and process management.
- Database and Table Settings: ClickHouse allows setting configurations at the database and table levels, influencing how data is stored, compressed, and accessed.
- User Profiles and Permissions: Configurations related to user profiles, access rights, and resource quotas are set up based on users.xml, defining how users interact with ClickHouse and what operations they are allowed to perform.
4. Dynamic Reconfiguration:
- For runtime modifications, certain settings can be dynamically changed through system management interfaces, such as SQL queries that adjust operational parameters. These changes are then reflected back into the central configuration state.
- Consistency and Coordination: For changes that affect the whole cluster, such as cluster-wide settings in a distributed ClickHouse setup, ClickHouse manages consistency by propagating changes across all nodes and ensuring that new settings do not disrupt ongoing operations.

### Decentralized

**FoundationDB**

In terms of a single FoundationDB instance, the configuration process is decentralized and is designed to efficiently handle runtime changes across all client and server components.

1. Configuration Loading:
- FoundationDB does not rely on a single central configuration file at startup. Instead, configurations can be specified through command-line options or set programmatically in the database itself.
2. Parsing and Validation:
- Instead of a single central validation step before starting operations, FoundationDB uses a global configuration framework. This framework broadcasts updates to configuration values, which are then independently managed and applied by each machine in the cluster. This setup allows for configurations to be adjusted on-the-fly without needing to restart processes or interfere with ongoing operations.
3. Application of Settings:
- In FoundationDB, the terms "client" and "server" refer to different roles within the distributed system. The client refers to the applications or processes that interact with the database to read and write data, while the server refers to the FoundationDB processes that handle data storage, coordination, and transaction management.
- Configuration changes in FoundationDB are applied almost in real-time through the global configuration key space. Configuration is pull-based, with each component such as servers and clients utilize an event-driven watch mechanism to monitor the global configuration key space. This means that instead of polling at regular intervals, components are immediately notified when a configuration change occurs.
- The system uses an eventually consistent model to ensure that all components across the cluster eventually synchronize on the configuration settings without necessitating simultaneous downtime or reboots.
4. Dynamic Reconfiguration:
- FoundationDB supports dynamic changes to configurations without service interruption. Changes are propagated through special transactions in the database and do not require direct file edits or command-line interventions post-initial setup.

## Solutions for a Distributed Setting

Overall, there are two ways how systems interpret centralized vs. decentralized: coordinator and workers level, and a worker and it's components level. We will discuss all of these options further.

### Centralized (Coordinator-Worker)

**SA1.** The coordinator stores all the configurations and propagates them to workers
- **Disadvantage:** This approach can be inefficient as the coordinator can become a bottleneck in the case of thousands/millions of potential devices.
- **Disadvantage:** Assumes homogeneity of workers — in case different workers will require different configurations it will require providing a worker id for specific configurations. However that is not known before the worker launch.

**SA2.** The coordinator stores all the configurations, and workers pull necessary configurations as needed.
- **Advantage:** Ensures that all workers have access to the latest configurations without storing them locally, which can simplify updates and maintenance.
- **Disadvantage:** Assumes homogeneity of workers — in case different workers will require different configurations it will require providing a worker id for specific configurations. However that is not known before the worker launch.

_Enhancement for Both:_ Utilize ZooKeeper to store configurations, allowing for centralized management with decentralized access.

### Decentralized (Coordinator-Worker)

#### Centralized (Worker-Components)

**SB1.** The worker stores all configurations, perform per-type validation and distributes them across different components (current implementation)
- **Advantage:** No duplicates in configurations options (e.g. numberOfBuffersPerEpoch in fault-tolerance participates in the network stack, final sinks, query optimizer)
- **Advantage:** No redundant type-based (Int, Bool, String, Array, etc.) validations
- **Disadvantage:** No support for runtime reconfigurations. However, it could be implemented via query API
- **Disadvantage:** No semantic-based validation (for example, numberOfBuffersInSourceLocalBufferPool cannot be larger than available memory)

**SB2.** The worker stores all configurations, perform per-component validation and distributes them across different components
- **Advantage:** Semantic based validation
- **Advantage:** No redundant type-based validations
- **Disadvantage:** No support for runtime reconfigurations. However, it could be implemented via query API
- **Disadvantage:** Duplicates in configurations options

#### Hybrid (Worker-Components)

**SC1.**  Worker stores all configurations, and components proactively pull configurations from a specialized structure that is created after validation of individual configuration options.
- **Advantage:** No duplicates in configurations options
- **Advantage:** No redundant type-based validations
- **Advantage:** Support of runtime reconfigurations
- **Disadvantage:** Imposes additional overhead on components, distracting them from their primary tasks and possibly affecting performance.
- **Disadvantage:** No semantic-based validation

#### Decentralized (Worker-Components)

**SD1.** All components store and manage their own configurations independently.
- **Advantage:** Maximizes autonomy and responsiveness, as each component can quickly adapt configurations without dependencies.
- **Advantage:** Semantic-based validation
- **Disadvantage:** Risk of configuration drift where components become out of sync, leading to potential inconsistencies and duplication of configuration data across the system.
- **Disadvantage:** Duplicates in configurations options
- **Disadvantage:** Redundant typed-based validators. Could be potentially avoided if implemented in Util.
- **Disadvantage:** No support for runtime reconfigurations. Potential difficulties implementing it without pushing extra burdain on the user to be aware of existing system subcomponents.

# Open Questions

Do we want to keep component related classes with configs or load configurations directly from the components themselves?

# (Optional) Sources and Further Reading

[MongoDB Configuration Options] (https://www.mongodb.com/docs/manual/reference/configuration-options/)

[Runtime Configurations MongoDB] (https://www.mongodb.com/docs/manual/reference/configuration-options/)

[How to Create, Use, and Customize MongoDB Configuration Files] (https://hevodata.com/learn/mongodb-configuration-files/)

[RocksDB Configuration Options] (https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)

[ClickHouse Configuration Files] (https://clickhouse.com/docs/en/operations/configuration-files/)

[MySQL Server System Variables] (https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html)

[MySQL Option Files] (https://dev.mysql.com/doc/refman/8.0/en/option-files.html)

[FoundationDB Configurations] (https://apple.github.io/foundationdb/configuration.html)

[Overview of DuckDB Configurations] (https://duckdb.org/docs/configuration/overview)



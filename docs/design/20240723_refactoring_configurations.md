# The Problem

The current implementation of configuration in NES is very ambiguous. In general, we define the following problems:

P1: Existing configurations include partially implemented/non-implemented features. For instance, enableMonitoring, nodeSpatialType, numberOfBuffersPerEpoch, etc.

P2: Different methods to create different configurations. For instance, to create a physicalSource configuration (a WrapOption type), we use PhysicalSourceFactoryPlugin and PhysicalSourceTypeFactory. To create locationCoordinates (also a WrapOption), we only use GeoLocationFactory.

P3: Different places to define new configuration names. Specifically, most config names are defined in ConfigurationNames.hpp. However, some names are defined in WorkerConfigurationKeys.hpp and WorkerPropertyKeys.hpp.

P4: Most of the configurations are not tested.

P5: Unstructured module. The module's structure includes subdirectories: worker, coordinator, enums and validation on the same level. Logically, these subdirectories should not be on the same granularity as they are semantically different. What is worse is that enums are not the only complex type of configuration. We have WrapOption configurations, where each type requires a factory, but they are split between worker and coordinator subdirectories.

# Goals

G1: Remove irrelevant configurations.

G2: Create documentation on a common way to create a configuration option of a non-primitive type.

G3: Move all configuration names to ConfigurationNames.hpp.

G4: Make sure all the configuration options that are left are tested.

G5: Create a new module structure. 

# Non-Goals

NG1: Runtime configurations.
NG2: Configurations for the distributed setup or related to the coordinator.

# Our Proposed Solution

## Remove irrelevant configurations
As part of the first Milestone targeting a stable worker, I propose removing everything unrelated to the single worker and adding the rest later on. To address P1, I propose to leave the following configs (I further mark with "+" configs that stay and with "-" configs that go with an explanation)

WorkerConfiguration.hpp
- workerId (+)
- localWorkerHost (+)
- coordinatorHost (-) // No coordinator        
- rpcPort (+)
- dataPort (-) // No ZMQ Server
- coordinatorPort (-) // No coordinator
- numberOfSlots (-) // Related to the heuristics-based placement strategy in the query optimizer
- bandwidth (-) // Related to the distributed setup
- latency (-) // Related to the distributed setup
- numWorkerThreads (+)
- numberOfBuffersInGlobalBufferManager (+)
- numberOfBuffersPerWorker (+)
- numberOfBuffersInSourceLocalBufferPool (+)
- bufferSizeInBytes (+)
- parentId (-) // Not sure about this config. At the stage of configuring the yaml the only id that is certain is the coordinator id (1). The rest is determined during the launch. For instance, even if you write 3 here and then launch 2 other workers with the parentId 1, it will depend on the order when the workers got launched who gets the id 3. In case one uses docker or Kubernetes to launch workers, this order is undefined. It also depends if the worker gets restarted. It gets even worse with deeper hierarchies. Individual workers have to get started manually to keep the order and assume that none of the workers will get restarted. To address these issues, typically, we use the REST calls "addParent" and "removeParent" to adjust the topology once all nodes are launched. 
- logLevel (+)
- sourcePinList (-) // Related to the new implementation of sources
- workerPinList (-) // Related to the distributed setup
- numaAwareness (-) // Can be addressed at the later stages
- enableMonitoring (-) // Partially supported
- monitoringWaitTime (-) // Partially supported
- queryCompiler (+)
- physicalSourceTypes (-) // Related to the new implementation of sources
- locationCoordinates (-) // Partially supported
- nodeSpatialType (-) // Partially supported
- mobilityConfiguration (-) // Partially supported
- numberOfQueues (-) // Part of the optimization
- queuePinList (-) // Related to the numberOfQueues
- numberOfThreadsPerQueue (-) // Related to the numberOfQueues
- numberOfBuffersPerEpoch (-) // Not supported feature
- queryManagerMode (-) // Does not make sense if we do not support numberOfQueues
- enableSourceSharing (+)
- workerHealthCheckWaitTime (-) // Related to the distributed setup
- configPath (+) 
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
- CompilationStrategy (+)
- DistributedJoinOptimizationMode (-) // Related to the distributed setup
- DumpMode (+)
- EnumOption (+)
- EnumOptionDetails (-) // Can be just added to EnumOption.hpp
- MemoryLayoutPolicy (-) // Part of the query optimizer
- NautilusBackend (+)
- OutputBufferOptimizationLevel (-) // Part of a partially implemented feature
- PipeliningStrategy (-) // Part of a partially implemented feature
- PlacementAmendmentMode (-) // Part of the query optimizer
- QueryCompilerType (-) // Only Nautilus for now
- QueryExecutionMode (-) // Part of the optimization
- QueryMergerRule (-) // Part of the optimization
- StorageHandlerType (-) // Coordinator config
- WindowingStrategy (-) // Only slicing for now

Validation: All (+)

Worker:
PhysicalSourceTypes: All (-) // Related to the new implementation of sources
- GeoLocationFactory (-) // Partially implemented feature
- PhysicalSourceFactoryPlugin (-) // Related to the new implementation of sources
- PhysicalSourceTypeFactory (-) // Related to the new implementation of sources
- QueryCompilerConfiguration (+)
- WorkerConfiguration (+)
- WorkerMobilityConfiguration (-) // Partially implemented feature

The rest of the files without a specific type are related to the overall configuration hierarchy: 
- BaseConfiguration (+)
- BaseOption (+)
- ConfigOptions.md (-) // Can be moved to the documentation
- ConfigurationException (-) // Related to the new exception handling
- ConfigurationOption (-) // Can be merged with the TypedBaseOption. They basically do the same, except that ConfigurationOption handles magic_enum.
- ConfigurationNames (+) // Adjust to the names of configurations that are left, remove the note
- ScalarOption (+)
- SequenceOption (+) // Potentially is not used by the configurations that will be currently left in the system, however, is a valid type for future configurations
- TypedBaseOption (+)
- WorkerConfigurationKeys.hpp (-) // Include TENSOR_FLOW, JAVA_UDF, MOBILITY, SPATIAL_TYPE, OPENCL_DEVICES
- WorkerPropertyKeys (-) // SLOTS, LOCATION, MAINTENANCE are irrelevant for the current milestone, DATA_PORT, GRPC_PORT can be moved to ConfigurationNames.hpp. I am not sure what is ADDRESS (it is never used)
- WrapOption (+) // It is currently used only for Sources and Spatial things. However, it's a good code basis for the future complex configuration types

The "+" and "-" are related to both hpp and cpp files.

## Create documentation on a common way of how to create complex configurations.

We propose the following subsection in the documentation of configuration:

"To use WrapOption in a configuration class, follow these steps:

1. Define a Factory Class:

class XFactory {
public:
    static X createFromString(std::string, std::map<std::string, std::string>& commandLineParams); {
        // Logic to create X from string with custom validation
    }

    static X createFromYaml(Yaml::Node& yamlConfig) {
        // Logic to create X from YAML node with custom validation
    }
};

2. Define a WrapOption in the configuration file:

WrapOption<X, XFactory> xOption = {X, "Description of XOptions's configuration.");

A WrapOption can also be combined with a SequenceOption to create a composite configuration of a complex type:

SequenceOption<WrapOption<X, XFactory>> XOptions = {X, "Description of XOptions's configuration."); 

3. Write tests for valid and invalid values of created configuration in ConfigTest."

## Move all configuration names to ConfigurationNames.hpp.

In ConfigurationNames.hpp, we adjust the names of configurations that are left, and remove the note.
We remove WorkerConfigurationKeys.hpp because it includes TENSOR_FLOW, JAVA_UDF, MOBILITY, SPATIAL_TYPE, and OPENCL_DEVICES, which are not relevant to the stable worker but are relevant for partially implemented features. We also remove WorkerPropertyKeys.hpp because SLOTS, LOCATION, and MAINTENANCE are irrelevant for the current milestone. DATA_PORT and GRPC_PORT can be moved to ConfigurationNames.hpp, and the ADDRESS is never used.

## Make sure all the configuration options that are left are tested.

- We update the current test suite to include all system configurations.
- The configurations should be included in all four types of the current test suite:
	- Valid configuration option from the YAML file
	- Valid configuration option from the command line
	- Invalid configuration option from the YAML file
	- Invalid configuration option from the command line 
- Valid configuration tests check for every option a valid value and an empty value (a default value should be assigned).
- Invalid configuration tests check for every option a value of a wrong type, a semantically incorrect value (out of range), or an incorrect value that is specific for the type (in case applicable) (e.g., impossible number of buffers per global buffer pool).

## Create a new module structure. 

Current module structure:

- nes-configurations:
	- include
		- Configurations:
			- Coordinator
			- Enums
			- Validation
			- Worker
			*Other Configuration files related to existing configuration types and names*
	- src
	- tests

The proposed structure of the module includes:

- nes-configurations:
	- include
		- Core
			- BaseOption.hpp
			- BaseConfiguration.hpp
			- ConfigurationNames.hpp
		- ConfigurationTypes 
			- Worker: // Will be extended to contain Coordinator
                		- WorkerConfiguration.hpp 
                		- QueryCompilerConfiguration.hpp
		- OptionTypes
			- ScalarOption.hpp 
			- TypedBaseOption.hpp
            		- SequenceOption.hpp
            		- WrapOptions: // Potentially, all factories can be placed here
				- WrapOption.hpp 
			- Enums:
            			- CompilationStrategy.hpp
            			- DumpMode.hpp
            			- NautilusBackend.hpp
            			- OutputBufferOptimizationLevel.hpp
            			- PipeliningStrategy.hpp
            			- QueryCompilerType.hpp
            			- WindowingStrategy.hpp
				- EnumOption.hpp // General enum option
				
		- Validation
			// All validation classes
			
	- src
	- tests

# Minimal Viable Prototype

This work is refactoring of an existing solution.

# Alternatives

Centralized, hybrid and decentralized configuration architectures of exiting systems:

## Existing Solutions

### Centralized

#### MongoDB

1.	Configuration Loading:
	  -	When a MongoDB instance (mongod or mongos) starts up, it first loads the configuration from the specified sources. These can be a configuration file (typically YAML), command-line arguments, or environment variables.
2.	Parsing and Validation:
	  -	The loaded configurations are parsed and validated in a centralized manner. This means that all configurations are read and checked for correctness and validity (such as data types, range constraints, and dependencies among settings) before any operational components (like the network or storage engines) start up.
	  -	This step ensures that only valid configurations are applied to the system components, preventing runtime failures due to misconfigurations.
3.	Application of Settings:
	  -	After validation, the settings are then distributed internally to the various components of the MongoDB instance.
4.	Dynamic Reconfiguration:
	  -	Some settings can be changed at runtime without restarting the instance. These changes are typically made via administrative commands that apply the new settings immediately to the relevant system component. Even these changes follow a centralized validation process within the instance before they are applied.

#### RocksDB

1. Configuration Loading:
   -	When a RocksDB instance is initialized, it first loads configuration settings from the application code where the database is embedded. Configurations are typically set up through various C++ structs such as Options, DBOptions, and ColumnFamilyOptions.
   -	These configuration objects can be manually set in the code or loaded from an external source like a configuration file processed by the application, but RocksDB itself does not natively parse files or environment variables.
2. Parsing and Validation:
   -	Configuration objects like Options are filled with settings programmatically. Each setting is applied to the object properties directly in the code.
   -	As the configuration settings are applied, RocksDB internally validates these settings. For example, write_buffer_size must be within a practical and permissible range to ensure it's operational. If any setting fails its validation checks (e.g., an excessively large buffer size or an invalid path), RocksDB will not proceed with the database initialization and will return an error.
   -	This step ensures that only valid configurations are applied to the database engine, preventing runtime failures due to misconfigurations.
3. Application of Settings:
   -	Once validated, the settings contained within these configuration objects are applied to the respective components of the RocksDB instance.
4.  Dynamic Reconfiguration:
   -	Some options in RocksDB can be adjusted dynamically at runtime using methods like SetOptions(). This allows developers to modify certain parameters without needing to restart the database or application.
   -	Dynamic reconfigurations are validated in a similar manner to initial configurations to ensure they are within allowable ranges and do not conflict with other operational parameters.

#### MySQL

1. Configuration Loading:
   - When a MySQL instance (mysqld) starts up, it first loads the configuration from specified sources. These sources typically include configuration files (my.cnf or my.ini), which are the primary method for setting configuration options. Additionally, command-line arguments can be used to override settings in the configuration files, and in some cases, environment variables might influence settings. 
   - Configuration files can exist in multiple standard locations, and MySQL reads these files in a specific order, allowing settings in files read later to override those read earlier.
2. Parsing and Validation:
   - The loaded configurations are parsed in a centralized manner as MySQL starts. This process involves interpreting the settings specified in the configuration files and command-line arguments, and integrating these into the server's operational parameters.
   - Each configuration setting is validated for correctness and applicability. This includes checking data types (ensuring string or numeric values are appropriate), range constraints (ensuring numeric values fall within acceptable limits), and dependencies among settings (e.g., buffer pool size relative to available system memory).
3. Application of Settings:
   - After successful validation, the settings are applied internally to the various components of the MySQL instance.
4. Dynamic Reconfiguration:
   - MySQL supports dynamic changes to certain settings while the server is running. These changes can be made through SQL commands, such as SET GLOBAL or SET SESSION, allowing administrators to adjust operational parameters without restarting the server.
   - Dynamic changes are again validated 

### Hybrid

#### ClickHouse

ClickHouse uses XML files (config.xml and users.xml) for configuration, which is somewhat less common in modern DBMSs that often prefer simpler formats like YAML or JSON. XML allows for a more structured and hierarchical configuration, which can be particularly advantageous for complex setups with many nested options.

1. Configuration Loading:
   - All configuration settings for ClickHouse are defined in XML files, such as config.xml for system settings and users.xml for user and access management settings. These files act as the central repository of all configuration data.
   - These files act as the central repository of all configuration data.
2. Parsing and Validation:
   - When a ClickHouse server starts, it parses the XML configuration files. This process is centralized and handled by the main server process. The XML format allows complex hierarchical configurations, suitable for defining detailed settings and policies.
   - During the parsing process, ClickHouse validates the configuration settings. This includes checking data types, validating the format of specified values (like IP addresses and ports), and ensuring that dependent settings are correctly configured (e.g., ensuring that settings related to replication are consistent across the cluster).
3. Application of Settings:
   - Each component within ClickHouse accesses the configuration settings it requires from this centralized repository as needed. This approach is somewhat like a "pull" model, where each component retrieves its specific configuration parameters.
   - The components actively query the central configuration for the parameters relevant to their operational context.
4. Dynamic Reconfiguration:
   - Runtime Changes: ClickHouse supports changing some settings dynamically at runtime through SQL commands, such as ALTER SYSTEM. This allows administrators to adjust and tune system settings without restarting the server.
   - Consistency and Coordination: For changes that affect the whole cluster, such as cluster-wide settings in a distributed ClickHouse setup, ClickHouse manages consistency by propagating changes across all nodes and ensuring that new settings do not disrupt ongoing operations.

### Decentralized

#### FoundationDB

1. Configuration Loading:
   - FoundationDB does not rely on a single central configuration file at startup. Instead, configurations can be specified through command-line options or set programmatically in the database itself.
2. Parsing and Validation:
   - Instead of a single central validation step before starting operations, FoundationDB uses a global configuration framework. This framework broadcasts updates to configuration values, which are then independently managed and applied by each machine in the cluster. This setup allows for configurations to be adjusted on-the-fly without needing to restart processes or interfere with ongoing operations.
3. Application of Settings:
   - Configuration changes in FoundationDB are applied almost in real-time through the global configuration key space. Each component, whether client or server, checks this key space and applies updates to its operational parameters independently. This includes settings for network operations, data storage, and other critical functions.
   - The system uses an eventually consistent model to ensure that all components across the cluster eventually synchronize on the configuration settings without necessitating simultaneous downtime or reboots.
4. Dynamic Reconfiguration:
   - FoundationDB supports dynamic changes to configurations without service interruption. Changes are propagated through special transactions in the database and do not require direct file edits or command-line interventions post initial setup.
   - This feature is particularly important in environments that require high availability and minimal downtime.

## Potential Solutions

### Centralized

1. The coordinator stores everything and sends it to workers
-	Disadvantage: This approach is not efficient because workers require configurations at launch, and waiting for the coordinator to send configurations can delay startup processes.

2. The coordinator stores all configurations, and workers pull necessary configurations as needed.
-	Advantage: Ensures that all workers have access to the latest configurations without storing them locally, which can simplify updates and maintenance.
-	Disadvantage: Assumes heterogeneity—different workers might require different configurations, which could complicate management and scaling in diverse environments.

Enhancement for Both: Utilize ZooKeeper to store configurations, allowing for centralized management with decentralized access.

### Decentralized

1. The worker stores all configurations and distributes them across different components – current implementation

2. Worker stores all configurations, and components proactively pull configurations from a specialized structure.
-	Disadvantage: Imposes additional overhead on components, distracting them from their primary tasks and possibly affecting performance.

3. All components store and manage their own configurations independently.
-	Advantage: Maximizes autonomy and responsiveness, as each component can quickly adapt configurations without dependencies.
-	Disadvantage: Risk of configuration drift where components become out of sync, leading to potential inconsistencies and duplication of configuration data across the system.

# Open Questions

# (Optional) Sources and Further Reading

# (Optional) Appendix

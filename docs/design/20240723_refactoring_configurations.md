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

An alternative solution can be using an xml configuration and treating each configuration option as a string, performing validation only on the semantical level directly at a specific component. For example, ClickHouse provides both xml and yaml configuration options.

# Open Questions

# (Optional) Sources and Further Reading

# (Optional) Appendix

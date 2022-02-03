
## Design of new Configuration Options:

### Requirements:
1. Definition of options should not require to write any boilerplate except the definition of the individual options.
2. Parsing of YAML and command line fields should require minimal manual effort.
4. Types of Options
   - Scalar Values -> Int, String, Bool (e.g., rpcPort)
   - Enumerations -> Specific values of en Enum entry (e.g., Optimizer::QueryMergerRule)
   - Nested Options -> Allows nesting of configurations into each other 
   - SequenceOptions -> A list of options only applicable to yaml configuration 
   - Custom Objects -> Uses a custom class as an option (e.g., PhysicalSource) and uses an Factory to construct the custom object (e.g., PhysicalSourceFactory).

### Design:


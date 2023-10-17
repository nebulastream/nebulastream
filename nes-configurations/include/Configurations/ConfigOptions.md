
## Design of new Configuration Options:

### Requirements:
1. Definition of options should not require to write any boilerplate except the definition of the individual options.
2. Parsing of YAML and command line fields should require minimal manual effort.
4. Types of Options
   - Scalar Values -> Int, String, Bool (e.g., rpcPort)
   - Enumerations -> Specific values of en Enum entry (e.g., Optimizer::QueryMergerRule)
   - Nested Options -> Allows nesting of configurations into each other 
   - SequenceOptions -> A list of options only applicable to yaml configuration 
   - Custom Objects -> Uses a custom class as an option (e.g., `PhysicalSource`) and uses an Factory to construct the custom object (e.g., `PhysicalSourceTypeFactory`).

### Design:

We declare four Option types, which inherit from `BaseOption`:
1. `ScalarOptions<T>` -> implemented as `IntOption`, `BoolOption`, `StringOption`.
2. `EnumOption<T>` -> where `T` is of type enum. Accepts only instances of the enum as values.
3. `SequenceOption<T>` -> where `T` is a option type. Stores a sequence of option values.
4. `WrapOption<T, Factory>` -> where `T` can be arbitrary type and Factory provides a factory to create a object of type T from a YAML node or command line argument.

**Configurations:**  
A configuration inherits from the `BaseConfiguration` type and declares a set of option, 
which are subtypes of the BaseOption type.
The `BaseConfiguration` class provides a generic implementation to load configuration values from YAML files and command line parameters.

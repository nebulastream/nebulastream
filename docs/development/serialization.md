# Serialization of Logical Operators, Logical Functions, Traits, ... 

We utilize reflection to enable a flexible serialization of logical operators, and logical functions.
While we use an external library (reflect-cpp) for reflection, we wrap the reflection functionality in 
a custom interface (see `Utils/Reflection.hpp` in nes-commons).

## Adding reflection to a Logical Operator, Logical Function, Trait, or other 

1. In the class header you must declare a `Reflector` and `Unreflector` struct. Following is an example declaration which
   assumes the class to reflect has the name `CLASS_NAME`.
    ```cpp
    template <>
    struct Reflector<CLASS_NAME>
    {
        Reflected operator()(const CLASS_NAME& op) const;
    };
    
    template <>
    struct Unreflector<CLASS_NAME>
    {
        CLASS_NAME operator()(const Reflected& rfl) const;
    };
    ```
2. In the class source file, you need to implement the above-mentioned struct functions. In the 
   `Reflector` struct function, you need to call the `reflect` function on a reflectable data type or struct only consisting
   of reflectable data types. By default, the types `bool, int, double, std::string, std::map, std::vector, std::nullopt_t` 
   are reflectable, and all types where a custom `Reflector` and `Unreflector` are defined. If a struct of reflectable 
   objects is used, the struct should be defined within the classes header file within the namespace `NES::detail` and
   follow the naming pattern `ReflectedCLASS_NAME`. See the existing logical operators, logical functions, or 
   traits for example implementations.
   To be able to automatically reflect and unreflect objects, a header which declares the `Reflector` and the `Unreflector` must be 
   included. In most cases, the `Reflector` and `Unreflector` of a class are defined in the same header as the class itself and thus
   no additional header is required to be included. There are some exceptions, most notably 
   * LogicalFunctions -> use `#include <Serialization/LogicalFunctionReflection.hpp>`
   * WindowTypes -> use `#include <Serialization/WindowTypeReflection.hpp>`
   
   If the relevant header is not included, you'll get a compiler error similar to `error: static assertion failed due to requirement 'always_false_v<NES::TypedLogicalFunction<NES::detail::ErasedLogicalFunction>>': Unsupported type` 
   
   Following is an example implementation of a `Reflector` and `Unreflector` which assumes the class to reflect has the name `CLASS_NAME`.
   ```cpp
   Reflected Reflector<CLASS_NAME>::operator()(const CLASS_NAME& op) const
   {
       auto to_reflect = \* define the reflectable config data *\;
       return reflect(to_reflect);
   }

   SelectionLogicalOperator Unreflector<CLASS_NAME>::operator()(const Reflected& rfl) const
   {
       auto [arg1, arg2, ...] = unreflect</*type of above-defined to_reflect*/>(rfl);
       return CLASS_NAME(arg1, arg2, ...);
   }
   ```

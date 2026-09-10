# Serialization

This page covers two kinds of serialization: the conversion of individual field values between their textual and binary representation, and the serialization of query plans.

## Value Deserializers
Value Deserializers convert textual values that arrive with the incoming records into binary values.
Thus, they are part of the input-formatting routine and handle the deserialization of the actual values, which were located by the
input formatter.
The data type of a field determines its default deserializer type, which the user may override per field. For each deserializer type,
a separate deserializer for nullable fields is implemented.

The system comes with the `(Nullable)Default<Typename>ValueDeserializer` implementations for every data type, which use `std::from_chars`.
Value Deserializers can be configured with the flags `quoted` and `hasTrailingSpaces`, which need to be set by the `RawBufferIndex` during the construction of the Value Deserializer.

Further Value Deserializer implementations, for example using third party parser libraries, may be added as plugin.
Note, that each new deserializer implementations requires a separate "Nullable" variant.

The used Value Deserializer implementation of a specific field can be adjusted with the `VALUE_DESERIALIZERS` parameter of the input formatter by providing a comma-separated list of <Field-Name>:<Deserializer-Key> entries.
Fields that the user did not configure keep the default deserializer of their data type.
Configuring the deserializer per field (instead of per data type) allows each field to be parsed by the deserializer that fits its external representation, for example parsing one floating-point field with single and another one with double precision:
```sql
CREATE PHYSICAL SOURCE FOR source_name TYPE TCP SET(
'CSV' as INPUT_FORMATTER."TYPE",
'coarse:DefaultF32,precise:DefaultF64' as "INPUT_FORMATTER".VALUE_DESERIALIZERS,
...
);
```

## Value Serializers
Value Serializers convert binary values into their textual representations within textual formats like CSV or JSON.
Thus, they are part of the output-formatting routine and handle the serialization of the actual values while the output formatter handles the remaining symbols like delimiting characters.
Unlike the Value Deserializers, Value Serializers do not need to differentiate between nullable and non-nullable fields, as null-values will be detected and written by the output formatter.

The system comes with the `Default<Typename>ValueSerializer` implementations, as well as JSONCHARValueSerializer and JSONVARSIZEDValueSerializer for json-escaped strings and chars.
The default implementations use `std::to_string` for int - string conversions and `fmt::format`, which utilizes the Dragonbox algorithm, for float - string conversions.
Value Serializers can currently be configured with the `quoted` flag, which needs to be set by the output formatter that calls the Value Serializer.

Further Value Serializer implementations, for example using third party parser libraries, may be added as plugin.

The used Value Serializer implementation of a specific field can be adjusted with the `VALUE_SERIALIZERS` parameter of the output formatter by providing a comma-separated list of <Field-Name>:<Serializer-Key> entries.
Fields that the user did not configure keep the default serializer of their data type.
Configuring the serializer per field (instead of per data type) allows two fields of the same data type to leave the system in different external representations, for example one string JSON-escaped and another one written as is:
```sql
CREATE SINK sink_name TYPE FILE SET(
       'CSV' as "SINK".OUTPUT_FORMAT,
       'escaped:JSONVARSIZED,raw:DefaultVARSIZED' as "OUTPUT_FORMATTER".VALUE_SERIALIZERS,
       ...
);
```

## Serialization of Logical Operators, Logical Functions, Traits, ...

We utilize reflection to enable a flexible serialization of logical operators, and logical functions.
While we use an external library (reflect-cpp) for reflection, we wrap the reflection functionality in 
a custom interface (see `Utils/Reflection.hpp` in nes-commons).

### Adding reflection to a Logical Operator, Logical Function, Trait, or other 

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
        CLASS_NAME operator()(const Reflected& rfl, const ReflectionContext& context) const;
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

   SelectionLogicalOperator Unreflector<CLASS_NAME>::operator()(const Reflected& rfl, const ReflectionContext& context) const
   {
       auto [arg1, arg2, ...] = context.unreflect</*type of above-defined to_reflect*/>(rfl);
       return CLASS_NAME(arg1, arg2, ...);
   }
   ```
3. You can pass objects from the start of the unreflection call-chain downwards through the ReflectionContext.
   To receive it, declare a constructor for the `Unreflector` which takes the object as a parameter.
   ```cpp
   struct MyConfigType{
      bool unnest;
   };
   
   template <>
   struct Unreflector<MyType>{
   MyConfigType config;
   explicit Unreflector(MyConfigType config): config(std::move(config)) { }
   
   MyType operator()(const Reflected& rfl, const ReflectionContext& context) const
   {
       auto [self, children] = context.unreflect<MyTypeHelper>(rfl);
       if (config.unnest){
           return MyType{self, children};
       }
       else{
           return MyType{self};
       }
   }
   
   int main(){
      auto context = ReflectionContext{MyConfigType{true}};
      auto rfl = reflect(MyType{...});
      auto my_type = context.unreflect<MyType>(rfl);
   }
   ```
   See the ReflectionTest.cpp for more examples.

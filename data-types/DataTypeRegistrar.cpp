//
// Created by ls on 4/18/25.
//

#include <DataType.hpp>
#include <DataTypeSerializationFactory.hpp>

namespace NES
{
void registerGenericType(DataTypeRegistry& registry, DataTypeSerializationFactory& serialization_factory);
void registerIntegerType(DataTypeRegistry& registry, DataTypeSerializationFactory& serialization_factory);
void registerPointType(DataTypeRegistry& registry, DataTypeSerializationFactory& serialization_factory);

void registerAllTypes(DataTypeRegistry& registry, DataTypeSerializationFactory& serialization_factory)
{
    registerGenericType(registry, serialization_factory);
    registerIntegerType(registry, serialization_factory);
    registerPointType(registry, serialization_factory);
}

DataTypeRegistry DataTypeRegistry::init()
{
    DataTypeRegistry registry;
    auto& serializationFactory = const_cast<DataTypeSerializationFactory&>(DataTypeSerializationFactory::instance());
    registerAllTypes(registry, serializationFactory);
    return registry;
}

void registerGenericFunctions(FunctionRegistry& registry, const DataTypeRegistry& types);
void registerIntegerFunctions(FunctionRegistry& registry, const DataTypeRegistry& types);
void registerPointFunctions(FunctionRegistry& registry, const DataTypeRegistry& types);

void registerAllFunctions(FunctionRegistry& registry, const DataTypeRegistry& types)
{
    registerGenericFunctions(registry, types);
    registerIntegerFunctions(registry, types);
    registerPointFunctions(registry, types);
}
}
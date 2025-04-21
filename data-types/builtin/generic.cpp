#include <Util/Strings.hpp>
#include <rfl/flexbuf.hpp>
#include "WellKnownDataTypes.hpp"

#include <DataType.hpp>
#include <DataTypeSerializationFactory.hpp>
#include <ErrorHandling.hpp>

#include "generic.hpp"

namespace NES
{
DataType constant(std::string value)
{
    auto constantDataType = std::make_shared<Constant>(ConstantProperties{value});
    return DataType(COW<DataTypeImpl>(std::move(constantDataType)), false, detail::Scalar{});
}

void registerGenericType(DataTypeRegistry& registry, DataTypeSerializationFactory& serialization_factory)
{
    registry.registerDataType(
        Constant::Name,
        [impl = DataType{COW<DataTypeImpl>(std::make_shared<Constant>(ConstantProperties{})), false, detail::Scalar{}}] { return impl; });

    struct ConstantSerializer : Serializer
    {
        std::vector<char> serialize(const DataTypeImpl& impl) override
        {
            INVARIANT(dynamic_cast<const Constant*>(&impl) != nullptr, "Attempted to deserialize non bool type");
            return rfl::flexbuf::write(dynamic_cast<const Constant&>(impl).properties);
        }
        std::shared_ptr<DataTypeImpl> deserialize(const std::vector<char>& impl) override
        {
            if (auto properties = rfl::flexbuf::read<ConstantProperties>(impl))
            {
                return std::make_shared<Constant>(*properties);
            }
            throw std::runtime_error("Failed to deserialize bool type");
        }
    };
    serialization_factory.registerSerializer(Constant::Name, std::make_unique<ConstantSerializer>());

    registry.registerDataType(
        Bool::Name,
        [impl = DataType{COW<DataTypeImpl>(std::make_shared<Bool>(BoolProperties{})), false, detail::Scalar{}}] { return impl; });

    struct BoolSerializer : Serializer
    {
        std::vector<char> serialize(const DataTypeImpl& impl) override
        {
            INVARIANT(dynamic_cast<const Bool*>(&impl) != nullptr, "Attempted to deserialize non bool type");
            return rfl::flexbuf::write(dynamic_cast<const Bool&>(impl).properties);
        }
        std::shared_ptr<DataTypeImpl> deserialize(const std::vector<char>& impl) override
        {
            if (auto properties = rfl::flexbuf::read<BoolProperties>(impl))
            {
                return std::make_shared<Bool>(*properties);
            }
            throw std::runtime_error("Failed to deserialize bool type");
        }
    };
    serialization_factory.registerSerializer(Bool::Name, std::make_unique<BoolSerializer>());
}
void registerGenericFunctions(FunctionRegistry& registry, const DataTypeRegistry&)
{
    using namespace Matcher;
    registry.registerFunction(
        Bool::Name,
        {Scalar(Constant::Name)},
        [](auto args)
        {
            auto value = Util::from_chars<bool>(args[0].template typeProperties<Constant>().properties.constantValue);
            if (!value.has_value())
                throw std::runtime_error("invalid integer");
            auto boolType = std::make_shared<Bool>(BoolProperties{value});
            DataType returnType{COW<DataTypeImpl>(std::move(boolType)), false, detail::Scalar{}};
            return Function(Bool::Name, returnType);
        });

    registry.registerFunction(
        "NULLABLE", {AnyNonNullable()}, [](std::span<const DataType> types) { return Function("NULLABLE", types[0].asNullable()); });
    registry.registerFunction(
        "IS_NULL",
        {AnyNullable()},
        [](std::span<const DataType>) { return Function("IS_NULL", DataTypeRegistry::instance().lookup("Bool").value()); });
    registry.registerFunction(
        "LIST", {}, AnyEqual(), [](std::span<const DataType> args) { return Function("LIST", args[0].asList(args.size())); });

    registry.registerFunction(
        "SIZE",
        {AnyList()},
        [](std::span<const DataType>)
        {
            auto integer = DataTypeRegistry::instance().lookup("Integer").value();
            return Function("SIZE", integer);
        });

    registry.registerFunction(
        "GET", {AnyList(), Scalar("Integer")}, [](std::span<const DataType> dt) { return Function("GET", dt[0].asNullable()); });
}
}
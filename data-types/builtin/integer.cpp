#include <memory>
#include <Util/Strings.hpp>
#include <fmt/base.h>
#include <rfl/flexbuf.hpp>
#include <DataType.hpp>
#include <DataTypeSerializationFactory.hpp>
#include <ErrorHandling.hpp>
#include <rfl.hpp>
#include "WellKnownDataTypes.hpp"

#include "generic.hpp"

namespace NES
{

struct IntegerProperties
{
    bool isSigned = true;
    int64_t upperBound = std::numeric_limits<uint64_t>::max();
    int64_t lowerBound = std::numeric_limits<int64_t>::min();
    bool operator==(const IntegerProperties&) const = default;
};

struct Integer final : DataTypeImplHelper<Integer>
{
    explicit Integer(const IntegerProperties& properties) : properties(properties) { }
    constexpr static NameT Name = WellKnown::Integer;
    bool operator==(const Integer&) const = default;
    IntegerProperties properties;
};


void registerIntegerType(NES::DataTypeRegistry& registry, DataTypeSerializationFactory& serialization_factory)
{
    registry.registerDataType(
        Integer::Name,
        [integerType = DataType{COW<DataTypeImpl>(std::make_shared<Integer>(IntegerProperties{})), false, detail::Scalar{}}]
        { return integerType; });

    struct IntegerSerializer : Serializer
    {
        std::vector<char> serialize(const DataTypeImpl& impl) override
        {
            INVARIANT(dynamic_cast<const Integer*>(&impl) != nullptr, "Attempted to deserialize non integer type");
            return rfl::flexbuf::write(dynamic_cast<const Integer&>(impl).properties);
        }
        std::shared_ptr<DataTypeImpl> deserialize(const std::vector<char>& impl) override
        {
            if (auto properties = rfl::flexbuf::read<IntegerProperties>(impl))
            {
                return std::make_shared<Integer>(*properties);
            }
            throw std::runtime_error("Failed to deserialize integer type");
        }
    };
    serialization_factory.registerSerializer(Integer::Name, std::make_unique<IntegerSerializer>());
}

void registerIntegerFunctions(FunctionRegistry& registry, const NES::DataTypeRegistry&)
{
    using namespace Matcher;

    registry.registerFunction(Integer::Name, {Scalar(Integer::Name)}, [](auto args) { return Function(Integer::Name, args[0]); });
    registry.registerFunction(
        Integer::Name,
        {Scalar(WellKnown::Constant)},
        [](auto args)
        {
            auto value = Util::from_chars<int64_t>(args[0].template typeProperties<Constant>().properties.constantValue);
            if (!value.has_value())
                throw std::runtime_error("invalid integer");
            auto integerType = std::make_shared<Integer>(IntegerProperties{});
            if (*value < 0)
            {
                integerType->properties.isSigned = true;
            }
            integerType->properties.upperBound = *value;
            integerType->properties.upperBound = *value;

            DataType returnType{COW<DataTypeImpl>(std::move(integerType)), false, detail::Scalar{}};
            return Function(Integer::Name, returnType);
        });

    registry.registerFunction(
        "ADD",
        {Scalar(Integer::Name), Scalar(Integer::Name)},
        [](std::span<const DataType> args)
        {
            DataType returnType{
                COW<DataTypeImpl>(std::make_shared<Integer>(IntegerProperties{})), Util::any_nullable(args), detail::Scalar{}};
            return Function("ADD", std::move(returnType));
        });

    registry.registerFunction(
        "NotEqual",
        {Scalar(Integer::Name), Scalar(Integer::Name)},
        [](std::span<const DataType> args)
        {
            auto returnType = DataTypeRegistry::instance().lookup(WellKnown::Bool).value();
            if (Util::any_nullable(args))
            {
                returnType = returnType.asNullable();
            }
            return Function("NotEqual", returnType);
        });

    registry.registerFunction("NEGATE", {Scalar(Integer::Name)}, [](std::span<const DataType> types) { return Function("ADD", types[0]); });
}
}
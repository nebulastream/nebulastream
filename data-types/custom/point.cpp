//
// Created by ls on 4/18/25.
//

#include <rfl/flexbuf.hpp>
#include <DataType.hpp>
#include <DataTypeSerializationFactory.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct PointProperties
{
    bool operator==(const PointProperties&) const = default;
};

struct Point : DataTypeImplHelper<Point>
{
    static constexpr NameT Name = "Point";
    explicit Point(const PointProperties& properties) : properties(properties) { }
    PointProperties properties;
    bool operator==(const Point&) const = default;
};

void registerPointType(NES::DataTypeRegistry& registry, DataTypeSerializationFactory& serialization_factory)
{
    registry.registerDataType(
        Point::Name,
        [pointType = DataType{COW<DataTypeImpl>(std::make_shared<Point>(PointProperties{})), false, detail::Scalar{}}]
        { return pointType; });

    struct PointSerializer : Serializer
    {
        std::vector<char> serialize(const DataTypeImpl& impl) override
        {
            INVARIANT(dynamic_cast<const Point*>(&impl) != nullptr, "Attempted to deserialize non point type");
            return rfl::flexbuf::write(dynamic_cast<const Point&>(impl).properties);
        }
        std::shared_ptr<DataTypeImpl> deserialize(const std::vector<char>& impl) override
        {
            if (auto properties = rfl::flexbuf::read<PointProperties>(impl))
            {
                return std::make_shared<Point>(std::move(*properties));
            }
            throw std::runtime_error("Failed to deserialize point type");
        }
    };
    serialization_factory.registerSerializer(Point::Name, std::make_unique<PointSerializer>());
}

void registerPointFunctions(FunctionRegistry& registry, const NES::DataTypeRegistry&)
{
    using namespace Matcher;

    registry.registerFunction(
        "x",
        {Scalar(Point::Name)},
        [](std::span<const DataType>) { return Function("x", DataTypeRegistry::instance().lookup("Integer").value()); });
    registry.registerFunction(
        "y",
        {Scalar(Point::Name)},
        [](std::span<const DataType>) { return Function("y", DataTypeRegistry::instance().lookup("Integer").value()); });
    registry.registerFunction(
        "z",
        {Scalar(Point::Name)},
        [](std::span<const DataType>) { return Function("z", DataTypeRegistry::instance().lookup("Integer").value()); });

    registry.registerFunction(
        "MUL", {Scalar(Point::Name), Scalar("Integer")}, [](std::span<const DataType> args) { return Function("ADD", args[0]); });

    registry.registerFunction(
        "ADD", {Scalar(Point::Name), Scalar(Point::Name)}, [](std::span<const DataType> args) { return Function("ADD", args[0]); });

    registry.registerFunction(
        Point::Name,
        {Scalar("Integer"), Scalar("Integer"), Scalar("Integer")},
        [](std::span<const DataType>) {
            return Function(Point::Name, DataType{COW<DataTypeImpl>(std::make_shared<Point>(PointProperties{})), false, detail::Scalar{}});
        });
}
}
//
// Created by ls on 4/21/25.
//

#include <DataTypeSerializationFactory.hpp>

void NES::DataTypeSerializationFactory::serialize(const DataType& dt, MessageType& msg) const
{
    if (dt.isScalar())
    {
        msg.mutable_scalar();
    }
    else if (auto size = dt.isFixedSize())
    {
        msg.mutable_list()->set_size(*size);
    }
    else
    {
        msg.mutable_varsized();
    }

    msg.set_nullable(dt.isNullable());
    msg.set_name(dt.name());

    const auto& serializer = serializers.at(dt.name());
    auto serializedImplementation = serializer->serialize(*dt.impl);
    msg.mutable_impl()->assign(serializedImplementation.begin(), serializedImplementation.end());
}

NES::DataType NES::DataTypeSerializationFactory::deserialize(const MessageType& msg) const
{
    auto& serializer = serializers.at(msg.name());
    auto impl = serializer->deserialize(std::ranges::to<std::vector<char>>(msg.impl()));
    auto shape = [&]() -> detail::Shape
    {
        if (msg.has_list())
        {
            return detail::FixedSize(msg.list().size());
        }
        if (msg.has_scalar())
        {
            return detail::Scalar{};
        }
        if (msg.has_varsized())
        {
            return detail::VarSized{};
        }
        throw std::invalid_argument("Unknown DataType Shape");
    }();
    auto nullable = msg.nullable();
    return DataType(COW(std::move(impl)), nullable, shape);
}
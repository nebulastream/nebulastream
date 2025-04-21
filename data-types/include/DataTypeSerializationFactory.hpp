//
// Created by ls on 4/21/25.
//

#pragma once
#include <DataType.hpp>
#include <SerializableDataType.grpc.pb.h>

namespace NES
{

class Serializer
{
public:
    virtual ~Serializer() = default;
    virtual std::vector<char> serialize(const DataTypeImpl& impl) = 0;
    virtual std::shared_ptr<DataTypeImpl> deserialize(const std::vector<char>& impl) = 0;
};

class DataTypeSerializationFactory
{
    using MessageType = SerializableDataType;
    std::unordered_map<DataTypeImpl::NameT, std::unique_ptr<Serializer>> serializers;

public:
    static const DataTypeSerializationFactory& instance()
    {
        static DataTypeSerializationFactory instance;
        return instance;
    }

    void serialize(const DataType& dt, MessageType& msg) const;
    DataType deserialize(const MessageType& msg) const;

    void registerSerializer(DataTypeImpl::NameT name, std::unique_ptr<Serializer> serializer)
    {
        if (!serializers.try_emplace(name, std::move(serializer)).second)
        {
            throw std::runtime_error("Attempted to register a datatype serializer multiple times");
        }
    }
};

}
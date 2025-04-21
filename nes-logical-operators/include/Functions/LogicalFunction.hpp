/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <memory>
#include <ostream>
#include <span>
#include <stack>
#include <string>
#include <API/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>
#include <SerializableFunction.pb.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

struct LogicalFunctionConcept
{
public:
    virtual ~LogicalFunctionConcept() = default;
    [[nodiscard]] virtual std::string toString() const = 0;

    [[nodiscard]] virtual const DataType& getStamp() const = 0;
    virtual void setStamp(std::shared_ptr<DataType> stamp) = 0;

    [[nodiscard]] virtual std::vector<struct LogicalFunction> getChildren() const = 0;

    [[nodiscard]] virtual std::string getType() const = 0;
    [[nodiscard]] virtual SerializableFunction serialize() const = 0;
};

struct LogicalFunction
{
public:
    template <typename T>
    LogicalFunction(const T& op) : self(std::make_unique<Model<T>>(op))
    {
    }

    LogicalFunction(const LogicalFunction& other) : self(other.self->clone()) { }

    template <typename T>
    const T& get() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        INVARIANT(false, "Bad cast: requested type {} , but stored type is {}", typeid(T).name(), self->getType());
    }

    template <typename T>
    const T* tryGet() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return &(p->data);
        }
        return nullptr;
    }

    LogicalFunction(LogicalFunction&&) noexcept = default;

    LogicalFunction& operator=(const LogicalFunction& other)
    {
        if (this != &other)
        {
            self = other.self->clone();
        }
        return *this;
    }

    bool operator==(const LogicalFunction& other) const { return self->equals(*other.self); }

    [[nodiscard]] std::string toString() const { return self->toString(); }

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const { return self->getChildren(); }

    const DataType& getStamp() const { return self->getStamp(); }

    void setStamp(std::shared_ptr<DataType> stamp) { self->setStamp(stamp); }

    SerializableFunction serialize() const { return self->serialize(); }

    std::string getType() const { return self->getType(); }

private:
    struct Concept : LogicalFunctionConcept
    {
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    template <typename T>
    struct Model : Concept
    {
        T data;
        explicit Model(T d) : data(std::move(d)) { }

        [[nodiscard]] std::unique_ptr<Concept> clone() const override { return std::unique_ptr<Concept>(new Model<T>(data)); }

        [[nodiscard]] std::string toString() const override { return data.toString(); }

        [[nodiscard]] std::vector<LogicalFunction> getChildren() const override { return data.getChildren(); }

        SerializableFunction serialize() const override { return data.serialize(); }

        std::string getType() const override { return data.getType(); }

        const DataType& getStamp() const override { return data.getStamp(); }

        void setStamp(std::shared_ptr<DataType> stamp) override { data.setStamp(stamp); }

        [[nodiscard]] bool equals(const Concept& other) const override
        {
            if (auto p = dynamic_cast<const Model<T>*>(&other))
            {
                return data == p->data;
            }
            return false;
        }
    };

    std::unique_ptr<Concept> self;
};

inline std::ostream& operator<<(std::ostream& os, const LogicalFunction& lf)
{
    return os << lf.toString();
}

}
FMT_OSTREAM(NES::LogicalFunction);

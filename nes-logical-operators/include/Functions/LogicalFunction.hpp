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
#include <memory>
#include <stack>
#include <string>
#include <span>
#include <ostream>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <SerializableFunction.pb.h>
#include <ErrorHandling.hpp>

namespace NES
{

struct LogicalFunctionConcept
{
public:
    virtual ~LogicalFunctionConcept() = default;
    [[nodiscard]] virtual std::string toString() const = 0;

    [[nodiscard]] virtual const DataType& getStamp() const = 0;
    virtual void setStamp(std::shared_ptr<DataType> stamp) = 0;

    virtual bool inferStamp(Schema schema) = 0;

    [[nodiscard]] virtual std::vector<struct LogicalFunction> getChildren() const = 0;
    virtual void setChildren(std::vector<struct LogicalFunction>) = 0;

    [[nodiscard]] virtual std::string getType() const = 0;
    [[nodiscard]] virtual SerializableFunction serialize() const = 0;
};

class NullLogicalFunction : public LogicalFunctionConcept
{
public:
    NullLogicalFunction()
        : stamp(nullptr)
    {}

    [[nodiscard]] std::string toString() const override
    {
        PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    }

    [[nodiscard]] const DataType& getStamp() const override
    {
        PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    }

    void setStamp(std::shared_ptr<DataType>) override
    {
        PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    }

    bool inferStamp(Schema) override
    {
        PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    }

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override
    {
        PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    }

    void setChildren(std::vector<LogicalFunction>) override
    {
        PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    }

    [[nodiscard]] std::string getType() const override
    {
        PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    }

    [[nodiscard]] SerializableFunction serialize() const override
    {
        PRECONDITION(false, "Calls in NullLogicalFunction are undefined");
    }

    bool operator==(const NullLogicalFunction&) const
    {
        return false;
    }
private:
    std::shared_ptr<DataType> stamp;
};



struct LogicalFunction {
public:
    template<typename T>
    LogicalFunction(const T& op) : self(std::make_unique<Model<T>>(op)) {}

    LogicalFunction() : self(std::make_unique<Model<NullLogicalFunction>>(NullLogicalFunction{})) {}

    LogicalFunction(const LogicalFunction& other)
        : self(other.self->clone()) {}

    template<typename T>
    const T& get() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        INVARIANT(false, "Bad cast: requested type {} , but stored type is {}", typeid(T).name(), self->getType());
    }

    template<typename T>
    const T* tryGet() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get())) {
            return &(p->data);
        }
        return nullptr;
    }

    LogicalFunction(LogicalFunction&&) noexcept = default;

    LogicalFunction& operator=(const LogicalFunction& other) {
        if (this != &other)
        {
            self = other.self->clone();
        }
        return *this;
    }

    bool operator==(const LogicalFunction &other) const {
        return self->equals(*other.self);
    }

    [[nodiscard]] std::string toString() const
    {
        return self->toString();
    }

    bool inferStamp(Schema schema)
    {
        return self->inferStamp(schema);
    }

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const
    {
        return self->getChildren();
    }

    void setChildren(std::vector<LogicalFunction> children)
    {
        self->setChildren(children);
    }

    const DataType& getStamp() const
    {
        return self->getStamp();
    }

    void setStamp(std::shared_ptr<DataType> stamp)
    {
        self->setStamp(stamp);
    }

    SerializableFunction serialize() const
    {
        return self->serialize();
    }

    std::string getType() const
    {
        return self->getType();
    }

private:
    struct Concept : LogicalFunctionConcept {
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    template<typename T>
    struct Model : Concept {
        T data;
        explicit Model(T d) : data(std::move(d)) {}

        [[nodiscard]] std::unique_ptr<Concept> clone() const override {
            return std::unique_ptr<Concept>(new Model<T>(data));
        }

        [[nodiscard]] std::string toString() const override
        {
            return data.toString();
        }

        [[nodiscard]] std::vector<LogicalFunction> getChildren() const override
        {
            return data.getChildren();
        }

        void setChildren(std::vector<LogicalFunction> children) override
        {
            return data.setChildren(children);
        }

        SerializableFunction serialize() const override
        {
            return data.serialize();
        }

        std::string getType() const override
        {
            return data.getType();
        }

        const DataType& getStamp() const override
        {
            return data.getStamp();
        }

        bool inferStamp(Schema schema) override
        {
            return data.inferStamp(schema);
        }

        void setStamp(std::shared_ptr<DataType> stamp) override
        {
            data.setStamp(stamp);
        }

        [[nodiscard]] bool equals(const Concept& other) const override {
            if (auto p = dynamic_cast<const Model<T>*>(&other)) {
                return data.operator==(p->data);
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

namespace std {
template<>
struct hash<NES::LogicalFunction> {
    std::size_t operator()(const NES::LogicalFunction& lf) const noexcept {
        return std::hash<std::string>{}(lf.toString());
    }
};
}

FMT_OSTREAM(NES::LogicalFunction);
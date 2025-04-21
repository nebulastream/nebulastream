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
#include <string>
#include <ostream>
#include <vector>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Util/Logger/Formatter.hpp>
#include <SerializableFunction.pb.h>
#include <ErrorHandling.hpp>

namespace NES
{

struct LogicalFunction;


/// @brief Abstract base class for representing a logical function.
///
/// All logical functions are expected to be immutable. Operations that modify
/// the function (e.g., updating the stamp or children) should produce a new instance.
struct LogicalFunctionConcept
{
    virtual ~LogicalFunctionConcept() = default;
    [[nodiscard]] virtual std::string toString() const = 0;

    [[nodiscard]] virtual const DataType& getStamp() const = 0;
    [[nodiscard]] virtual LogicalFunction withStamp(std::shared_ptr<DataType> stamp) const = 0;

    [[nodiscard]] virtual LogicalFunction withInferredStamp(Schema schema) const = 0;

    [[nodiscard]] virtual std::vector<LogicalFunction> getChildren() const = 0;
    [[nodiscard]] virtual LogicalFunction withChildren(std::vector<LogicalFunction>) const = 0;

    [[nodiscard]] virtual std::string getType() const = 0;
    [[nodiscard]] virtual SerializableFunction serialize() const = 0;

    [[nodiscard]] virtual bool operator==(const LogicalFunctionConcept& rhs) const = 0;
};

/// @brief Null implementation of the LogicalFunctionConcept.
///
/// This class serves as a placeholder or default implementation of a logical function.
/// All operations in this class are undefined and will trigger a runtime precondition failure
/// if invoked. It is intended solely as a safe default for data structures such as hashmaps
/// or error state and should not be used for actual logical function implementations.
class NullLogicalFunction : public LogicalFunctionConcept
{
public:
    NullLogicalFunction();
    [[nodiscard]] std::string toString() const override;

    [[nodiscard]] const DataType& getStamp() const override;
    [[nodiscard]] LogicalFunction withStamp(std::shared_ptr<DataType>) const override;
    [[nodiscard]] LogicalFunction withInferredStamp(Schema Schema) const override;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(std::vector<LogicalFunction>) const override;

    [[nodiscard]] std::string getType() const override;
    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] bool operator==(const NullLogicalFunction&) const;
    [[nodiscard]] bool operator==(const LogicalFunctionConcept&) const override;
};

/// @brief Value-semantic, immutable wrapper for logical functions.
/// C.f. @link: https://sean-parent.stlab.cc/presentations/2017-01-18-runtime-polymorphism/2017-01-18-runtime-polymorphism.pdf
///
/// Provides a type-erased container for any object implementing the LogicalFunctionConcept interface.
/// The class is designed with immutability in mind: "with" methods return new instances rather than
/// modifying the existing object. Internally, the implementation is stored via a unique pointer to
/// an abstract Concept, and concrete implementations are provided via the Model<T> template.
struct LogicalFunction final
{
    template<typename T>
    LogicalFunction(const T& op) : self(std::make_unique<Model<T>>(op)) {}
    LogicalFunction(const LogicalFunction& other);
    LogicalFunction();

    template<typename T>
    [[nodiscard]] const T& get() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        INVARIANT(false, "Bad cast: requested type {} , but stored type is {}", typeid(T).name(), self->getType());
    }

    template<typename T>
    [[nodiscard]] const T* tryGet() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get())) {
            return &(p->data);
        }
        return nullptr;
    }

    [[nodiscard]] LogicalFunction(LogicalFunction&&) noexcept = default;
    LogicalFunction& operator=(const LogicalFunction& other);
    [[nodiscard]] bool operator==(const LogicalFunction &other) const;

    [[nodiscard]] std::string toString() const;

    [[nodiscard]] LogicalFunction withInferredStamp(Schema schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] LogicalFunction withChildren(std::vector<LogicalFunction> children) const;

    [[nodiscard]] const DataType& getStamp() const;
    [[nodiscard]] LogicalFunction withStamp(std::shared_ptr<DataType> stamp) const;

    [[nodiscard]] SerializableFunction serialize() const;
    [[nodiscard]] std::string getType() const;

private:
    /// @brief Internal abstract base class for type-erased implementations of LogicalFunction.
    struct Concept : LogicalFunctionConcept {
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    /// @brief Template-based concrete implementation of a logical function.
    ///
    /// Wraps a specific type T that implements LogicalFunctionConcept and provides
    /// concrete implementations for all required operations by delegating to T.
    /// @tparam T The concrete type implementing the logical function.
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

        [[nodiscard]] LogicalFunction withChildren(std::vector<LogicalFunction> children) const override
        {
            return data.withChildren(children);
        }

        [[nodiscard]] SerializableFunction serialize() const override
        {
            return data.serialize();
        }

        [[nodiscard]] std::string getType() const override
        {
            return data.getType();
        }

        [[nodiscard]] const DataType& getStamp() const override
        {
            return data.getStamp();
        }

        [[nodiscard]] LogicalFunction withInferredStamp(Schema schema) const override
        {
            return data.withInferredStamp(schema);
        }

        [[nodiscard]] LogicalFunction withStamp(std::shared_ptr<DataType> stamp) const override
        {
            return data.withStamp(stamp);
        }

        [[nodiscard]] bool operator==(const LogicalFunctionConcept& other) const override
        {
            if (auto p = dynamic_cast<const Model<T>*>(&other)) {
                return data.operator==(p->data);
            }
            return false;
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
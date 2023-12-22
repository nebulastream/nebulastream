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

#ifndef NES_AUTOMATICDATAGENERATOR_H
#define NES_AUTOMATICDATAGENERATOR_H

#include <DataGeneration/DataGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <fstream>
#include <random>

class FieldDataGenerator {
  public:
    virtual void generate(NES::Runtime::MemoryLayouts::DynamicField&) = 0;
    virtual ~FieldDataGenerator() = default;
};

template<typename T>
concept IsInteger = std::is_integral_v<T>;

template<IsInteger T>
class IncreasingSequence final : public FieldDataGenerator {
    T current = 0;

  public:
    void generate(NES::Runtime::MemoryLayouts::DynamicField& field) override { field.write<T>(current++); }
};

template<IsInteger T>
class ConstantSequence final : public FieldDataGenerator {
    T value;

  public:
    explicit ConstantSequence(T value) : value(value) {}
    void generate(NES::Runtime::MemoryLayouts::DynamicField& field) override { field.write<T>(value); }
};

template<typename T>
class RandomValues final : public FieldDataGenerator {
    std::random_device rd;
    std::mt19937 gen;
    T min;
    T max;

  public:
    RandomValues(T min, T max) : rd(), gen(rd()), min(min), max(max) {}

    void generate(NES::Runtime::MemoryLayouts::DynamicField& field) override {
        T value = (static_cast<T>(gen()) / static_cast<T>(RAND_MAX));
        T diff = max - min;
        T value_in_range = min + (value * diff);
        field.write<T>(value_in_range);
    }
};

class AutomaticDataGenerator : public NES::Benchmark::DataGeneration::DataGenerator {
    NES::SchemaPtr schema;
    std::optional<size_t> numberOfTupleToCreate;
    std::unordered_map<std::string, std::unique_ptr<FieldDataGenerator>> generators;

  public:
    AutomaticDataGenerator(NES::SchemaPtr schema,
                           std::unordered_map<std::string, std::unique_ptr<FieldDataGenerator>>&& generators)
        : schema(std::move(schema)), generators(std::move(generators)) {}
    std::vector<NES::Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;
    NES::SchemaPtr getSchema() override;
    std::string getName() override;
    std::string toString() override;
    [[nodiscard]] std::optional<size_t> getNumberOfTupleToCreate() const { return numberOfTupleToCreate; }
    void setNumberOfTupleToCreate(const std::optional<size_t>& numberOfTupleToCreate) {
        this->numberOfTupleToCreate = numberOfTupleToCreate;
    }
    static std::unique_ptr<AutomaticDataGenerator> create(NES::SchemaPtr schema);
    NES::Configurations::SchemaTypePtr getSchemaType() override;
};

#endif//NES_AUTOMATICDATAGENERATOR_H

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

#ifndef NES_BYTEDATAGENERATOR_HPP
#define NES_BYTEDATAGENERATOR_HPP
#include "Common/DataTypes/FixedChar.hpp"
#include "Common/DataTypes/Text.hpp"
#include "Common/ExecutableType/Array.hpp"
#include "Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp"
#include "Util/ZipfianGenerator.hpp"
#include <API/Schema.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <DataGeneration/DataGenerator.hpp>
#include <DataGeneration/data/TextDataSet.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <fstream>
#include <random>

namespace NES::Benchmark::DataGeneration {
// ====================================================================================================
// Distributions
// ====================================================================================================
enum class DistributionName { REPEATING_VALUES, UNIFORM, BINOMIAL, ZIPF };
[[maybe_unused]] static std::string getDistributionName(enum DistributionName dn) {
    switch (dn) {
        case DistributionName::REPEATING_VALUES: return "Repeating Values";
        case DistributionName::UNIFORM: return "Uniform";
        case DistributionName::BINOMIAL: return "Binomial";
        case DistributionName::ZIPF: return "Zipf";
    }
}

std::random_device rd;

class ByteDataDistribution {
  public:
    ~ByteDataDistribution();
    virtual DistributionName getName();
    size_t seed = rd();
    bool sort = false;

  protected:
    DistributionName distributionName;
};

class RepeatingValues : public ByteDataDistribution {
  public:
    /**
     * Generate data with repeating values. Each column will has its own distribution.
     * @param numRepeats how often a value shall be repeated consecutively
     * @param sigma maximum deviation of `numRepeats`: [numRepeats - sigma, numRepeats + sigma]
     * @param changeProbability probability that numRepeats will change within [numRepeats - sigma, numRepeats + sigma]
     */
    explicit RepeatingValues(int numRepeats, uint8_t sigma = 0, double changeProbability = 0);
    virtual ~RepeatingValues();

    int numRepeats;
    uint8_t sigma;
    double changeProbability;
};

class Uniform : public ByteDataDistribution {
  public:
    /**
     * Generate uniformly distributed data
     */
    explicit Uniform();
};

class Binomial : public ByteDataDistribution {
  public:
    explicit Binomial(double probability);

    double probability;
};

class Zipf : public ByteDataDistribution {
  public:
    explicit Zipf(double alpha);

    double alpha;
};

// ====================================================================================================
// Parameters
// TODO move to dedicated file
// ====================================================================================================
class Parameter {
  public:
    explicit Parameter(BasicType nesType, ByteDataDistribution* dataDistribution) {
        this->nesType = nesType;
        this->dataDistribution = dataDistribution;
    }
    /*
    template<typename T>
    bool isType() {
        return (dynamic_cast<T*>(this) != nullptr);
    }
     */

    BasicType nesType;
    ByteDataDistribution* dataDistribution;
};
class Int8Parameter : public Parameter {
  public:
    Int8Parameter(int8_t minValue, int8_t maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::INT8, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    int8_t minValue{};
    int8_t maxValue{};
};
class Uint8Parameter : public Parameter {
  public:
    Uint8Parameter(uint8_t minValue, uint8_t maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::UINT8, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    uint8_t minValue{};
    uint8_t maxValue{};
};
class Int16Parameter : public Parameter {
  public:
    Int16Parameter(int16_t minValue, int16_t maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::INT16, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    int16_t minValue{};
    int16_t maxValue{};
};
class Uint16Parameter : public Parameter {
  public:
    Uint16Parameter(uint16_t minValue, uint16_t maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::INT16, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    uint16_t minValue{};
    uint16_t maxValue{};
};
class Int32Parameter : public Parameter {
  public:
    Int32Parameter(int32_t minValue, int32_t maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::INT32, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    int32_t minValue{};
    int32_t maxValue{};
};
class Uint32Parameter : public Parameter {
  public:
    Uint32Parameter(uint32_t minValue, uint32_t maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::UINT32, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    uint32_t minValue{};
    uint32_t maxValue{};
};
class Int64Parameter : public Parameter {
  public:
    Int64Parameter(int32_t minValue, int64_t maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::INT64, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    int64_t minValue{};
    int64_t maxValue{};
};
class Uint64Parameter : public Parameter {
  public:
    Uint64Parameter(uint32_t minValue, uint64_t maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::UINT64, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    uint64_t minValue{};
    uint64_t maxValue{};
};
class FloatParameter : public Parameter {
  public:
    FloatParameter(float minValue, float maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::FLOAT32, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    float minValue{};
    float maxValue{};
};
class DoubleParameter : public Parameter {
  public:
    DoubleParameter(double minValue, double maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::FLOAT64, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    double minValue{};
    double maxValue{};
};
class BooleanParameter : public Parameter {
  public:
    BooleanParameter(bool minValue, bool maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::BOOLEAN, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    bool minValue{};
    bool maxValue{};
};
class CharParameter : public Parameter {
  public:
    CharParameter(char minValue, char maxValue, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::CHAR, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    char minValue{};
    char maxValue{};
};
class TextParameter : public Parameter {
  public:
    TextParameter(size_t minValue, size_t maxValue, size_t fixedCharLength, ByteDataDistribution* dataDistribution)
        : Parameter(BasicType::TEXT, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
        this->fixedCharLength = fixedCharLength;
    }
    size_t minValue{};
    size_t maxValue{};
    size_t fixedCharLength{};
};

// ====================================================================================================
// Data Generator
// ====================================================================================================
/*
template<typename... Args>
class ByteDataGenerator : public DataGenerator {
  public:
    explicit ByteDataGenerator(Schema::MemoryLayoutType layoutType, const Args&... tupleArgs);

    std::string getName() override;
    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;
    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize, const Args&... tupleArgs);

    SchemaPtr getSchema() override;
    //DistributionName getDistributionName();
    std::string toString() override;

  private:
    SchemaPtr schema;
};
*/
template<typename... Args>
class ByteDataGenerator : public DataGenerator {
  public:
    explicit ByteDataGenerator(Schema::MemoryLayoutType layoutType, const Args&... tupleArgs) {
        this->schema = Schema::create();
        schema->setLayoutType(layoutType);
        size_t i = 0;
        (
            [&] {
                Parameter baseParameter = (Parameter&&) tupleArgs;
                if (baseParameter.nesType == BasicType::TEXT) {
                    TextParameter textParameter = (TextParameter&&) tupleArgs;
                    DataTypePtr dataType = std::make_shared<FixedChar>(textParameter.fixedCharLength);
                    std::string str = "f_" + std::to_string(i);
                    schema->addField(str, dataType);
                } else {
                    std::string str = "f_" + std::to_string(i);
                    schema->addField(str, baseParameter.nesType);
                }
                i++;
            }(),
            ...);
    }

    std::string getName() override { return "Bytes"; }
    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override {
        (void) numberOfBuffers;
        (void) bufferSize;
        NES_NOT_IMPLEMENTED();
    }
    std::vector<Runtime::TupleBuffer> createData(size_t numBuffers, size_t bufferSize, const Args&... tupleArgs) {
        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numBuffers);

        for (uint64_t curBuffer = 0; curBuffer < numBuffers; curBuffer++) {
            Runtime::TupleBuffer bufferRef = DataGenerator::allocateBuffer();
            auto dynamicBuffer = Runtime::MemoryLayouts::CompressedDynamicTupleBuffer(getMemoryLayout(bufferSize), bufferRef);

            size_t columnIndex = 0;
            (// iterate over tupleArgs
                [&] {
                    Parameter baseParameter = (Parameter&&) tupleArgs;
                    switch (baseParameter.nesType) {
                        case BasicType::INT8: break;
                        case BasicType::UINT8: break;
                        case BasicType::INT16: break;
                        case BasicType::UINT16: break;
                        case BasicType::INT32: {
                            Int32Parameter parameter = (Int32Parameter&&) tupleArgs;
                            fillColumnInt32(dynamicBuffer, columnIndex, parameter);
                            break;
                        }
                        case BasicType::UINT32: break;
                        case BasicType::INT64: break;
                        case BasicType::FLOAT32: break;
                        case BasicType::UINT64: break;
                        case BasicType::FLOAT64: break;
                        case BasicType::BOOLEAN: break;
                        case BasicType::CHAR: break;
                        case BasicType::TEXT: {
                            TextParameter parameter = (TextParameter&&) tupleArgs;
                            fillColumnText(dynamicBuffer, columnIndex, parameter);
                            break;
                        }
                    }
                    columnIndex++;
                }(),
                ...);
            createdBuffers.push_back(bufferRef);
        }
        return createdBuffers;
    }

    SchemaPtr getSchema() override { return schema; }
    //DistributionName getDistributionName();
    std::string toString() override {
        std::ostringstream oss;
        oss << getName();
        return oss.str();
    }

  private:
    SchemaPtr schema;
    template<typename T>
    static void randomizeHistogramOrder(std::map<T, size_t>& histogram, size_t seed) {
        std::mt19937 gen(seed);
        std::uniform_int_distribution<size_t> randomIndex(0, histogram.size() - 1);
        std::vector<T> keys;
        keys.reserve(histogram.size());
        for (const auto& [key, _] : histogram) {
            keys.push_back(key);
        }
        for (size_t i = 0; i < histogram.size(); i++) {
            size_t swap1 = keys[randomIndex(gen)];
            size_t swap2 = keys[randomIndex(gen)];
            auto tmp = histogram[swap1];
            histogram[swap1] = histogram[swap2];
            histogram[swap2] = tmp;
        }
    }
    template<typename T>
    static void fillColumnRepeatingValues(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& buffer,
                                          size_t columnIndex,
                                          RepeatingValues& distribution,
                                          T minValue,
                                          T maxValue) {
        if (distribution.sort)
            NES_THROW_RUNTIME_ERROR("Sorting this distribution is not allowed.");
        std::mt19937 gen(distribution.seed);
        std::uniform_int_distribution<T> randomValue(minValue, maxValue);
        auto minDeviation = distribution.numRepeats - distribution.sigma;
        if (minDeviation < 0)
            minDeviation = 0;
        auto maxDeviation = distribution.numRepeats + distribution.sigma;
        std::uniform_int_distribution<> randomRepeat(minDeviation, maxDeviation);
        auto value = randomValue(gen);
        std::vector repeats(buffer.getOffsets().size(), distribution.numRepeats);
        int curRun = 0;
        size_t row = 0;
        while (row < buffer.getCapacity()) {
            while (curRun < repeats[columnIndex] && row < buffer.getCapacity()) {
                buffer[row][columnIndex].write<T>(value);
                curRun++;
                row++;
            }
            auto newValue = randomValue(gen);
            while (newValue == value)
                newValue = randomValue(gen);
            value = newValue;
            bool changeRepeats = (rand() % 100) < (100 * distribution.changeProbability);
            if (changeRepeats)
                repeats[columnIndex] = randomRepeat(gen);
            curRun = 0;
        }
    }
    template<typename T>
    static void fillColumnUniform(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& buffer,
                                  size_t columnIndex,
                                  Uniform& distribution,
                                  T minValue,
                                  T maxValue) {
        std::mt19937 gen(distribution.seed);
        std::uniform_int_distribution<uint8_t> randomValue(minValue, maxValue);
        if (distribution.sort) {
            // count all value occurrences
            std::map<uint8_t, size_t> histogram;
            for (size_t row = 0; row < buffer.getCapacity(); row++) {
                histogram[randomValue(gen)]++;
            }
            randomizeHistogramOrder(histogram, distribution.seed);
            // write values to buffer
            size_t row = 0;
            for (const auto& [value, count] : histogram) {
                for (size_t i = 0; i < count; i++) {
                    buffer[row][columnIndex].write<uint8_t>(value);
                    row++;
                }
            }
        } else {
            for (size_t row = 0; row < buffer.getCapacity(); row++)
                buffer[row][columnIndex].write<uint8_t>(randomValue(gen));
        }
    }
    template<typename T>
    static void fillColumnBinomial(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& buffer,
                                   size_t columnIndex,
                                   Binomial& distribution,
                                   T minValue,
                                   T maxValue) {
        std::mt19937 gen(distribution.seed);
        std::binomial_distribution<T> randomValue(maxValue - minValue, distribution.probability);
        if (distribution.sort) {
            // count all value occurrences
            std::map<T, size_t> histogram;
            for (size_t row = 0; row < buffer.getCapacity(); row++) {
                histogram[randomValue(gen) + minValue]++;
            }
            randomizeHistogramOrder(histogram, distribution.seed);
            // write values to buffer
            size_t row = 0;
            for (const auto& [value, count] : histogram) {
                for (size_t i = 0; i < count; i++) {
                    buffer[row][columnIndex].write<T>(value);
                    row++;
                }
            }
        } else {
            for (size_t row = 0; row < buffer.getCapacity(); row++) {
                buffer[row][columnIndex].write<T>(randomValue(gen) + minValue);
            }
        }
    }
    template<typename T>
    static void fillColumnZipf(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& buffer,
                               size_t columnIndex,
                               Zipf& distribution,
                               T minValue,
                               T maxValue) {
        std::mt19937 gen(distribution.seed);
        ZipfianGenerator randomValue(minValue, maxValue, distribution.alpha);
        if (distribution.sort) {
            // count all value occurrences
            std::map<uint8_t, size_t> histogram;
            for (size_t row = 0; row < buffer.getCapacity(); row++) {
                histogram[randomValue(gen)]++;
            }
            randomizeHistogramOrder(histogram, distribution.seed);
            // write values to buffer
            size_t row = 0;
            for (const auto& [value, count] : histogram) {
                for (size_t i = 0; i < count; i++) {
                    buffer[row][columnIndex].write<uint8_t>(value);
                    row++;
                }
            }
        } else {
            for (size_t row = 0; row < buffer.getCapacity(); row++)
                buffer[row][columnIndex].write<uint8_t>(randomValue(gen));
        }
    }
    static void getTextValue(char* out, const std::string& filePath, size_t lineNumber) {
        std::string line;
        std::ifstream f(filePath);
        for (size_t i = 0; i <= lineNumber; i++)
            std::getline(f, line);
        strcpy(out, line.c_str());
    }
    static void fillColumnRepeatingValuesText(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& buffer,
                                              size_t columnIndex,
                                              RepeatingValues* distribution,
                                              size_t minIndexValue,
                                              size_t maxIndexValue,
                                              DataSet dataSet) {
        if (distribution->sort)
            NES_THROW_RUNTIME_ERROR("Sorting this distribution is not allowed.");
        switch (dataSet) {
            case IPSUM_WORDS: {
                if (maxIndexValue > IpsumWords::maxIndexValue)
                    NES_THROW_RUNTIME_ERROR("maxIndexValue cannot be larger than " << IpsumWords::maxIndexValue);
                break;
            }
            case IPSUM_SENTENCES: {
                if (maxIndexValue > IpsumSentences::maxIndexValue)
                    NES_THROW_RUNTIME_ERROR("maxIndexValue cannot be larger than " << IpsumSentences::maxIndexValue);
                break;
            }
        }
        std::mt19937 gen(distribution->seed);
        std::uniform_int_distribution<size_t> randomIndex(minIndexValue, maxIndexValue);
        auto minDeviation = distribution->numRepeats - distribution->sigma;
        if (minDeviation < 0)
            minDeviation = 0;
        auto maxDeviation = distribution->numRepeats + distribution->sigma;
        std::uniform_int_distribution<> randomRepeat(minDeviation, maxDeviation);
        auto value = randomIndex(gen) % maxIndexValue;
        std::vector repeats(buffer.getOffsets().size(), distribution->numRepeats);
        int curRun = 0;
        size_t row = 0;
        while (row < buffer.getCapacity()) {
            switch (dataSet) {

                case IPSUM_WORDS: {
                    while (curRun < repeats[columnIndex] && row < buffer.getCapacity()) {
                        char* textValue = (char*) malloc(sizeof(char) * IpsumWords::maxValueLength);
                        getTextValue(textValue, IpsumWords::filePath, value);
                        ExecutableTypes::Array<char, IpsumWords::maxValueLength> str(textValue);
                        buffer[row][columnIndex].write<ExecutableTypes::Array<char, IpsumWords::maxValueLength>>(str);
                        curRun++;
                        row++;
                    }
                    break;
                }
                case IPSUM_SENTENCES: break;
            }
            if (dataSet == DataSet::IPSUM_WORDS) {
                while (curRun < repeats[columnIndex] && row < buffer.getCapacity()) {
                    char* textValue = (char*) malloc(sizeof(char) * IpsumSentences::maxValueLength);
                    getTextValue(textValue, IpsumSentences::filePath, value);
                    ExecutableTypes::Array<char, IpsumSentences::maxValueLength> str(textValue);
                    buffer[row][columnIndex].write<ExecutableTypes::Array<char, IpsumSentences::maxValueLength>>(str);
                    curRun++;
                    row++;
                }
            }
            auto newValue = randomIndex(gen) % maxIndexValue;
            while (newValue == value)
                newValue = randomIndex(gen) % maxIndexValue;
            value = newValue;
            bool changeRepeats = (rand() % 100) < (100 * distribution->changeProbability);
            if (changeRepeats)
                repeats[columnIndex] = randomRepeat(gen);
            curRun = 0;
        }
    }

    static void
    fillColumnInt32(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& buffer, size_t columnIndex, Int32Parameter parameter) {
        switch (parameter.dataDistribution->getName()) {
            case DistributionName::REPEATING_VALUES: {
                auto distribution = (RepeatingValues&&) parameter.dataDistribution;
                fillColumnRepeatingValues<int32_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::UNIFORM: {
                auto distribution = (Uniform&&) parameter.dataDistribution;
                fillColumnUniform<int32_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::BINOMIAL: {
                auto distribution = (Binomial&&) parameter.dataDistribution;
                fillColumnBinomial<int32_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::ZIPF: {
                auto distribution = (Zipf&&) parameter.dataDistribution;
                fillColumnZipf<int32_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
        }
        //buffer.setNumberOfTuples(dynamicBuffer.getCapacity()); // TODO set capacity
    }
    static void
    fillColumnInt64(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& buffer, size_t columnIndex, Int64Parameter parameter) {
        switch (parameter.dataDistribution->getName()) {
            case DistributionName::REPEATING_VALUES: {
                auto distribution = (RepeatingValues&&) parameter.dataDistribution;
                fillColumnRepeatingValues<int64_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::UNIFORM: {
                auto distribution = (Uniform&&) parameter.dataDistribution;
                fillColumnUniform<int64_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::BINOMIAL: {
                auto distribution = (Binomial&&) parameter.dataDistribution;
                fillColumnBinomial<int64_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::ZIPF: {
                auto distribution = (Zipf&&) parameter.dataDistribution;
                fillColumnZipf<int64_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
        }
        //buffer.setNumberOfTuples(dynamicBuffer.getCapacity()); // TODO set capacity
    }
    static void fillColumnDouble(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& buffer,
                                 size_t columnIndex,
                                 DoubleParameter parameter) {
        switch (parameter.dataDistribution->getName()) {
            case DistributionName::REPEATING_VALUES: {
                auto distribution = (RepeatingValues&&) parameter.dataDistribution;
                fillColumnRepeatingValues<double>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::UNIFORM: {
                auto distribution = (Uniform&&) parameter.dataDistribution;
                fillColumnUniform<double>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::BINOMIAL: {
                auto distribution = (Binomial&&) parameter.dataDistribution;
                fillColumnBinomial<double>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::ZIPF: {
                auto distribution = (Zipf&&) parameter.dataDistribution;
                fillColumnZipf<double>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
        }
        //buffer.setNumberOfTuples(dynamicBuffer.getCapacity()); // TODO set capacity
    }
    static void
    fillColumnText(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& buffer, size_t columnIndex, TextParameter parameter) {
        switch (parameter.dataDistribution->getName()) {
            case DistributionName::REPEATING_VALUES: {
                auto* distribution = dynamic_cast<RepeatingValues*>(parameter.dataDistribution);
                fillColumnRepeatingValuesText(buffer,
                                              columnIndex,
                                              distribution,
                                              parameter.minValue,
                                              parameter.maxValue,
                                              DataSet::IPSUM_WORDS);
                break;
            }
            default:
                NES_THROW_RUNTIME_ERROR("Not implemented");
                /*
            case DistributionName::UNIFORM: {
                auto distribution = (Uniform&&) parameter.dataDistribution;
                fillColumnUniform<int32_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::BINOMIAL: {
                auto distribution = (Binomial&&) parameter.dataDistribution;
                fillColumnBinomial<int32_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
            case DistributionName::ZIPF: {
                auto distribution = (Zipf&&) parameter.dataDistribution;
                fillColumnZipf<int32_t>(buffer, columnIndex, distribution, parameter.minValue, parameter.maxValue);
                break;
            }
                 */
        }
        //buffer.setNumberOfTuples(dynamicBuffer.getCapacity()); // TODO set capacity
    }
};

}// namespace NES::Benchmark::DataGeneration

#endif//NES_BYTEDATAGENERATOR_HPP

#ifndef NES_GENERICDATAGEN_HPP
#define NES_GENERICDATAGEN_HPP

#include "Common/DataTypes/BasicTypes.hpp"
#include "Common/DataTypes/FixedChar.hpp"
#include "Common/ExecutableType/Array.hpp"
#include "DataGeneration/data/TextDataSet.hpp"
#include "DataGenerator.hpp"
#include "Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp"
#include "Runtime/TupleBuffer.hpp"
#include "Util/ZipfianGenerator.hpp"
#include <API/Schema.hpp>
#include <fstream>
#include <random>
#include <type_traits>
#include <utility>
#include <vector>

namespace NES::Benchmark::DataGeneration {

enum class DistributionName2 { DELTA_VALUES, REPEATING_VALUES, UNIFORM, BINOMIAL, ZIPF };
NLOHMANN_JSON_SERIALIZE_ENUM(DistributionName2,
                             {{DistributionName2::DELTA_VALUES, "DELTA_VALUES"},
                              {DistributionName2::REPEATING_VALUES, "REPEATING_VALUES"},
                              {DistributionName2::UNIFORM, "UNIFORM"},
                              {DistributionName2::BINOMIAL, "BINOMIAL"},
                              {DistributionName2::ZIPF, "ZIPF"}})

[[maybe_unused]] static std::string getDistributionName(enum DistributionName2 dn) {
    switch (dn) {
        case DistributionName2::DELTA_VALUES: return "Delta Values";
        case DistributionName2::REPEATING_VALUES: return "Repeating Values";
        case DistributionName2::UNIFORM: return "Uniform";
        case DistributionName2::BINOMIAL: return "Binomial";
        case DistributionName2::ZIPF: return "Zipf";
    }
}

class Distribution {
  public:
    Distribution() = default;
    /*
    template<typename T>
    T getRandomValue() {};
     */
    DistributionName2 distributionName;
    NES::BasicType nesType;
    DataSet dataSet = DataSet::INVALID;
    TextDataSet textDataSet = TextDataSet(dataSet);
    //bool sort = false;
    /*
    void setTextDataSet(DataSet dataSet){
        this->textDataSet = TextDataSet(dataSet);
    }
     */
    void setDataSet(DataSet newDataSet) {
        this->dataSet = newDataSet;
        this->textDataSet = TextDataSet(dataSet);
        this->nesType = BasicType::TEXT;
    }

  protected:
    template<typename T>
    Distribution(T someValue, DistributionName2 distributionName, size_t seed) {
        this->distributionName = distributionName;
        this->nesType = TypeMap[std::type_index(typeid(someValue))];
        this->seed = seed;
        gen = std::mt19937(seed);
    };
    static void getTextValue(char* out, const std::string& filePath, size_t lineNumber) {
        std::string line;
        std::ifstream f(filePath);
        for (size_t i = 0; i <= lineNumber; i++)
            std::getline(f, line);
        strcpy(out, line.c_str());
    }
    /*
    static void getTextValue2(const std::shared_ptr<char[]>& ptr, const std::string& filePath, size_t lineNumber) {
        std::string line;
        std::ifstream f(filePath);
        for (size_t i = 0; i <= lineNumber; i++)
            std::getline(f, line);
        strcpy(ptr.get(), line.c_str());
    }
     */
    size_t seed{};
    std::mt19937 gen;
};
using DistributionPtr = std::shared_ptr<Distribution>;

template<typename T>
class DeltaValueDistribution : public Distribution {
  public:
    DeltaValueDistribution(T initValue, int numRepeats, int sigma, T delta, T deltaDeviation, size_t seed) : Distribution(initValue, DistributionName2::DELTA_VALUES, seed) {
        this->lastValue = initValue;
        this->currentRun = numRepeats;
        this->sigma = sigma;
        this->delta = delta;

        auto minRunDeviation = numRepeats - sigma;
        if (minRunDeviation < 0)
            minRunDeviation = 0;
        auto maxRunDeviation = numRepeats + sigma;
        randomRepeatGen = std::uniform_int_distribution<int>(minRunDeviation, maxRunDeviation);

        auto minDeltaDeviation = delta - deltaDeviation;
        if (minDeltaDeviation < 0)
            minDeltaDeviation = 0;
        auto maxDeltaDeviation = delta + deltaDeviation;
        randomDeltaGen = std::uniform_int_distribution<T>(minDeltaDeviation, maxDeltaDeviation);
    }

    T getRandomValue() {
        if constexpr (std::is_integral_v<T>) {
            if (currentRun < currentRepeat) {
                currentRun++;
            } else {
                // generate new step
                this->delta = randomDeltaGen(gen);
                auto direction = 0 + (rand() % (1 - 0 + 1)) == 1;
                if (direction == 0)
                    this->delta *= -1;

                // generate new value
                lastValue += delta;

                // generate new run-length
                currentRepeat = randomRepeatGen(gen);
                currentRun = 0;
            }
            return lastValue;
        } else {
            // TODO
            NES_NOT_IMPLEMENTED();
        }
    }

  protected:
    T lastValue;
    int sigma{};
    T delta;
    size_t currentRun{};
    size_t currentRepeat{};
    std::uniform_int_distribution<int> randomRepeatGen;
    std::uniform_int_distribution<T> randomDeltaGen;
};
template<typename T>
using DeltaValueDistributionPtr = std::shared_ptr<DeltaValueDistribution<T>>;

template<typename T>
class RepeatingValuesDistribution : public Distribution {
  public:
    RepeatingValuesDistribution(T minValue, T maxValue, int numRepeats, int sigma, double changeProbability, size_t seed)
        : Distribution(minValue, DistributionName2::REPEATING_VALUES, seed) {
        this->minValue = minValue;
        this->maxValue = maxValue;
        this->numRepeats = numRepeats;
        this->sigma = sigma;
        this->changeProbability = changeProbability;

        auto minDeviation = numRepeats - sigma;
        if (minDeviation < 0)
            minDeviation = 0;
        auto maxDeviation = numRepeats + sigma;
        randomRepeatGen = std::uniform_int_distribution<int>(minDeviation, maxDeviation);
        //lastValue = randomValueGen(gen);
        lastValue = minValue;
    }
    [[maybe_unused]] RepeatingValuesDistribution(T minValue, T maxValue, int numRepeats, int sigma, double changeProbability) {
        std::random_device rd;
        RepeatingValuesDistribution(minValue, maxValue, numRepeats, sigma, changeProbability, rd());
    }

    T getRandomValue() {
        if constexpr (std::is_same<T, bool>::value) {
            // TODO
            NES_NOT_IMPLEMENTED();
        } else if constexpr (std::is_integral_v<T>) {
            auto randomValueGen = std::uniform_int_distribution<T>(minValue, maxValue);
            if (currentRun < currentRepeat) {
                currentRun++;
            } else {
                auto newValue = randomValueGen(gen);
                while (newValue == lastValue)
                    newValue = randomValueGen(gen);
                lastValue = newValue;
                bool changeRepeats = (std::rand() % 100) < (100 * changeProbability);
                if (changeRepeats)
                    currentRepeat = randomRepeatGen(gen);
                currentRun = 0;
            }
            return lastValue;
        } else if constexpr (std::is_floating_point_v<T>) {
            auto randomValueGen = std::uniform_int_distribution<int>(minValue, maxValue);
            if (currentRun < currentRepeat) {
                currentRun++;
            } else {
                auto r = randomValueGen(gen);
                auto newValue = r + ((T) r / ((int) maxValue - minValue));
                while (newValue == lastValue) {
                    r = randomValueGen(gen);
                    newValue = r + ((T) r / ((int) maxValue - minValue));
                }
                lastValue = newValue;
                bool changeRepeats = (std::rand() % 100) < (100 * changeProbability);
                if (changeRepeats)
                    currentRepeat = randomRepeatGen(gen);
                currentRun = 0;
            }
            return lastValue;
        }
    }

  protected:
    T minValue;
    T maxValue;
    int numRepeats{};
    int sigma{};
    double changeProbability{};
    std::uniform_int_distribution<int> randomRepeatGen;
    T lastValue;
    size_t currentRun{};
    size_t currentRepeat{};
};
template<>
class RepeatingValuesDistribution<std::string> : public RepeatingValuesDistribution<int> {
  public:
    RepeatingValuesDistribution<std::string>(int minValue,
                                             int maxValue,
                                             int numRepeats,
                                             int sigma,
                                             double changeProbability,
                                             size_t seed,
                                             DataSet dataSet)
        : RepeatingValuesDistribution<int>(minValue, maxValue, numRepeats, sigma, changeProbability, seed) {
        setDataSet(dataSet);
    }
    char* getRandomValue() {
        //std::cout << "xxx\nSeed rep: " << seed << "\nXXX" << std::endl;// TODO
        auto randomValueGen = std::uniform_int_distribution<int>(minValue, maxValue);
        if (currentRun < currentRepeat) {
            currentRun++;
        } else {
            auto newValue = randomValueGen(gen);
            while (newValue == lastValue)
                newValue = randomValueGen(gen);
            lastValue = newValue;
            bool changeRepeats = (std::rand() % 100) < (100 * changeProbability);
            if (changeRepeats)
                currentRepeat = randomRepeatGen(gen);
            currentRun = 0;
        }

        char* textValue = (char*) malloc(sizeof(char) * textDataSet.maxValueLength);
        switch (dataSet) {
            case INVALID: NES_THROW_RUNTIME_ERROR("invalid dataset");
            case IPSUM_WORDS: {
                getTextValue(textValue, IpsumWords::filePath, lastValue);
                return textValue;
            }
            case IPSUM_SENTENCES: {
                getTextValue(textValue, IpsumSentences::filePath, lastValue);
                return textValue;
            }
            case TOP_500_DOMAINS: {
                getTextValue(textValue, Top500Urls::filePath, lastValue);
                return textValue;
            }
            case WIKI_TITLES_10000: {
                getTextValue(textValue, WikiTitles10000::filePath, lastValue);
                return textValue;
            }
        }
    }
};
template<typename T>
using RepeatingValuesDistributionPtr = std::shared_ptr<RepeatingValuesDistribution<T>>;

template<typename T>
class UniformDistribution : public Distribution {
  public:
    UniformDistribution(T minValue, T maxValue, size_t seed) : Distribution(minValue, DistributionName2::UNIFORM, seed) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    [[maybe_unused]] UniformDistribution(T minValue, T maxValue) {
        std::random_device rd;
        return UniformDistribution(minValue, maxValue, rd());
    }

    T getRandomValue() {

        if constexpr (std::is_same<T, bool>::value) {
            // TODO
            NES_NOT_IMPLEMENTED();
        } else if constexpr (std::is_integral_v<T>) {
            std::uniform_int_distribution<T> randomIntValueGen = std::uniform_int_distribution<T>(minValue, maxValue);
            return randomIntValueGen(gen);
        } else if (std::is_floating_point_v<T>) {
            std::uniform_real_distribution<T> randomRealValueGen = std::uniform_real_distribution<T>(minValue, maxValue);
            return randomRealValueGen(gen);
        }
    }

  protected:
    T minValue;
    T maxValue;
};
template<>
class UniformDistribution<std::string> : public UniformDistribution<int> {
  public:
    UniformDistribution<std::string>(int minValue, int maxValue, size_t seed, DataSet dataSet)
        : UniformDistribution<int>(minValue, maxValue, seed) {
        setDataSet(dataSet);
    }
    char* getRandomValue() {
        auto randomIntValueGen = std::uniform_int_distribution<int>(minValue, maxValue);
        auto value = randomIntValueGen(gen);

        char* textValue = (char*) malloc(sizeof(char) * textDataSet.maxValueLength);
        switch (dataSet) {
            case INVALID: NES_THROW_RUNTIME_ERROR("invalid dataset");
            case IPSUM_WORDS: {
                getTextValue(textValue, IpsumWords::filePath, value);
                return textValue;
            }
            case IPSUM_SENTENCES: {
                getTextValue(textValue, IpsumSentences::filePath, value);
                return textValue;
            }
            case TOP_500_DOMAINS: {
                getTextValue(textValue, Top500Urls::filePath, value);
                return textValue;
            }
            case WIKI_TITLES_10000: {
                getTextValue(textValue, WikiTitles10000::filePath, value);
                return textValue;
            }
        }
    }
};
template<typename T>
using UniformDistributionPtr = std::shared_ptr<UniformDistribution<T>>;

template<typename T>
class BinomialDistribution : public Distribution {
  public:
    BinomialDistribution(T minValue, T maxValue, double probability, size_t seed)
        : Distribution(minValue, DistributionName2::BINOMIAL, seed) {
        this->minValue = minValue;
        this->maxValue = maxValue;
        this->probability = probability;
    }
    [[maybe_unused]] BinomialDistribution(T minValue, T maxValue, double probability) {
        std::random_device rd;
        BinomialDistribution(minValue, maxValue, probability, rd());
    }

    T getRandomValue() {
        if constexpr (std::is_same<T, bool>::value || std::is_same<T, char>::value || std::is_same<T, signed char>::value
                      || std::is_same<T, short>::value || std::is_same<T, signed short>::value
                      || std::is_same<T, unsigned short>::value) {
            // TODO
            NES_NOT_IMPLEMENTED();
        } else if constexpr (std::is_same<T, unsigned char>::value) {
            if (this->maxValue > std::numeric_limits<uint8_t>::max())
                NES_THROW_RUNTIME_ERROR("Max value too large");
            std::binomial_distribution<int> randomValue = std::binomial_distribution((maxValue - minValue), probability);
            return (uint8_t) randomValue(gen) + minValue;
        } else if constexpr (std::is_integral_v<T>) {
            std::binomial_distribution<T> randomValue = std::binomial_distribution((maxValue - minValue), probability);
            return randomValue(gen) + minValue;
        } else if (std::is_floating_point_v<T>) {
            // https://stackoverflow.com/q/29472645
            int n = ((int) maxValue - minValue);
            std::binomial_distribution<int> randomValue = std::binomial_distribution(n, probability);
            auto r = randomValue(gen);
            //return (double) r / n + minValue;
            return r + ((T) r / ((int) maxValue - minValue)) + minValue;
        }
    }

  protected:
    T minValue;
    T maxValue;
    double probability{};
};
template<>
class BinomialDistribution<std::string> : public BinomialDistribution<int> {
  public:
    BinomialDistribution<std::string>(int minValue, int maxValue, double probability, size_t seed, DataSet dataSet)
        : BinomialDistribution<int>(minValue, maxValue, probability, seed) {
        setDataSet(dataSet);
    }
    char* getRandomValue() {
        auto randomValueGen = std::binomial_distribution((maxValue - minValue), probability);
        auto value = randomValueGen(gen) + minValue;

        char* textValue = (char*) malloc(sizeof(char) * textDataSet.maxValueLength);
        switch (dataSet) {
            case INVALID: NES_THROW_RUNTIME_ERROR("invalid dataset");
            case IPSUM_WORDS: {
                getTextValue(textValue, IpsumWords::filePath, value);
                return textValue;
            }
            case IPSUM_SENTENCES: {
                getTextValue(textValue, IpsumSentences::filePath, value);
                return textValue;
            }
            case TOP_500_DOMAINS: {
                getTextValue(textValue, Top500Urls::filePath, value);
                return textValue;
            }
            case WIKI_TITLES_10000: {
                getTextValue(textValue, WikiTitles10000::filePath, value);
                return textValue;
            }
        }
    }
};
template<typename T>
using BinomialDistributionPtr = std::shared_ptr<BinomialDistribution<T>>;

template<typename T>
class ZipfianDistribution : public Distribution {
  public:
    ZipfianDistribution(T minValue, T maxValue, double alpha, size_t seed)
        : Distribution(minValue, DistributionName2::ZIPF, seed), minValue(minValue), maxValue(maxValue), alpha(alpha),
          randomValueGen(minValue, maxValue, alpha) {}
    ZipfianDistribution(T minValue, T maxValue, double alpha) {
        std::random_device rd;
        ZipfianDistribution(minValue, maxValue, alpha, rd());
    }

    T getRandomValue() {
        if (std::is_floating_point_v<T>) {
            auto r = randomValueGen(gen);
            if (r == minValue) {
                return r + 0.1415926535;
            }
            auto offset = r / minValue;
            auto res = r + offset;
            if (res > maxValue)
                return maxValue - offset;
            return res;
        }
        return randomValueGen(gen);
    }

  protected:
    T minValue;
    T maxValue;
    double alpha{};
    ZipfianGenerator randomValueGen;
};
template<>
class ZipfianDistribution<std::string> : public ZipfianDistribution<uint64_t> {
  public:
    ZipfianDistribution<std::string>(uint64_t minValue, uint64_t maxValue, double alpha, size_t seed, DataSet dataSet)
        : ZipfianDistribution<uint64_t>(minValue, maxValue, alpha, seed) {
        setDataSet(dataSet);
    }
    char* getRandomValue() {
        auto value = randomValueGen(gen);

        char* textValue = (char*) malloc(sizeof(char) * textDataSet.maxValueLength);
        switch (dataSet) {
            case INVALID: NES_THROW_RUNTIME_ERROR("invalid dataset");
            case IPSUM_WORDS: {
                getTextValue(textValue, IpsumWords::filePath, value);
                return textValue;
            }
            case IPSUM_SENTENCES: {
                getTextValue(textValue, IpsumSentences::filePath, value);
                return textValue;
            }
            case TOP_500_DOMAINS: {
                getTextValue(textValue, Top500Urls::filePath, value);
                return textValue;
            }
            case WIKI_TITLES_10000: {
                getTextValue(textValue, WikiTitles10000::filePath, value);
                return textValue;
            }
        }
    }
};
template<typename T>
using ZipfianDistributionPtr = std::shared_ptr<ZipfianDistribution<T>>;

class GenericDataGen : public DataGenerator {
  public:
    explicit GenericDataGen(Schema::MemoryLayoutType layoutType, std::vector<DistributionPtr> distributions)
        : distributions(distributions) {
        schema = Schema::create();
        schema->setLayoutType(layoutType);
        for (size_t i = 0; i < distributions.size(); i++) {
            std::string str = "f_" + std::to_string(i);
            if (distributions[i]->nesType == NES::BasicType::TEXT) {
                size_t fixedCharLength = distributions[i]->textDataSet.maxValueLength;
                if (fixedCharLength == 0)
                    NES_THROW_RUNTIME_ERROR("Array length is too small");
                DataTypePtr dataType = std::make_shared<FixedChar>(fixedCharLength);
                schema->addField(str, dataType);
            } else {
                schema->addField(str, distributions[i]->nesType);
            }
        }
    }
    SchemaPtr getSchema() override { return this->schema; }
    std::string toString() override { NES_NOT_IMPLEMENTED(); }
    std::string getName() override { NES_NOT_IMPLEMENTED(); }

    std::vector<NES::Runtime::TupleBuffer> createData(size_t numBuffers, size_t bufferSize) override {
        std::vector<Runtime::TupleBuffer> createdBuffers;
        createdBuffers.reserve(numBuffers);
        auto memoryLayout = getMemoryLayout(bufferSize);
        size_t numColumns = schema->fields.size();
        if (numColumns != distributions.size())
            NES_THROW_RUNTIME_ERROR("Number of columns does not match number of distributions.");

        for (uint64_t curBuffer = 0; curBuffer < numBuffers; curBuffer++) {
            Runtime::TupleBuffer bufferRef = DataGenerator::allocateBuffer();
            auto dynamicBuffer = Runtime::MemoryLayouts::CompressedDynamicTupleBuffer(memoryLayout, bufferRef);
            size_t numTuples = 0;
            for (size_t columnIdx = 0; columnIdx < numColumns; columnIdx++) {
                auto distribution = distributions[columnIdx];
                switch (distribution->distributionName) {
                    case DistributionName2::DELTA_VALUES: {
                        numTuples = fillWithDeltaValuesDist(dynamicBuffer, columnIdx, distribution);
                        break;
                    }
                    case DistributionName2::REPEATING_VALUES: {
                        numTuples = fillWithRepeatingValuesDist(dynamicBuffer, columnIdx, distribution);
                        break;
                    }
                    case DistributionName2::UNIFORM: {
                        numTuples = fillWithUniformDist(dynamicBuffer, columnIdx, distribution);
                        break;
                    }
                    case DistributionName2::BINOMIAL: {
                        numTuples = fillWithBinomialDist(dynamicBuffer, columnIdx, distribution);
                        break;
                    }
                    case DistributionName2::ZIPF: {
                        numTuples = fillWithZipfianDist(dynamicBuffer, columnIdx, distribution);
                        break;
                    }
                }
            }
            bufferRef.setNumberOfTuples(numTuples);
            createdBuffers.push_back(bufferRef);
        }
        return createdBuffers;
    }

    std::vector<DistributionPtr> distributions;

  private:
    SchemaPtr schema;
    static size_t fillWithDeltaValuesDist(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& dynamicBuffer,
                                    size_t columnIdx,
                                    const DistributionPtr& distribution);
    static size_t fillWithRepeatingValuesDist(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& dynamicBuffer,
                                              size_t columnIdx,
                                              const DistributionPtr& distribution);
    static size_t fillWithUniformDist(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& dynamicBuffer,
                                      size_t columnIdx,
                                      const DistributionPtr& distribution);
    static size_t fillWithBinomialDist(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& dynamicBuffer,
                                       size_t columnIdx,
                                       const DistributionPtr& distribution);
    static size_t fillWithZipfianDist(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& dynamicBuffer,
                                      size_t columnIdx,
                                      const DistributionPtr& distribution);
};

size_t GenericDataGen::fillWithDeltaValuesDist(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& dynamicBuffer,
                                                   size_t columnIdx,
                                                   const DistributionPtr& distribution) {
    size_t row = 0;
    switch (distribution->nesType) {
        case BasicType::UINT8: {
            auto derivedDistribution = std::static_pointer_cast<DeltaValueDistribution<uint8_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint8_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        default:{
            NES_NOT_IMPLEMENTED();
        }
    }
    return row;
}

size_t GenericDataGen::fillWithRepeatingValuesDist(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& dynamicBuffer,
                                                   size_t columnIdx,
                                                   const DistributionPtr& distribution) {
    size_t row = 0;
    switch (distribution->nesType) {
        case BasicType::INT8: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<int8_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int8_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT8: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<uint8_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint8_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::INT16: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<int16_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int16_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT16: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<uint16_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint16_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::INT32: {
            if (distribution->dataSet != DataSet::INVALID)
                goto go_to_text_label;
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<int32_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int32_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT32: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<uint32_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint32_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::INT64: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<int64_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int64_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::FLOAT32: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<float>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<float>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT64: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<uint64_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint64_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::FLOAT64: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<double>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<double>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::BOOLEAN: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<bool>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<bool>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::CHAR: {
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<char>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<char>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::TEXT: {
        go_to_text_label:
            auto derivedDistribution = std::static_pointer_cast<RepeatingValuesDistribution<std::string>>(distribution);
            switch (derivedDistribution->dataSet) {
                case INVALID: NES_THROW_RUNTIME_ERROR("Invalid dataset.");
                case IPSUM_WORDS: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, IpsumWords::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, IpsumWords::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case IPSUM_SENTENCES: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, IpsumSentences::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, IpsumSentences::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case TOP_500_DOMAINS: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, Top500Urls::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, Top500Urls::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case WIKI_TITLES_10000: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, WikiTitles10000::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, WikiTitles10000::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
            }
            break;
        }
    }
    return row;
}

size_t GenericDataGen::fillWithUniformDist(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& dynamicBuffer,
                                           size_t columnIdx,
                                           const DistributionPtr& distribution) {
    size_t row = 0;
    switch (distribution->nesType) {
        case BasicType::INT8: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<int8_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int8_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT8: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<uint8_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint8_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::INT16: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<int16_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int16_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT16: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<uint16_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint16_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::INT32: {
            if (distribution->dataSet != DataSet::INVALID)
                goto go_to_text_label;
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<int32_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int32_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT32: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<uint32_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint32_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::INT64: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<int64_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int64_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::FLOAT32: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<float>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<float>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT64: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<uint64_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint64_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::FLOAT64: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<double>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<double>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::BOOLEAN: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<bool>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<bool>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::CHAR: {
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<char>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<char>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::TEXT: {
        go_to_text_label:
            auto derivedDistribution = std::static_pointer_cast<UniformDistribution<std::string>>(distribution);
            switch (derivedDistribution->dataSet) {
                case INVALID: NES_THROW_RUNTIME_ERROR("Invalid dataset.");
                case IPSUM_WORDS: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, IpsumWords::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, IpsumWords::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case IPSUM_SENTENCES: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, IpsumSentences::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, IpsumSentences::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case TOP_500_DOMAINS: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, Top500Urls::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, Top500Urls::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case WIKI_TITLES_10000: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, WikiTitles10000::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, WikiTitles10000::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
            }
            break;
        }
    }
    return row;
}

size_t GenericDataGen::fillWithBinomialDist(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& dynamicBuffer,
                                            size_t columnIdx,
                                            const DistributionPtr& distribution) {
    size_t row = 0;
    switch (distribution->nesType) {
        case BasicType::INT8: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<int8_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int8_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT8: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<uint8_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint8_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::INT16: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<int16_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int16_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT16: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<uint16_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint16_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::INT32: {
            if (distribution->dataSet != DataSet::INVALID)
                goto go_to_text_label;
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<int32_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int32_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT32: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<uint32_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint32_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::INT64: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<int64_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int64_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::FLOAT32: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<float>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<float>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT64: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<uint64_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint64_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::FLOAT64: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<double>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<double>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::BOOLEAN: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<bool>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<bool>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::CHAR: {
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<char>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<char>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::TEXT: {
        go_to_text_label:
            auto derivedDistribution = std::static_pointer_cast<BinomialDistribution<std::string>>(distribution);
            switch (derivedDistribution->dataSet) {
                case INVALID: NES_THROW_RUNTIME_ERROR("Invalid dataset.");
                case IPSUM_WORDS: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, IpsumWords::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, IpsumWords::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case IPSUM_SENTENCES: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, IpsumSentences::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, IpsumSentences::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case TOP_500_DOMAINS: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, Top500Urls::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, Top500Urls::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case WIKI_TITLES_10000: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, WikiTitles10000::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, WikiTitles10000::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
            }
            break;
        }
    }
    return row;
}

size_t GenericDataGen::fillWithZipfianDist(Runtime::MemoryLayouts::CompressedDynamicTupleBuffer& dynamicBuffer,
                                           size_t columnIdx,
                                           const DistributionPtr& distribution) {
    size_t row = 0;
    switch (distribution->nesType) {
        case BasicType::UINT8: {
            auto derivedDistribution = std::static_pointer_cast<ZipfianDistribution<uint8_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint8_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::INT32: {
            auto derivedDistribution = std::static_pointer_cast<ZipfianDistribution<int32_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<int32_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::FLOAT64: {
            auto derivedDistribution = std::static_pointer_cast<ZipfianDistribution<double>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<double>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::UINT64: {
            auto derivedDistribution = std::static_pointer_cast<ZipfianDistribution<uint64_t>>(distribution);
            while (row < dynamicBuffer.getCapacity()) {
                dynamicBuffer[row][columnIdx].write<uint64_t>(derivedDistribution->getRandomValue());
                row++;
            }
            break;
        }
        case BasicType::TEXT: {
            auto derivedDistribution = std::static_pointer_cast<ZipfianDistribution<std::string>>(distribution);
            switch (derivedDistribution->dataSet) {
                case INVALID: NES_THROW_RUNTIME_ERROR("Invalid dataset.");
                case IPSUM_WORDS: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, IpsumWords::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, IpsumWords::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case IPSUM_SENTENCES: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, IpsumSentences::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, IpsumSentences::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case TOP_500_DOMAINS: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, Top500Urls::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, Top500Urls::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
                case WIKI_TITLES_10000: {
                    while (row < dynamicBuffer.getCapacity()) {
                        auto textValue = derivedDistribution->getRandomValue();
                        ExecutableTypes::Array<char, WikiTitles10000::maxValueLength> value(textValue);
                        dynamicBuffer[row][columnIdx].write<ExecutableTypes::Array<char, WikiTitles10000::maxValueLength>>(value);
                        row++;
                    }
                    break;
                }
            }
            break;
        }
        default: NES_NOT_IMPLEMENTED();
    }
    return row;
}

}// namespace NES::Benchmark::DataGeneration

#endif//NES_GENERICDATAGEN_HPP

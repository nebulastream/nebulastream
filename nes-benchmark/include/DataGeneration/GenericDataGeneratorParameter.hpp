#ifndef NES_GENERICDATAGENERATORPARAMETER_HPP
#define NES_GENERICDATAGENERATORPARAMETER_HPP

#include <Common/DataTypes/BasicTypes.hpp>
#include <DataGeneration/GenericDataDistribution.hpp>

// Unfortunately, we need these classes to provide a mapping between C++ types and NESTypes

namespace NES::Benchmark::DataGeneration {
class GenericDataGeneratorParameter {
  public:
    BasicType nesType;
    GenericDataDistribution* dataDistribution;
    GenericDataDistribution dataDistribution2;
  protected:
    explicit GenericDataGeneratorParameter(BasicType nesType, GenericDataDistribution* dataDistribution) {
        this->nesType = nesType;
        this->dataDistribution = dataDistribution;
    }
    explicit GenericDataGeneratorParameter(BasicType nesType, const GenericDataDistribution& dataDistribution) {
        this->nesType = nesType;
        this->dataDistribution2 = dataDistribution;
    }
    size_t minValue{};
    size_t maxValue{};
    size_t fixedCharLength = -1; // only needed for TextParameter, but needed here for memory allocation
};

class Int8Parameter : public GenericDataGeneratorParameter {
  public:
    Int8Parameter(int8_t minValue, int8_t maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::INT8, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    int8_t minValue{};
    int8_t maxValue{};
};

class Uint8Parameter : public GenericDataGeneratorParameter {
  public:
    Uint8Parameter(uint8_t minValue, uint8_t maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::UINT8, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    uint8_t minValue{};
    uint8_t maxValue{};
};

class Int16Parameter : public GenericDataGeneratorParameter {
  public:
    Int16Parameter(int16_t minValue, int16_t maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::INT16, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    int16_t minValue{};
    int16_t maxValue{};
};

class Uint16Parameter : public GenericDataGeneratorParameter {
  public:
    Uint16Parameter(uint16_t minValue, uint16_t maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::INT16, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    uint16_t minValue{};
    uint16_t maxValue{};
};

class Int32Parameter : public GenericDataGeneratorParameter {
  public:
    Int32Parameter(int32_t minValue, int32_t maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::INT32, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    Int32Parameter(int32_t minValue, int32_t maxValue, const GenericDataDistribution& dataDistribution)
        : GenericDataGeneratorParameter(BasicType::INT32, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    int32_t minValue{};
    int32_t maxValue{};
};

class Uint32Parameter : public GenericDataGeneratorParameter {
  public:
    Uint32Parameter(uint32_t minValue, uint32_t maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::UINT32, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    uint32_t minValue{};
    uint32_t maxValue{};
};

class Int64Parameter : public GenericDataGeneratorParameter {
  public:
    Int64Parameter(int32_t minValue, int64_t maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::INT64, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    int64_t minValue{};
    int64_t maxValue{};
};

class Uint64Parameter : public GenericDataGeneratorParameter {
  public:
    Uint64Parameter(uint32_t minValue, uint64_t maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::UINT64, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    uint64_t minValue{};
    uint64_t maxValue{};
};

class FloatParameter : public GenericDataGeneratorParameter {
  public:
    FloatParameter(float minValue, float maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::FLOAT32, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    float minValue{};
    float maxValue{};
};

class DoubleParameter : public GenericDataGeneratorParameter {
  public:
    DoubleParameter(double minValue, double maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::FLOAT64, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    double minValue{};
    double maxValue{};
};

class BooleanParameter : public GenericDataGeneratorParameter {
  public:
    BooleanParameter(bool minValue, bool maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::BOOLEAN, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    bool minValue{};
    bool maxValue{};
};

class CharParameter : public GenericDataGeneratorParameter {
  public:
    CharParameter(char minValue, char maxValue, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::CHAR, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
    }
    char minValue{};
    char maxValue{};
};

class TextParameter : public GenericDataGeneratorParameter {
  public:
    TextParameter(size_t minValue, size_t maxValue, size_t fixedCharLength, GenericDataDistribution* dataDistribution)
        : GenericDataGeneratorParameter(BasicType::TEXT, dataDistribution) {
        this->minValue = minValue;
        this->maxValue = maxValue;
        this->fixedCharLength = fixedCharLength;
    }
    size_t minValue{};
    size_t maxValue{};
    size_t fixedCharLength{};
};

}// namespace NES::Benchmark::DataGeneration

#endif//NES_GENERICDATAGENERATORPARAMETER_HPP

#ifndef NES_GENERICDATADISTRIBUTION_HPP
#define NES_GENERICDATADISTRIBUTION_HPP

namespace NES::Benchmark::DataGeneration {

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

class GenericDataDistribution {
  public:
    ~GenericDataDistribution() = default;
    virtual DistributionName getName() { return distributionName; }
    size_t seed = rd();
    bool sort = false;

  protected:
    DistributionName distributionName;
};

class RepeatingValues : public GenericDataDistribution {
  public:
    /**
     * Generate data with repeating values. Each column will has its own distribution.
     * @param numRepeats how often a value shall be repeated consecutively
     * @param sigma maximum deviation of `numRepeats`: [numRepeats - sigma, numRepeats + sigma]
     * @param changeProbability probability that numRepeats will change within [numRepeats - sigma, numRepeats + sigma]
     */
    explicit RepeatingValues(int numRepeats, uint8_t sigma = 0, double changeProbability = 0) {
        distributionName = DistributionName::REPEATING_VALUES;
        if (numRepeats < 1)
            NES_THROW_RUNTIME_ERROR("Number of numRepeats must be greater than 1.");// TODO max value
        if (sigma < 0)
            NES_THROW_RUNTIME_ERROR("Sigma must be greater than 0.");
        if (changeProbability < 0 || changeProbability > 1)
            NES_THROW_RUNTIME_ERROR("Change probability must be between 0 and 1.");
        this->numRepeats = numRepeats;
        this->sigma = sigma;
        this->changeProbability = changeProbability;
    }
    virtual ~RepeatingValues() {
        numRepeats = 0;
        sigma = 0;
        changeProbability = 0;
    }

    int numRepeats;
    uint8_t sigma;
    double changeProbability;
};

class Uniform : public GenericDataDistribution {
  public:
    /**
     * Generate uniformly distributed data
     */
    explicit Uniform() { distributionName = DistributionName::UNIFORM; }
};

class Binomial : public GenericDataDistribution {
  public:
    explicit Binomial(double probability) {
        if (probability < 0 || probability > 1)
            NES_THROW_RUNTIME_ERROR("Probability must be between 0 and 1.");
        distributionName = DistributionName::BINOMIAL;
        this->probability = probability;
    }

    double probability;
};

class Zipf : public GenericDataDistribution {
  public:
    explicit Zipf(double alpha) {
        if (alpha < 0 || alpha > 1)
            NES_THROW_RUNTIME_ERROR("Alpha must be between 0 and 1.");
        distributionName = DistributionName::ZIPF;
        this->alpha = alpha;
    }

    double alpha;
};
}// namespace NES::Benchmark::DataGeneration

#endif//NES_GENERICDATADISTRIBUTION_HPP

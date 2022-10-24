#ifndef NES_ZIPFIANDATAGENERATOR_HPP
#define NES_ZIPFIANDATAGENERATOR_HPP

#include <DataGeneration/DataGenerator.hpp>

namespace NES::Benchmark::DataGeneration {

    class ZipfianDataGenerator : public DataGenerator {

      public:
        explicit ZipfianDataGenerator(double alpha, uint64_t minValue, uint64_t maxValue);

      private:
        /**
         * @brief creates Zipfian data with the schema "id, value, payload, timestamp"
         * the id, payload, and timestamp are just counters that increment whereas the value gets drawn
         * randomly from a Zipfian distribution in the range [minValue, maxValue]
         * @param numberOfBuffers
         * @param bufferSize
         * @return success
         */
        std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;

        /**
         * @brief overrides the schema from the abstract parent class
         * @return schema from a DefaultDataGenerator
         */
        SchemaPtr getSchema() override;

        /**
         * @brief overrides the name from the abstract parent class
         * @return name
         */
        std::string getName() override;

        /**
         * @brief overrides the string representation of the parent class
         * @return
         */
        std::string toString() override;


      private:
        double alpha;
        uint64_t minValue;
        uint64_t maxValue;
    };
}
#endif//NES_ZIPFIANDATAGENERATOR_HPP

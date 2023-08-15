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
#ifndef NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMIN_HPP
#define NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMIN_HPP

#include <StatManager/StatCollectors/StatCollectorConfiguration.hpp>
#include <StatManager/StatCollectors/StatCollector.hpp>
#include <StatManager/StatCollectors/Synopses/Sketches/Sketches.hpp>
#include <StatManager/Util/Hashing.hpp>

namespace NES::Experimental::Statistics {

  class CountMin : public Sketch {

    public:

      /**
       * @brief gets the error of the sketch
       * @return mError
       */
      [[nodiscard]] double getError() const;

      /**
       * @brief gets the probability that the error exceeds the parametrized error
       * @return mProb
       */
      [[nodiscard]] double getProb() const;

      /**
       * @brief gets the sketch data
       * @return mDataPointer
       */
      uint32_t** getDataPointer();

      /**
       * @brief sets the pointer to the sketches data
       * @param DataPointer a pointer to a sketche
       */
      void setDataPointer(uint32_t** DataPointer);

      /**
       * @brief returns the H3 object of the CountMin sketch, which contains a number of hash functions equal to the
       * number of rows of the CountMin sketch
       * @return a pointer to an H3 object
       */
      H3* getClassOfHashFunctions();

      /**
       * @brief sets the H3 object of a CountMin sketch
       * @param ClassOfHashingFunctions an H3 object
       */
      void setClassOfHashFunctions(H3* ClassOfHashingFunctions);

      /**
       * @brief the constructor of a CountMin sketch
       * @param config a general purpose configuration object of type StatCollectorConfig, which contains information on
       * the type of StatCollector which is to be created, over which stream and field to construct the statCollector,
       * duration, frequency, and more
       */
      CountMin(const StatCollectorConfig& config);

      /**
       * @brief updates the CountMin sketch once per row using the key
       * @param key a key that is hashed once per row, updating exactly one counter of the CountMin sketch per row
       */
      void update(uint32_t key) override;

      /**
       * @brief checks whether two CountMin sketches are initialized equally (depth, width, and H3)
       * @param rightSketch a unique pointer to a StatCollector which is a sketch in this case.
       * @param statCollection a boolean which signifies if we want to two sketches to be equal for statistic collection.
       * In this case we also have to check that the duration and frequency are the same
       * @return true if the two CountMin sketches are equally initilized
       */
      bool equal(const std::unique_ptr<StatCollector>& rightSketch, bool statCollection) override;

      /**
       * takes two unique StatCollector pointers to sketches and returns a unique StatCollector pointer to another sketch,
       * which is the result of merging the two input sketches
       * @param rightSketch a second unique statCollector pointer to a CountMin sketch
       * @param statCollection a boolean wether this is being done for statisitic collection
       * @return a unique statCollector pointer to the merged result of the input
       */
      std::unique_ptr<StatCollector> merge(std::unique_ptr<StatCollector> rightSketch, bool statCollection) override;

      /**
       * @brief the function approximates how many times the CountMin Sketch observed the key
       * @param key the key whose frequency we want to approximate
       * @return an approximation of the frequency of the key
       */
      uint32_t pointQuery(uint32_t key);

    private:
      double mError;
      double mProb;
      uint32_t** mDataPointer;
      H3* mClassOfHashingFunctions;
    };

} // NES::Experimental::Statistics

#endif //NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMIN_HPP

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
#ifndef NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_HYPERLOGLOG_HPP
#define NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_HYPERLOGLOG_HPP

#include <StatManager/StatCollectors/StatCollectorConfiguration.hpp>
#include "StatManager/StatCollectors/StatCollector.hpp"
#include <StatManager/StatCollectors/Synopses/Sketches/Sketches.hpp>
#include <StatManager/Util/Hashing.hpp>

namespace NES::Experimental::Statistics {

  class HyperLogLog : public Sketch {

    using HyperLogLogPtr = std::unique_ptr<HyperLogLog>;

    public:
      [[nodiscard]] double getError() const;
      [[nodiscard]] uint16_t getB() const;
      void setAlphaM(double_t alpha_m);
      double_t getAlphaM() const;
      uint32_t getNumZeroBuckets() const;
      uint32_t* getDataPointer();
      void setDataPointer(uint32_t* DataPointer);
      H3* getClassOfHashFunctions();
      void setClassOfHashFunctions(H3* ClassOfHashingFunctions);
      HyperLogLog(const StatCollectorConfig& config);
      void update(uint32_t key) override;
      bool equal(const std::unique_ptr<StatCollector>& rightSketch, bool statCollection) override;
      std::unique_ptr<StatCollector> merge(std::unique_ptr<StatCollector>& rightSketch, bool statCollection) override;
      double_t countDistinctQuery();

    private:
      double_t mError;
      uint16_t b;                     // the number of bits dividing the hash as according to the paper
      double_t alphaM;                // the bias correction constant
      uint32_t numZeroBuckets;
      uint32_t* mDataPointer;
      H3* mClassOfHashingFunctions;
  };

} // NES::Experimental::Statistics

#endif //NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_HYPERLOGLOG_HPP

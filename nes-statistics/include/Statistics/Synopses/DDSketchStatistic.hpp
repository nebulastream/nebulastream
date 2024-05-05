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

#ifndef NES_NES_STATISTICS_INCLUDE_STATISTICS_SYNOPSES_DDSKETCHSTATISTIC_HPP_
#define NES_NES_STATISTICS_INCLUDE_STATISTICS_SYNOPSES_DDSKETCHSTATISTIC_HPP_

#include <Statistics/Synopses/SynopsesStatistic.hpp>
#include <fmt/ostream.h>
#include <map>
namespace NES::Statistic {

/**
 * @brief A statistic that is represented by a DD-Sketch. We take the implementation from the paper
 * "DD-Sketch: A fast and fully-mergeable quantile sketch with relative-error guarantees" by Charles Masson, Jee E. Rim, and Homin K. Lee.
 * (http://www.vldb.org/pvldb/vol12/p2195-masson.pdf)
 * For now, we do not perform any optimizations, such as the use of merging buckets together to reduce the memory requirements.
 * The authors state in the paper for αlpha = 0.01, a sketch of size 2048 can handle values from 80 microseconds to 1 year,
 * and cover all quantiles.
 */
class DDSketchStatistic : public SynopsesStatistic {
  public:
    using BucketPos = uint64_t;
    using LogFloorIndex = int64_t;
    struct Bucket {
        LogFloorIndex index = 0;
        uint64_t counter = 0;
        bool operator==(const Bucket& other) const { return index == other.index && counter == other.counter; }
        bool operator<(const Bucket& other) const { return index < other.index; }
        friend std::ostream& operator<<(std::ostream& os, const Bucket& bucket) {
            return os << fmt::format("(index: {}, counter: {})", bucket.index, bucket.counter);
        }
    };

    /**
     * @brief Factory method for creating a CountMinStatistic
     * @param startTs: Timestamp of the first tuple for that this CountMin was created
     * @param endTs: Timestamp of the last tuple for that this CountMin was created
     * @param observedTuples: Number of tuples seen during the creation
     * @param numberOfBuckets: Number of buckets that should be used for the DD-Sketch
     * @param gamma: The relative accuracy parameter of the DD-Sketch. The smaller the gamma, the more accurate the sketch is.
     * @param ddSketchDataString: String that stores the DD-Sketch data, so the underlying vector of Buckets
     * @return StatisticPtr
     */
    static StatisticPtr create(const Windowing::TimeMeasure& startTs,
                               const Windowing::TimeMeasure& endTs,
                               uint64_t observedTuples,
                               uint64_t numberOfBuckets,
                               double gamma,
                               const std::string_view ddSketchDataString);

    /**
     * @brief Creates a DD-Sketch for the given startTs, endTs, and numberOfBuckets. The #observedTuples and all buckets are set to 0
     * @param startTs
     * @param endTs
     * @param numberOfPreAllocatedBuckets
     * @param gamma
     * @return StatisticPtr
     */
    static StatisticPtr createInit(const Windowing::TimeMeasure& startTs,
                                   const Windowing::TimeMeasure& endTs,
                                   uint64_t numberOfPreAllocatedBuckets,
                                   double gamma);


    /**
     * @brief Adds a value to the buckets. If needed, this method will merge buckets to keep it under the
     * allowed number of buckets.
     * @param logFloorIndex: The integer value, after calling floor(log2(abs(value))), which is the index of the bucket
     * If the value is negative, meaning that it should be added to a negative bucket.
     * IMPORTANT: To allow us to accommodate zero values, we expect the index to be either +1 or -1, depending if it is a
     * positive or negative value. The zero values are stored in the middle of the buckets.
     * @param weight: How much the bucket should be increased. Default is 1.
     */
    void addValue(LogFloorIndex logFloorIndex, uint64_t weight = 1);

    void* getStatisticData() const override;
    bool equal(const Statistic& other) const override;
    std::string toString() const override;
    void merge(const SynopsesStatistic& other) override;
    std::string getDDSketchDataAsString() const;
    uint64_t getNumberOfBuckets() const;
    double getGamma() const;
    double getValueAtQuantile(double quantile) const;

  private:
    DDSketchStatistic(const Windowing::TimeMeasure& startTs,
                      const Windowing::TimeMeasure& endTs,
                      uint64_t observedTuples,
                      double gamma,
                      const std::vector<Bucket>& buckets);

    /* Stores all three buckets (negative, zero values, and positive) in one vector. The first part is for the negative values,
     * the second part for the zero values, and the third part for the positive values. The positive and negative parts
     * have the size numberOfBuckets. So to calculate the position of the zero value = buckets[numberOfBuckets]
     */
    std::vector<Bucket> buckets;
    std::map<LogFloorIndex, BucketPos> indexToBucketPos;
    double gamma;
};
} // namespace NES::Statistic
#endif//NES_NES_STATISTICS_INCLUDE_STATISTICS_SYNOPSES_DDSKETCHSTATISTIC_HPP_

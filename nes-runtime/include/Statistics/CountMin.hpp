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

#ifndef NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_COUNTMIN_HPP_
#define NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_COUNTMIN_HPP_

#include <vector>
#include <Statistics/Statistic.hpp>

namespace NES::Experimental::Statistics {

/**
 * @brief this class stores the 2D array of a CountMin sketches,
 * its meta data and provides the functionalities associated with Count-Min Sketches.
 */
class CountMin : public Statistic {
  public:
    /**
     * @param width the width of the sketch
     * @param data the data of the sketch
     * @param statisticCollectorIdentifier the key of the sketch
     * @param observedTuples the number of observedTuples of the sketch
     * @param depth the depth of the sketch
     */
    CountMin(uint64_t width,
             const std::vector<uint64_t>& data,
             StatisticCollectorIdentifierPtr statisticCollectorIdentifier,
             uint64_t observedTuples,
             uint64_t depth);

    /**
     * @brief calculates the error that the Count-Min will most often guarantee
     * @param width the width of the Count-Min sketch
     * @return the error
     */
    static double calcError(uint64_t width);

    /**
     * @brief calculates the probability, that the actual error exceeds the parametrized error, given the depth
     * @param depth the depth of the Count-Min sketch
     * @return the probability
     */
    static double calcProbability(uint64_t depth);

    /**
     * @brief creates the seeds for CountMin
     * @param depth the depth of the countMin sketch. This specifies that seeds are generated such that we have one independent hash function per row
     * @param seed the seed with which the random data is generated
     * @return a vector of seeds
     */
    static std::vector<uint64_t> createSeeds(uint64_t depth, uint64_t seed = 42);

    /**
     * @brief hashes a key
     * @param key the key that is to be hashed
     * @param row the row or hash function with which we wish to hash
     * @return a random hash value (that still needs to be taken modulo to be in a specific range)
     */
    uint64_t H3Hash(uint64_t key, uint64_t row);

    /**
     * @brief returns an approximation to an equal expression (e.g. x == 15) in absolute numbers
     * @param key the key (15 in the above example)
     * @return the approximation of how many 15 values were seen
     */
    double pointQuery(uint64_t key);

    /**
     * @brief returns an approximation to a range expression (e.g. x < 15) in absolute numbers (also <=, >, >=)
     * @param key the key (15 in the above example)
     * @return the approximation of how many values smaller than 15 were seen
     */
    double rangeQuery(uint64_t key);

    /**
     * @brief calculates the result to a point or range query
     * @param probeParameters a parameter object which defines what is to be probed from the count min sketch
     * @return a statistic
     */
    double probe(StatisticProbeParameterPtr &probeParameters) override;

    /**
     * @return returns the width of the Count-Min Sketch
     */
    [[nodiscard]] uint64_t getWidth() const;

    /**
     * @return returns the error of the Count-Min Sketch
     */
    [[nodiscard]] double getError() const;

    /**
     * @return returns the probability of the Count-Min Sketch
     */
    [[nodiscard]] double getProbability() const;

    /**
     * @return the sketch data as a vector of vectors
     */
    [[nodiscard]] const std::vector<uint64_t>& getData() const;

    /**
     * @brief given a std::string that encodes the array of a Count-Min sketch, a depth, and a width, this function creates a basic
     * CountMin object with information about the (data) origin (logicalSourceName, physicalSourceName, etc.) omitted
     * @param cmString the string encoding the array of the sketch
     * @param depth the depth of the sketch
     * @param width the width of the sketch
     * @return a basic CountMin object, where not all fields are initilized
     */
    static CountMin createFromString(void* cmString, uint64_t depth, uint64_t width);

    /**
     * @brief increments the counter a 1D array that represents a flattened 2D CountMin Sketch
     * @param data the pointer to the flattened array
     * @param rowId the row in which to increase the counter
     * @param width the width of the sketch
     * @param columnId the column in which to increase the counter
     */
    static void incrementCounter(uint64_t* data, uint64_t rowId, uint64_t width, uint64_t columnId);

  private:
    uint64_t width;
    double error;
    double probability;
    std::vector<uint64_t> data;
    std::vector<uint64_t> seeds;
};
}// namespace NES::Experimental::Statistics
#endif//NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_COUNTMIN_HPP_

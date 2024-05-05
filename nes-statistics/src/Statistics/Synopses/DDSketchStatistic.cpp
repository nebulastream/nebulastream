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

#include <Statistics/Synopses/DDSketchStatistic.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Util/Common.hpp>
#include <string>

namespace NES::Statistic {

void DDSketchStatistic::addValue(LogFloorIndex logFloorIndex, uint64_t weight) {

    // Calculate the index of the bucket
    auto it = indexToBucketPos.find(logFloorIndex);
    if (it == indexToBucketPos.end()) {
        buckets.emplace_back(Bucket(0, 0));
        indexToBucketPos[logFloorIndex] = buckets.size() - 1;
    }
    const auto index = indexToBucketPos[logFloorIndex];

    // Increment the counter of the bucket and updating the index
    buckets[index].counter += weight;
    buckets[index].index = logFloorIndex;

    // Increment the observed tuples
    observedTuples += weight;
}

void* DDSketchStatistic::getStatisticData() const { return const_cast<Bucket*>(buckets.data());}

bool DDSketchStatistic::equal(const Statistic& other) const {
    if (other.instanceOf<DDSketchStatistic>()) {
        auto otherDDSketchStatistic = other.as<const DDSketchStatistic>();
        return startTs.equals(otherDDSketchStatistic->startTs) && endTs.equals(otherDDSketchStatistic->endTs) &&
            observedTuples == otherDDSketchStatistic->observedTuples &&
            Util::containsSameItems(buckets.begin(), buckets.end(), otherDDSketchStatistic->buckets.begin(), otherDDSketchStatistic->buckets.end());

    }
    return false;
}

std::string DDSketchStatistic::toString() const {
    std::ostringstream oss;
    oss << "DDSketch(";
    oss << "startTs: " << startTs.toString() << " ";
    oss << "endTs: " << endTs.toString() << " ";
    oss << "observedTuples: " << observedTuples << " ";
    oss << "gamma: " << gamma << " ";

    // Creating a string representation of the buckets in a sorted way
    auto tmpBuckets = buckets;
    std::sort(tmpBuckets.begin(), tmpBuckets.end());
    oss << std::endl << "buckets: [";
    for (const auto& bucket : tmpBuckets) {
        oss << bucket << std::endl;
    }
    oss << "]" << std::endl;
    oss << ")";
    return oss.str();
}

void DDSketchStatistic::merge(const SynopsesStatistic& other) {
    // 1. We can only merge the same synopsis type
    if (!other.instanceOf<DDSketchStatistic>()) {
        NES_ERROR("Other is not of type CountMinStatistic. Therefore, we skipped merging them together");
        return;
    }

    // 2. We can only merge statistics with the same period
    if (!startTs.equals(other.getStartTs()) || !endTs.equals(other.getEndTs())) {
        NES_ERROR("Can not merge HyperLogLog statistics with different periods");
        return;
    }
    const auto otherDDSKetchStatistic = other.as<const DDSketchStatistic>();

    // 3. We merge by adding each counter of each bucket
    for (auto [index, bucketPos] : otherDDSKetchStatistic->indexToBucketPos) {
        addValue(index, otherDDSKetchStatistic->buckets[bucketPos].counter);
    }

    // 5. Increment the observedTuples
    // We not have to do this, as we increment the observed tuples in the addValue method
}

std::string DDSketchStatistic::getDDSketchDataAsString() const {
    auto bucketsCopy = buckets;
    std::sort(bucketsCopy.begin(), bucketsCopy.end());
    const auto dataSizeInBytes = bucketsCopy.size() * sizeof(Bucket);
    std::string dataAsString(dataSizeInBytes, 0);
    std::memcpy(dataAsString.data(), bucketsCopy.data(), dataSizeInBytes);
    return dataAsString;
}

uint64_t DDSketchStatistic::getNumberOfBuckets() const { return buckets.size(); }
double DDSketchStatistic::getGamma() const {return gamma; }
double DDSketchStatistic::getValueAtQuantile(double quantile) const {
    if (quantile < 0) {
        NES_THROW_RUNTIME_ERROR("Currently, we do not support negative quantiles!");
    }
    NES_ASSERT(quantile <= 1, "Quantile has to be between 0 and 1");

    // We calculate the rank of the quantile
    const auto rank = quantile * (observedTuples - 1);
    NES_INFO("Rank: {}", rank);

    // We sort all buckets by their index
    auto bucketsCopy = buckets;
    std::sort(bucketsCopy.begin(), bucketsCopy.end());

    // We iterate over the buckets and calculate the rank
    auto count = 0.0;
    for (const auto& bucket : bucketsCopy) {
        if (bucket.index < 0) {
            continue;
        }
        if (count < rank) {
            return 2 * std::pow(gamma, bucket.index) / (gamma + 1);
        }
        count += bucket.counter;
    }

    // For now, we simply return 0.0 if we did not find a value
    return 0.0;
}

StatisticPtr DDSketchStatistic::create(const Windowing::TimeMeasure& startTs,
                                       const Windowing::TimeMeasure& endTs,
                                       uint64_t observedTuples,
                                       uint64_t numberOfBuckets,
                                       double gamma,
                                       const std::string_view ddSketchDataString) {
    // We just store the data as a string. So we have to copy the bytes from the string to a vector
    // We do this by first creating a large enough vector and then copy the bytes
    std::vector<Bucket> buckets(numberOfBuckets);
    std::memcpy(buckets.data(), ddSketchDataString.data(), ddSketchDataString.size());
    return std::make_shared<DDSketchStatistic>(DDSketchStatistic(startTs, endTs, observedTuples, gamma, buckets));
}

StatisticPtr DDSketchStatistic::createInit(const Windowing::TimeMeasure& startTs,
                                           const Windowing::TimeMeasure& endTs,
                                           uint64_t numberOfPreAllocatedBuckets,
                                           double gamma) {
    std::vector<Bucket> buckets;
//    buckets.reserve(numberOfPreAllocatedBuckets);
    ((void) numberOfPreAllocatedBuckets);
    constexpr auto observedTuples = 0;
    return std::make_shared<DDSketchStatistic>(DDSketchStatistic(startTs, endTs, observedTuples, gamma, buckets));
}

DDSketchStatistic::DDSketchStatistic(const Windowing::TimeMeasure& startTs,
                                     const Windowing::TimeMeasure& endTs,
                                     uint64_t observedTuples,
                                     double gamma,
                                     const std::vector<Bucket>& buckets)
    : SynopsesStatistic(startTs, endTs, observedTuples), buckets(buckets), gamma(gamma) {}

} // namespace NES::Statistic
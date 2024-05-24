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

#include <Statistics/Synopses/EquiWidthHistogramStatistic.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <memory>
#include <string>

namespace NES::Statistic {

StatisticPtr EquiWidthHistogramStatistic::create(const Windowing::TimeMeasure& startTs,
                                                 const Windowing::TimeMeasure& endTs,
                                                 uint64_t observedTuples,
                                                 uint64_t binWidth,
                                                 const std::string_view equiWidthDataString,
                                                 const uint64_t numberOfBins) {
    // We just store the EquiWidth data as a string. So we have to copy the bytes from the string to a vector
    // We do this by first creating a large enough vector and then copy the bytes
    std::vector<Bin> equiWidthData(numberOfBins, Bin());
    std::memcpy(equiWidthData.data(), equiWidthDataString.data(), sizeof(Bin) * numberOfBins);
    return std::make_shared<EquiWidthHistogramStatistic>(EquiWidthHistogramStatistic(startTs, endTs, observedTuples, binWidth, equiWidthData));
}

StatisticPtr EquiWidthHistogramStatistic::createInit(const Windowing::TimeMeasure& startTs,
                                                     const Windowing::TimeMeasure& endTs,
                                                     uint64_t binWidth) {
    constexpr auto observedTuples = 0;
    return std::make_shared<EquiWidthHistogramStatistic>(EquiWidthHistogramStatistic(startTs, endTs, observedTuples, binWidth, std::vector<Bin>{}));
}

void EquiWidthHistogramStatistic::update(uint64_t lowerBound, uint64_t upperBound, uint64_t increment) {
    // First, we find the bin belonging to the lowerBound and upperBound
    auto bin = std::find_if(equiWidthData.begin(), equiWidthData.end(), [lowerBound, upperBound](const Bin& bin) {
        return bin.lowerBound == lowerBound && bin.upperBound == upperBound;
    });

    if (bin != equiWidthData.end()) {
        // If the bin exists, we increment the count
        bin->count += increment;
    } else {
        // If the bin does not exist, we create a new bin
        equiWidthData.emplace_back(Bin(lowerBound, upperBound, increment));
    }
}

void* EquiWidthHistogramStatistic::getStatisticData() const { return nullptr; }

bool EquiWidthHistogramStatistic::equal(const Statistic& other) const {
    if (other.instanceOf<EquiWidthHistogramStatistic>()) {
        auto otherCountMinStatistic = other.as<const EquiWidthHistogramStatistic>();
        return startTs.equals(otherCountMinStatistic->startTs) && endTs.equals(otherCountMinStatistic->endTs) &&
            observedTuples == otherCountMinStatistic->observedTuples && binWidth == otherCountMinStatistic->binWidth &&
            equiWidthData == otherCountMinStatistic->equiWidthData;
    }
    return false;
}

std::string EquiWidthHistogramStatistic::toString() const { return std::string(); }

void EquiWidthHistogramStatistic::merge(const SynopsesStatistic& other) {
    // 1. We can only merge the same synopsis type
    if (!other.instanceOf<EquiWidthHistogramStatistic>()) {
        NES_ERROR("Other is not of type EquiWidthHistogramStatistic. Therefore, we skipped merging them together");
        return;
    }

    // 2. We can only merge statistics with the same period
    if (!startTs.equals(other.getStartTs()) || !endTs.equals(other.getEndTs())) {
        NES_ERROR("Can not merge EquiWidthHistogramStatistic statistics with different periods");
        return;
    }

    // 3. We can only merge an equi width histogram with the same width
    auto otherEquiWidthHist = other.as<const EquiWidthHistogramStatistic>();
    if (binWidth != otherEquiWidthHist->binWidth) {
        NES_ERROR("Can not combine sketches <{}> and <{}>, as they do not share the same dimensions",
                  binWidth,
                  otherEquiWidthHist->binWidth);
        return;
    }

    // 4. We merge two histograms by simply adding the counts of the bins
    for (const auto& bin : otherEquiWidthHist->equiWidthData) {
        update(bin.lowerBound, bin.upperBound, bin.count);
    }

    // 5. Increment the observedTuples
    observedTuples += otherEquiWidthHist->observedTuples;
}

std::string EquiWidthHistogramStatistic::getEquiWidthHistDataAsString() const {
    const auto dataSizeBytes = equiWidthData.size() * sizeof(Bin);
    std::string equiWidthHistStr;
    equiWidthHistStr.resize(dataSizeBytes);
    std::memcpy(equiWidthHistStr.data(), equiWidthData.data(), dataSizeBytes);
    return equiWidthHistStr;

}

uint64_t EquiWidthHistogramStatistic::getCountForValue(uint64_t value) const {
    // This is not the most optimal and most efficient, but good enough for now
    const auto bin = std::find_if(equiWidthData.begin(), equiWidthData.end(), [value](const Bin& bin) {
        return bin.lowerBound <= value && bin.upperBound >= value;
    });

    return bin != equiWidthData.end() ? bin->count : 0;
}

uint64_t EquiWidthHistogramStatistic::getBinWidth() const { return binWidth; }

uint64_t EquiWidthHistogramStatistic::getNumberOfBins() const { return equiWidthData.size(); }

EquiWidthHistogramStatistic::EquiWidthHistogramStatistic(const Windowing::TimeMeasure& startTs,
                                                         const Windowing::TimeMeasure& endTs,
                                                         const uint64_t observedTuples,
                                                         const uint64_t binWidth,
                                                         const std::vector<Bin>& equiWidthData)
: SynopsesStatistic(startTs, endTs, observedTuples), binWidth(binWidth), equiWidthData(equiWidthData) {}

} // namespace NES::Statistic
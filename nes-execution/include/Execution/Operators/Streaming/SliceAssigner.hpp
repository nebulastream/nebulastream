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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_SLICEASSIGNER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_SLICEASSIGNER_HPP_

#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <cinttypes>
#include <limits>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * A window having size=A and slide=B will need edges of slices each time a window starts or ends. The start time
 * (when beginning at 0) will be every increment of the slide, i.e., at x * B the x-th window starts and the end time will need
 * the size A added to it, i.e., at x * B + A the x-th window ends. If the query with this join was not deployed at time=0,
 * but at some later deploymentTime=C this means that C needs to be added to both formulas of slice edges.
 * We calculate slice edges for a new incoming tuple at time=ts. And the formula to find the slice start will calculate the
 * times that the maximal int x would produce for a window start and a different window's end that are both smaller than the ts.
 * I.e., (x1 * B + C) <= ts and (x2 * B + A + C) <= ts, from these values we take the max.
 * For the slice end the calculation reversely calculates the times that the smallest x would produce for a window start and a
 * different windows end that is bigger than the ts. I.e., (x1 * B + C) > ts and (x2 * B + A + C) > ts, we take the min of both.
 * The actual calculations use the modulo operation. This works in the following way.
 * 1. last window that started before the ts: C + x * B <= ts | maximize x ==>
 * (ts - C) % B returns the leftover part of dividing by the biggest divisor that is a whole number ((ts - C) / B = x (rounded down) + leftover
 * ==> x * B + C start of the window, but ts is leftover part bigger than start of the window ==> ts - leftover == ts - ((ts - C) % B)
 * 2. last window that ended before the ts: C + x * B + A <= ts | maximize x ==> rest similar to idea for lastWindowStartBeforeTs
 * 3. first window that started after the ts: 1. plus sliding the window once more
 * 4. first window that ended after the ts: 2. plus sliding the window once more
 * @brief The SliceAssigner assigner determines the start and end time stamp of a slice for a specific window definition,
 * that consists of a window size and a window slide.
 * @example Two windows with size=10 and slide=3. first window got deployed at t=0 and second at t=2 now a tuples comes with ts=14.
 * for the start we want the max x (each formula will have d different x) that holds true for both formulas for both deploymentTimes.
 * so we would get: 4*3+0=12; 1*3+10+0=13; 4*3+2=14; 0*3+10+2=12 => max is 14 so the slice starts at 14.
 * @note Tumbling windows are in general modeled at this point as sliding windows with the size is equals to the slide.
 */
class SliceAssigner {
  public:
    explicit SliceAssigner(uint64_t windowSize, uint64_t windowSlide, std::vector<uint64_t> deploymentTimes);

    /**
     * @brief Calculates the start of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the start of the particular slice.
     * @return uint64_t slice start
     */
    [[nodiscard]] inline uint64_t getSliceStartTs(uint64_t ts) const {
        auto thisSliceStart = std::numeric_limits<uint64_t>::min();//will be 0 for uint I guess
        auto belongsToAnySlice = false;

        for (auto windowDeploymentTime : windowsDeploymentTime) {
            if (ts < windowDeploymentTime) [[unlikely]] {
                continue;//this definition of the window can not be the start of the slice as it has it hast been deployed at a time > than the ts of the tuple
            }
            belongsToAnySlice = true;
            auto lastWindowStartBeforeTs = ts - ((ts - windowDeploymentTime) % windowSlide);
            if (ts < windowDeploymentTime
                    + windowSize) {//no window has ended yet. We can avoid unnecessary extra slices that would be defined by the ends of windows that started before time 0
                thisSliceStart = std::max(lastWindowStartBeforeTs, thisSliceStart);
            } else {
                auto lastWindowEndBeforeTs = ts - ((ts - windowSize - windowDeploymentTime) % windowSlide);
                thisSliceStart = std::max(thisSliceStart, std::max(lastWindowStartBeforeTs, lastWindowEndBeforeTs));
            }
        }
        // we should skip this tuple. not sure what return. This would happen if query gets deployed at time x, while all other queries are deleted at the same time or have a deployment time > x, and a tuple would arrive with a ts < x.
        if (!belongsToAnySlice) {
            NES_ERROR("tuple was sent through system but does not belong to any window");
            return -1;
        }
        return thisSliceStart;
    }

    /**
     * @brief Calculates the end of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the end of the particular slice.
     * @return uint64_t slice end
     */
    [[nodiscard]] inline uint64_t getSliceEndTs(uint64_t ts) const {
        auto thisSliceEnd = std::numeric_limits<uint64_t>::max();
        ;
        for (auto windowDeploymentTime : windowsDeploymentTime) {
            if (ts < windowDeploymentTime) {
                thisSliceEnd =
                    std::min(thisSliceEnd,
                             windowDeploymentTime);// this would be the nextWindowStartAfterTs, but the calculations would fail
                continue;// next window that ends is deploymentTime + size which cant be smaller than just deploymentTime
            }
            auto nextWindowStartAfterTs = ts - ((ts - windowDeploymentTime) % windowSlide) + windowSlide;
            uint64_t nextWindowEndAfterTs;
            if (ts < windowDeploymentTime + windowSize) {
                nextWindowEndAfterTs =
                    windowDeploymentTime + windowSize;//this would be nextWindowEndAfterTs, but the calculations would fail
            } else {
                nextWindowEndAfterTs = ts - ((ts - windowDeploymentTime - windowSize) % windowSlide) + windowSlide;
            }
            thisSliceEnd = std::min(thisSliceEnd, std::min(nextWindowEndAfterTs, nextWindowStartAfterTs));
        }
        return thisSliceEnd;
    }

    /**
     * @brief Getter for the window size
     * @return window size in uint64_t
     */
    uint64_t getWindowSize() const { return windowSize; }

    /**
     * @brief Getter for the window slide
     * @return window slide in uint64_t
     */
    uint64_t getWindowSlide() const { return windowSlide; }

    void addWindowDeploymentTime(uint64_t deploymentTime);

    void removeWindowDeploymentTime(uint64_t deploymentTime);

  private:
    const uint64_t windowSize;
    const uint64_t windowSlide;
    std::vector<uint64_t> windowsDeploymentTime;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_SLICEASSIGNER_HPP_

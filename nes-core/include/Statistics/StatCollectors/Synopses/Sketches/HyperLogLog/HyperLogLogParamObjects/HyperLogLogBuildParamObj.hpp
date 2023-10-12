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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_HYPERLOGLOG_HYPERLOGLOGBUILDPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_HYPERLOGLOG_HYPERLOGLOGBUILDPARAMOBJ_HPP_

#include <string>
#include <ctime>
#include <cmath>
#include "Statistics/StatCollectors/Synopses/Sketches/SketchParamObjects/SketchBuildParamObj.hpp"
#include "Configurations/Coordinator/CoordinatorConfiguration.hpp"

namespace NES {

	namespace Experimental::Statistics {

		class HyperLogLogBuildParamObj : public SketchBuildParamObj {

		public:
			HyperLogLogBuildParamObj(const std::string& logicalSourceName,
															 const std::string& physicalSourceName,
															 const std::string& fieldName,
															 const double error = Configurations::CoordinatorConfiguration::createDefault()->synopsisError,
															 const time_t windowSize = Configurations::CoordinatorConfiguration::createDefault()->synopsisWindowSize)
					: SketchBuildParamObj(logicalSourceName, physicalSourceName, fieldName, 1, calculateRegisters(error), windowSize),
					  error(error), b(log2(calculateRegisters(error))), alphaM(calculateAlphaM(calculateRegisters(error))) {}

			/**
			 *
			 * @param x the number which is to be rounded to the next power of two
			 * @return x if x is already a power of two, otherwise x rounded up to the next highest power of two
			 */
			uint32_t nextPowerOfTwo(uint32_t x) {
				uint32_t powerOfTwo = 1;
				while (powerOfTwo < x) {
					powerOfTwo <<= 1;
				}
				return powerOfTwo;
			}

			/**
			 *
			 * @param fError the maximum allowed error of the HLL sketch
			 * @return returns the width/number of registers to initialize the HLL sketch with
			 */
			uint32_t calculateRegisters(double fError) {
				uint32_t m = ceil(1 / pow(fError, 2));
				return nextPowerOfTwo(m);
			}

			/**
			 *
			 * @param m the width/number of register of the HLL sketch
			 * @return return the bias correction constant alphaM
			 */
			double calculateAlphaM(uint32_t m) {
				if (m <= 16) {
					return 0.673;
				} else if (m == 32) {
					return 0.697;
				} else if (m == 64) {
					return 0.709;
				} else {
					return 0.7213 / (1 + 0.079 / m);
				}
			}

			double getError() const {
				return error;
			}

			uint32_t getB() const {
				return b;
			}

			double getAlphaM() const {
				return alphaM;
			}

		private:
			const double error;
			const uint32_t b;
			const double alphaM;
		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_HYPERLOGLOG_HYPERLOGLOGBUILDPARAMOBJ_HPP_

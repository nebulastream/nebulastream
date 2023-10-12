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

#ifndef NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMINPARAMOBJ_HPP
#define NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMINPARAMOBJ_HPP

#include <string>
#include <ctime>
#include <cmath>
#include <Statistics//StatCollectors/Synopses/Sketches/SketchParamObjects/SketchBuildParamObj.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>


namespace NES {

	namespace Experimental::Statistics {

		/**
		 * The derived class whose objects contain all the necessary information to generate actual Count-Min sketches
		 */
		class CountMinBuildParamObj : public SketchBuildParamObj {

		public:
			CountMinBuildParamObj(const std::string& logicalSourceName,
														const std::string& physicalSourceName,
														const std::string& fieldName,
														const double error = Configurations::CoordinatorConfiguration::createDefault()->synopsisError,
														const double prob = Configurations::CoordinatorConfiguration::createDefault()->synopsisProb,
														const time_t windowSize = Configurations::CoordinatorConfiguration::createDefault()->synopsisWindowSize)
					: SketchBuildParamObj(logicalSourceName, physicalSourceName, fieldName, ceil(exp(1.0) / error), ceil(std::log(1.0 / prob)), windowSize),
					  error(error), prob(prob) {}

			/**
			 * @return returns the error of Count-Min sketch that is to be generated
			 */
			double getError() const {
				return error;
			}

			/**
			 * @return returns the probability of the Count-Min sketch that is to be generated
			 */
			double getProb() const {
				return prob;
			}

		private:
			const double error;
			const double prob;
		};

	}
}

#endif //NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMINPARAMOBJ_HPP

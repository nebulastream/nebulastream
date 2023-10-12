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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMINPARAMOBJECTS_COUNTMINPROBEPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMINPARAMOBJECTS_COUNTMINPROBEPARAMOBJ_HPP_

#include <string>
#include <ctime>
#include <cmath>
#include <Statistics/StatCollectors/Synopses/Sketches/SketchParamObjects/SketchProbeParamObj.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>

namespace NES {

	namespace Experimental::Statistics {

		class CountMinProbeParamObj : public SketchProbeParamObj {

		public:
			CountMinProbeParamObj(const std::string& logicalSourceName,
														const std::string& physicalSourceName,
					                  const std::string& fieldName,
													  const std::string& statCollectorType,
													  const std::string& expression,
					                  const double error = Configurations::CoordinatorConfiguration::createDefault()->synopsisError,
					                  const double prob = Configurations::CoordinatorConfiguration::createDefault()->synopsisProb,
					                  const time_t windowSize = Configurations::CoordinatorConfiguration::createDefault()->synopsisWindowSize)
					: SketchProbeParamObj(logicalSourceName, physicalSourceName, fieldName, statCollectorType, expression, ceil(exp(1.0) / error), ceil(std::log(1.0 / prob)), windowSize),
					  error(error), prob(prob) {}

			double getError() const {
				return error;
			}

			double getProb() const {
				return prob;
			}

		private:
			const double error;
			const double prob;
		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMINPARAMOBJECTS_COUNTMINPROBEPARAMOBJ_HPP_

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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_DDSKETCH_DDSKETCHPARAMOBJECTS_DDSKETCHBUILDPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_DDSKETCH_DDSKETCHPARAMOBJECTS_DDSKETCHBUILDPARAMOBJ_HPP_

#include <string>
#include <ctime>
#include "Statistics/StatCollectors/Synopses/Sketches/SketchParamObjects/SketchBuildParamObj.hpp"
#include "Configurations/Coordinator/CoordinatorConfiguration.hpp"

namespace NES {

	namespace Experimental::Statistics {

		class DDSketchBuildParamObj : public SketchBuildParamObj {

		public:
			DDSketchBuildParamObj(const std::string& logicalSourceName,
			                       const std::string& physicalSourceName,
			                       const std::string& fieldName,
			                       const double error = Configurations::CoordinatorConfiguration::createDefault()->synopsisError,
			                       const time_t windowSize = Configurations::CoordinatorConfiguration::createDefault()->synopsisWindowSize)
					: SketchBuildParamObj(logicalSourceName, physicalSourceName, fieldName,
					                       1, Configurations::CoordinatorConfiguration::createDefault()->synopsisWindowSize, windowSize),
					  error(error), gamma((1 + error) / (1 - error)) {}

			double getError() const {
				return error;
			}

			double getGamma() const {
				return gamma;
			}

		private:
			const double error;
			const double gamma;
		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_DDSKETCH_DDSKETCHPARAMOBJECTS_DDSKETCHBUILDPARAMOBJ_HPP_

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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SAMPLES_RESERVOIRSAMPLE_RESERVOIRSAMPLEBUILDPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SAMPLES_RESERVOIRSAMPLE_RESERVOIRSAMPLEBUILDPARAMOBJ_HPP_

#include <string>
#include <ctime>
#include "Statistics/StatCollectors/StatCollectorParamObjects/StatCollectorBuildParamObj.hpp"
#include "Configurations/Coordinator/CoordinatorConfiguration.hpp"


namespace NES {

	namespace Experimental::Statistics {

		/**
		 * The derived class, which is used to create parameter objects from which Reservoir Samples can be generated
		 */
		class ReservoirSampleBuildParamObj : public StatCollectorBuildParamObj {

		public:
			ReservoirSampleBuildParamObj(const std::string& logicalSourceName,
			                      const std::string& physicalSourceName,
			                      const std::string& fieldName,
														const uint32_t width = Configurations::CoordinatorConfiguration::createDefault()->synopsisWidth,
			                      const time_t windowSize = Configurations::CoordinatorConfiguration::createDefault()->synopsisWindowSize)
					: StatCollectorBuildParamObj(logicalSourceName, physicalSourceName, fieldName, windowSize),
					  width(width) {}

			/**
			 * @return returns the size of the Reservoir Sample that is to be constructed
			 */
			double getWidth() const {
				return width;
			}

		private:
			const uint32_t width;
		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SAMPLES_RESERVOIRSAMPLE_RESERVOIRSAMPLEBUILDPARAMOBJ_HPP_

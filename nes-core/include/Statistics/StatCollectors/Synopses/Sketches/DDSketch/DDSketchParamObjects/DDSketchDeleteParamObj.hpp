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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_DDSKETCH_DDSKETCHDELETEPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_DDSKETCH_DDSKETCHDELETEPARAMOBJ_HPP_

#include <string>
#include <ctime>
#include "Statistics/StatCollectors/Synopses/Sketches/SketchParamObjects/SketchDeleteParamObj.hpp"
#include "Configurations/Coordinator/CoordinatorConfiguration.hpp"

namespace NES {

	namespace Experimental::Statistics {

		/**
		 * The derived class that generated objects that can be used to delete DDSketches
		 */
		class DDSketchDeleteParamObj : public SketchDeleteParamObj {

		public:
			DDSketchDeleteParamObj(const std::string& logicalSourceName,
														 const std::string& physicalSourceName,
														 const std::string& fieldName,
														 const std::string& statCollectorType,
														 const double error = Configurations::CoordinatorConfiguration::createDefault()->synopsisError,
														 const time_t windowSize = Configurations::CoordinatorConfiguration::createDefault()->synopsisWindowSize)
					: SketchDeleteParamObj(logicalSourceName, physicalSourceName, fieldName, statCollectorType,
					                      1, Configurations::CoordinatorConfiguration::createDefault()->synopsisWindowSize, windowSize),
					  error(error), gamma((1 + error) / (1 - error)) {}

			/**
			 * @return returns the error of the DDSketch that is to be deleted with this object
			 */
			double getError() const {
				return error;
			}

			/**
			 * @return returns the gamma of the DDSketch that is to be deleted with this object
			 */
			double getGamma() const {
				return gamma;
			}

		private:
			const double error;
			const double gamma;
		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_DDSKETCH_DDSKETCHDELETEPARAMOBJ_HPP_

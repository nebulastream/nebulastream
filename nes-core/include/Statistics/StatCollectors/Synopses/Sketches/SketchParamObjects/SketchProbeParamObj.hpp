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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_SKETCHPARAMOBJECTS_SKETCHPROBEPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_SKETCHPARAMOBJECTS_SKETCHPROBEPARAMOBJ_HPP_

#include <string>
#include <ctime>
#include <Statistics/StatCollectors/StatCollectorParamObjects/StatCollectorProbeParamObj.hpp>

namespace NES {

	namespace Experimental::Statistics {

		/**
		 * The intermediate class from which all sketchProbeParamObjs classes inherit
		 */
		class SketchProbeParamObj : public StatCollectorProbeParamObj {

		public:
			SketchProbeParamObj(const std::string& logicalSourceName,
			                    const std::string& physicalSourceName,
			                    const std::string& fieldName,
													const std::string& statCollectorType,
													const std::string& expression,
			                    const uint32_t depth,
			                    const uint32_t width,
			                    const time_t windowSize)
					: StatCollectorProbeParamObj(logicalSourceName, physicalSourceName, fieldName, statCollectorType, expression, windowSize),
					  depth(depth), width(width) {}

			/**
			 * @return returns the depth of the sketch that is to be queried/probed
			 */
			uint32_t getDepth() const {
				return depth;
			}

			/**
			 * @return returns the width of the sketch that is to be queried/probed
			 */
			uint32_t getWidth() const {
				return width;
			}

		private:
			const uint32_t depth;
			const uint32_t width;
		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_SKETCHPARAMOBJECTS_SKETCHPROBEPARAMOBJ_HPP_

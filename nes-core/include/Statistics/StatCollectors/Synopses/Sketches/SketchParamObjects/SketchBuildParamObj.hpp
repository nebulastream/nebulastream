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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_SKETCHPARAMOBJECTS_SKETCHBUILDPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_SKETCHPARAMOBJECTS_SKETCHBUILDPARAMOBJ_HPP_

#include <string>
#include <ctime>
#include <Statistics/StatCollectors/StatCollectorParamObjects/StatCollectorBuildParamObj.hpp>

namespace NES {

	namespace Experimental::Statistics {

		class SketchBuildParamObj : public StatCollectorBuildParamObj {

		public:
			SketchBuildParamObj(const std::string& logicalSourceName,
													const std::string& physicalSourceName,
			                    const std::string& fieldName,
													uint32_t depth,
													uint32_t width,
													time_t windowSize)
					: StatCollectorBuildParamObj(logicalSourceName, physicalSourceName, fieldName, windowSize),
					  depth(depth), width(width) {}

			uint32_t getDepth() const {
				return depth;
			}

			uint32_t getWidth() const {
				return width;
			}

		private:
			const uint32_t depth;
			const uint32_t width;
		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_SYNOPSES_SKETCHES_SKETCHPARAMOBJECTS_SKETCHBUILDPARAMOBJ_HPP_

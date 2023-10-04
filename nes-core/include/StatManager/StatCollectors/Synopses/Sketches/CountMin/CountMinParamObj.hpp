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

#ifndef NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMINPARAMOBJ_HPP
#define NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMINPARAMOBJ_HPP

#include <StatManager/StatCollectors/StatCollectorParamObj.hpp>

namespace NES {

	namespace Experimental::Statistics {

		class CountMinParamObj : public StatCollectorParamObj {

		public:
			CountMinParamObj(const std::string& logicalSourceName,
			                 const std::string& physicalSourceName,
			                 const std::string& fieldName,
			                 const std::string& statCollectorType,
			                 uint32_t depth,
											 uint32_t width,
			                 time_t windowSize = 1,
			                 time_t slideFactor = 5)
					: StatCollectorParamObj(logicalSourceName, physicalSourceName, fieldName, statCollectorType, windowSize, slideFactor),
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

#endif //NES_CORE_INCLUDE_STATMANAGER_STATCOLLECTORS_SYNOPSES_SKETCHES_COUNTMIN_COUNTMINPARAMOBJ_HPP


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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_STATCOLLECTORPARAMOBJECTS_STATCOLLECTORBUILDPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_STATCOLLECTORPARAMOBJECTS_STATCOLLECTORBUILDPARAMOBJ_HPP_

#include <string>
#include <ctime>
#include <memory>

namespace NES {

namespace Experimental::Statistics {

	/**
	 * This is the abstract class for all StatCollectorBuildParamObjects
	 */
	class StatCollectorBuildParamObj {
	public:
		StatCollectorBuildParamObj(const std::string& logicalSourceName,
															 const std::string& physicalSourceName,
															 const std::string& fieldName,
															 time_t windowSize)
				: logicalSourceName(logicalSourceName),
				  physicalSourceName(physicalSourceName),
				  fieldName(fieldName),
				  windowSize(windowSize) {}

//		virtual ~StatCollectorBuildParamObj() = default;
		virtual ~StatCollectorBuildParamObj() {}

		/**
		 * @return returns the logicalSourceName over which the statCollector is to be constructed
		 */
		const std::string& getLogicalSourceName() const {
			return logicalSourceName;
		}

		/**
		 * @return returns the physicalSourceName over which the statCollector is to be constructed
		 */
		const std::string& getPhysicalSourceName() const {
			return physicalSourceName;
		}

		/**
		 * @return returns the fieldName over which the statCollector is to be constructed
		 */
		const std::string& getFieldName() const {
			return fieldName;
		}

		/**
		 * @return returns the windowSize over which the statCollector is to be constructed
		 */
		time_t getWindowSize() const {
			return windowSize;
		}

	private:
		const std::string logicalSourceName;
		const std::string physicalSourceName;
		const std::string fieldName;
		const time_t windowSize;
	};

}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_STATCOLLECTORPARAMOBJECTS_STATCOLLECTORBUILDPARAMOBJ_HPP_

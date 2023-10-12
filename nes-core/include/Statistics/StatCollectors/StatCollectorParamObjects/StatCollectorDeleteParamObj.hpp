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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_STATCOLLECTORPARAMOBJECTS_STATCOLLECTORDELETEPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_STATCOLLECTORPARAMOBJECTS_STATCOLLECTORDELETEPARAMOBJ_HPP_

#include <string>
#include <ctime>
#include <memory>

namespace NES {

	namespace Experimental::Statistics {

		class StatCollectorDeleteParamObj {
		public:
			StatCollectorDeleteParamObj(const std::string& logicalSourceName,
			                           const std::string& physicalSourceName,
			                           const std::string& fieldName,
			                           const std::string& statCollectorType,
			                           time_t windowSize)
					: logicalSourceName(logicalSourceName),
					  physicalSourceName(physicalSourceName),
					  fieldName(fieldName),
					  statCollectorType(statCollectorType),
					  windowSize(windowSize) {}

			virtual ~StatCollectorDeleteParamObj() {}

			const std::string& getLogicalSourceName() const {
				return logicalSourceName;
			}

			const std::string& getPhysicalSourceName() const {
				return physicalSourceName;
			}

			const std::string& getFieldName() const {
				return fieldName;
			}

			const std::string& getStatCollectorType() const {
				return statCollectorType;
			}

			time_t getWindowSize() const {
				return windowSize;
			}

		private:
			const std::string logicalSourceName;
			const std::string physicalSourceName;
			const std::string fieldName;
			const std::string statCollectorType;
			const time_t windowSize;
		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_STATCOLLECTORPARAMOBJECTS_STATCOLLECTORDELETEPARAMOBJ_HPP_

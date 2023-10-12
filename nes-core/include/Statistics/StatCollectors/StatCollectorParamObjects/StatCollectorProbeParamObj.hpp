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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_STATCOLLECTORPARAMOBJECTS_STATCOLLECTORPROBEPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_STATCOLLECTORPARAMOBJECTS_STATCOLLECTORPROBEPARAMOBJ_HPP_

#include <string>
#include <ctime>
#include <memory>

namespace NES {

	namespace Experimental::Statistics {

		/**
		 * This is the abstract class for all StatCollectorProbeParamObjects
		 */
		class StatCollectorProbeParamObj {
		public:
			StatCollectorProbeParamObj(const std::string& logicalSourceName,
			                           const std::string& physicalSourceName,
			                           const std::string& fieldName,
																 const std::string& statCollectorType,
																 const std::string& expression,
			                           time_t windowSize)
					: logicalSourceName(logicalSourceName),
					  physicalSourceName(physicalSourceName),
					  fieldName(fieldName),
						statCollectorType(statCollectorType),
						expression(expression),
					  windowSize(windowSize) {}

			virtual ~StatCollectorProbeParamObj() {}

			/**
			 * @return returns the logicalSourceName over which the statCollector which we want to query was generated
			 */
			const std::string& getLogicalSourceName() const {
				return logicalSourceName;
			}

			/**
			 * @return returns the physicalSourceName over which the statCollector which we want to query was generated
			 */
			const std::string& getPhysicalSourceName() const {
				return physicalSourceName;
			}

			/**
			 * @return returns the fieldName over which the statCollector which we want to query was generated
			 */
			const std::string& getFieldName() const {
				return fieldName;
			}

			/**
			 * @return returns the statCollectorType, e.g. Count-Min, HyperLogLog or similar, needed to identify the correct statCollector which is to be queried
			 */
			const std::string& getStatCollectorType() const {
				return statCollectorType;
			}

			/**
			 * @return returns the expression which describes the statistic to be queried
			 */
			const std::string& getExpression() const {
				return expression;
			}

			/**
			 * @return returns the windowSize over which the statCollector that we want to query was constructed
			 */
			time_t getWindowSize() const {
				return windowSize;
			}

		private:
			const std::string logicalSourceName;
			const std::string physicalSourceName;
			const std::string fieldName;
			const std::string statCollectorType;
			const std::string expression;
			const time_t windowSize;
		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORS_STATCOLLECTORPARAMOBJECTS_STATCOLLECTORPROBEPARAMOBJ_HPP_

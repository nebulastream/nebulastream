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

#ifndef NES_STATCOLLECTORPARAMOBJ_HPP
#define NES_STATCOLLECTORPARAMOBJ_HPP

#include <string>
#include <ctime>

namespace NES {

	using QueryId = uint64_t;

	class Schema;
	using SchemaPtr = std::shared_ptr<Schema>;

namespace Experimental::Statistics {

	class StatCollectorParamObj {
	public:
		StatCollectorParamObj(const std::string& logicalSourceName,
		                      const std::string& physicalSourceName,
		                      const std::string& fieldName,
		                      const std::string& statCollectorType,
		                      time_t windowSize = 1,
		                      time_t slideFactor = 5)
				: logicalSourceName(logicalSourceName),
				  physicalSourceName(physicalSourceName),
				  fieldName(fieldName),
				  statCollectorType(statCollectorType),
				  windowSize(windowSize),
				  slideFactor(slideFactor) {}

		virtual ~StatCollectorParamObj() {}

		const std::string& getLogicalSourceName() const {
			return logicalSourceName;
		}

		const std::string& getPhysicalSourceName() const {
			return physicalSourceName;
		}

		SchemaPtr getSchema() const {
			return schema;
		}

		void setSchema(const SchemaPtr& newSchema) {
			schema = newSchema;
		}

		const std::string& getFieldName() const {
			return fieldName;
		}

		const std::string& getStatCollectorType() const {
			return statCollectorType;
		}

		QueryId getQueryId() const {
			return queryId;
		}

		void setQueryId(QueryId newQueryId) {
			queryId = newQueryId;
		}

		time_t getWindowSize() const {
			return windowSize;
		}

		time_t getSlideFactor() const {
			return slideFactor;
		}

	private:
		const std::string logicalSourceName;
		const std::string physicalSourceName;
		SchemaPtr schema;
		const std::string fieldName;
		const std::string statCollectorType;
		QueryId queryId;
		const time_t windowSize;
		const time_t slideFactor;
	};

}
}

#endif //NES_STATCOLLECTORPARAMOBJ_HPP

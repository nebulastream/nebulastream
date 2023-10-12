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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORPARAMOBJFACTORY_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORPARAMOBJFACTORY_HPP_

#include <string>
#include <memory>

namespace NES {

	namespace Experimental::Statistics {

		class StatCollectorBuildParamObj;
		using statCollectorBuildParamObjPtr = std::shared_ptr<StatCollectorBuildParamObj>;

		class StatCollectorProbeParamObj;
		using StatCollectorProbeParamObjPtr = std::shared_ptr<StatCollectorProbeParamObj>;

		class StatCollectorDeleteParamObj;
		using StatCollectorDeleteParamObjPtr = std::shared_ptr<StatCollectorDeleteParamObj>;

		class StatCollectorParamObjFactory {

		public:

			statCollectorBuildParamObjPtr createCountMinBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName);

			StatCollectorProbeParamObjPtr createCountMinProbeParamObj(std::string& logicalSourceName,
																													 std::string& physicalSourceName,
																													 std::string& fieldName,
																													 std::string& statCollectorType,
																													 std::string& expression);

			StatCollectorDeleteParamObjPtr createCountMinDeleteParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName, std::string& statCollectorType);

			statCollectorBuildParamObjPtr createHyperLogLogBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName);

			StatCollectorProbeParamObjPtr createHyperLogLogProbeParamObj(std::string& logicalSourceName,
			                                                        std::string& physicalSourceName,
			                                                        std::string& fieldName,
			                                                        std::string& statCollectorType,
																															std::string& expression);

			StatCollectorDeleteParamObjPtr createHyperLogLogDeleteParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName, std::string& statCollectorType);

			statCollectorBuildParamObjPtr createDDSketchBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName);

			StatCollectorProbeParamObjPtr createDDSketchProbeParamObj(std::string& logicalSourceName,
			                                                     std::string& physicalSourceName,
			                                                     std::string& fieldName,
			                                                     std::string& statCollectorType,
			                                                     std::string& expression);

			StatCollectorDeleteParamObjPtr createDDSketchDeleteParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName, std::string& statCollectorType);

			statCollectorBuildParamObjPtr createReservoirSampleBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName);

			StatCollectorProbeParamObjPtr createReservoirSampleProbeParamObj(std::string& logicalSourceName,
			                                                            std::string& physicalSourceName,
																																	std::string& fieldName,
			                                                            std::string& statCollectorType,
			                                                            std::string& expression);

			StatCollectorDeleteParamObjPtr createReservoirSampleDeleteParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName, std::string& statCollectorType);

		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORPARAMOBJFACTORY_HPP_

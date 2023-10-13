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
		using StatCollectorBuildParamObjPtr = std::shared_ptr<StatCollectorBuildParamObj>;

		class StatCollectorProbeParamObj;
		using StatCollectorProbeParamObjPtr = std::shared_ptr<StatCollectorProbeParamObj>;

		class StatCollectorDeleteParamObj;
		using StatCollectorDeleteParamObjPtr = std::shared_ptr<StatCollectorDeleteParamObj>;

		/**
		 * This class serves as a factory that produces statCollectorParamObects for creation, probing, and deletion.
		 */
		class StatCollectorParamObjFactory {

		public:

			/**
			 * @brief Creates a CountMinBuildParamObj and returns a abstract statCollectorBuildParamObjPtr
			 * @param logicalSourceName the name of the logicalSource on which the Count-Min sketch is to be constructed
			 * @param physicalSourceName the name of the physicalSource on which the Count-Min sketch is to be constructed
			 * @param fieldName the name of the field on which the Count-Min sketch is to be constructed
			 * @return An abstract statCollectorBuildParamObjPtr to a CountMinBuildParamObj
			 */
			statCollectorBuildParamObjPtr createCountMinBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName);

			/**
			 * @brief Creates a CountMinProbeParamObj and returns a abstract statCollectorProbeParamObjPtr
			 * @param logicalSourceName the logicalSourceName on which the Count-Min sketch was generated
			 * @param physicalSourceName the physicalSourceName on which the Count-Min sketch was generated
			 * @param fieldName the field over which the Count-Min sketch was generated
			 * @param statCollectorType the type of statCollector, here Count-Min. This is needed to identify the correct statCollector
			 * @param expression The expression defining which statistic is to be queried
			 * @return An abstract statCollectorProbeParamObjPtr to a CountMinProbeParamObj
			 */
			StatCollectorProbeParamObjPtr createCountMinProbeParamObj(std::string& logicalSourceName,
																																std::string& physicalSourceName,
																																std::string& fieldName,
																																std::string& statCollectorType,
																																std::string& expression);

			/**
			 * @brief Creates a CountMinDeleteParamObj and returns a abstract statCollectorDeleteParamObjPtr
			 * @param logicalSourceName the logicalSourceName on which the Count-Min sketch, that is to be deleted was generated
			 * @param physicalSourceName the physicalSourceName on which the Count-Min sketch, that is to be deleted, was generated
			 * @param fieldName the field name on which the Count-Min sketch, that is to be deleted, was generated
			 * @param statCollectorType the type of the statCollector, that is to be deleted
			 * @return An abstract statCollectorDeleteParamObjPtr to a CountMinDeleteParamObj
			 */
			StatCollectorDeleteParamObjPtr createCountMinDeleteParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName, std::string& statCollectorType);

		 	/**
 			 * @brief Creates a HyperLogLogBuildParamObj and returns a abstract statCollectorBuildParamObjPtr
 			 * @param logicalSourceName the name of the logicalSource on which the HyperLogLog sketch is to be constructed
 			 * @param physicalSourceName the name of the physicalSource on which the HyperLogLog sketch is to be constructed
 			 * @param fieldName the name of the field on which the HyperLogLog sketch is to be constructed
 			 * @return An abstract statCollectorBuildParamObjPtr to a HyperLogLogBuildParamObj
 			 */
			statCollectorBuildParamObjPtr createHyperLogLogBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName);

			/**
			 * @brief Creates a HyperLogLogProbeParamObj and returns a abstract statCollectorProbeParamObjPtr
			 * @param logicalSourceName the logicalSourceName on which the HyperLogLog sketch was generated
			 * @param physicalSourceName the physicalSourceName on which the HyperLogLog sketch was generated
			 * @param fieldName the field over which the HyperLogLog sketch was generated
			 * @param statCollectorType the type of statCollector, here HyperLogLog. This is needed to identify the correct statCollector
			 * @param expression The expression defining which statistic is to be queried
			 * @return An abstract statCollectorProbeParamObjPtr to a HyperLogLogProbeParamObj
			 */
			StatCollectorProbeParamObjPtr createHyperLogLogProbeParamObj(std::string& logicalSourceName,
																																	 std::string& physicalSourceName,
																																	 std::string& fieldName,
																																	 std::string& statCollectorType,
																																	 std::string& expression);
			/**
			 * @brief Creates a HyperLogLogDeleteParamObj and returns a abstract statCollectorDeleteParamObjPtr
			 * @param logicalSourceName the logicalSourceName on which the HyperLogLog sketch, that is to be deleted was generated
			 * @param physicalSourceName the physicalSourceName on which the HyperLogLog sketch, that is to be deleted, was generated
			 * @param fieldName the field name on which the HyperLogLog sketch, that is to be deleted, was generated
			 * @param statCollectorType the type of the statCollector, that is to be deleted
			 * @return An abstract statCollectorDeleteParamObjPtr to a HyperLogLogDeleteParamObj
			 */
			StatCollectorDeleteParamObjPtr createHyperLogLogDeleteParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName, std::string& statCollectorType);

			/**
			 * @brief Creates a DDSketchBuildParamObj and returns a abstract statCollectorBuildParamObjPtr
			 * @param logicalSourceName the name of the logicalSource on which the DDSketch is to be constructed
			 * @param physicalSourceName the name of the physicalSource on which the DDSketch is to be constructed
			 * @param fieldName the name of the field on which the DDSketch is to be constructed
			 * @return An abstract statCollectorBuildParamObjPtr to a DDSketchBuildParamObj
			 */
			statCollectorBuildParamObjPtr createDDSketchBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName);

			/**
			 * @brief Creates a DDSketchProbeParamObj and returns a abstract statCollectorProbeParamObjPtr
			 * @param logicalSourceName the logicalSourceName on which the DDSketch was generated
			 * @param physicalSourceName the physicalSourceName on which the DDSketch was generated
			 * @param fieldName the field over which the DDSketch was generated
			 * @param statCollectorType the type of statCollector, here DDSketch. This is needed to identify the correct statCollector
			 * @param expression The expression defining which statistic is to be queried
			 * @return An abstract statCollectorProbeParamObjPtr to a DDSketchProbeParamObj
			 */
			StatCollectorProbeParamObjPtr createDDSketchProbeParamObj(std::string& logicalSourceName,
																																std::string& physicalSourceName,
																																std::string& fieldName,
																																std::string& statCollectorType,
																																std::string& expression);

			/**
			 * @brief Creates a DDSketchDeleteParamObj and returns a abstract statCollectorDeleteParamObjPtr
			 * @param logicalSourceName the logicalSourceName on which the DDSketch, that is to be deleted was generated
			 * @param physicalSourceName the physicalSourceName on which the DDSketch, that is to be deleted, was generated
			 * @param fieldName the field name on which the DDSketch, that is to be deleted, was generated
			 * @param statCollectorType the type of the statCollector, that is to be deleted
			 * @return An abstract statCollectorDeleteParamObjPtr to a DDSketchDeleteParamObj
			 */
			StatCollectorDeleteParamObjPtr createDDSketchDeleteParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName, std::string& statCollectorType);

			/**
			 * @brief Creates a ReservoirSampleBuildParamObj and returns a abstract statCollectorBuildParamObjPtr
			 * @param logicalSourceName the name of the logicalSource on which the Reservoir Sample is to be constructed
			 * @param physicalSourceName the name of the physicalSource on which the Reservoir Sample is to be constructed
			 * @param fieldName the name of the field on which the Reservoir Sample is to be constructed
			 * @return An abstract statCollectorBuildParamObjPtr to a ReservoirSampleBuildParamObj
			 */
			statCollectorBuildParamObjPtr createReservoirSampleBuildParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName);

			/**
			 * @brief Creates a ReservoirSampleProbeParamObj and returns a abstract statCollectorProbeParamObjPtr
			 * @param logicalSourceName the logicalSourceName on which the Reservoir Sample was generated
			 * @param physicalSourceName the physicalSourceName on which the Reservoir Sample was generated
			 * @param fieldName the field over which the Reservoir Sample was generated
			 * @param statCollectorType the type of statCollector, here ReservoirSample. This is needed to identify the correct statCollector
			 * @param expression The expression defining which statistic is to be queried
			 * @return An abstract statCollectorProbeParamObjPtr to a ReservoirSampleProbeParamObj
			 */
			StatCollectorProbeParamObjPtr createReservoirSampleProbeParamObj(std::string& logicalSourceName,
																																			 std::string& physicalSourceName,
																																			 std::string& fieldName,
																																			 std::string& statCollectorType,
																																			 std::string& expression);

			/**
			 * @brief Creates a ReservoirSampleDeleteParamObj and returns a abstract statCollectorDeleteParamObjPtr
			 * @param logicalSourceName the logicalSourceName on which the Reservoir Sample, that is to be deleted was generated
			 * @param physicalSourceName the physicalSourceName on which the Reservoir Sample, that is to be deleted, was generated
			 * @param fieldName the field name on which the Reservoir Sample, that is to be deleted, was generated
			 * @param statCollectorType the type of the statCollector, that is to be deleted
			 * @return An abstract statCollectorDeleteParamObjPtr to a ReservoirSampleDeleteParamObj
			 */
			StatCollectorDeleteParamObjPtr createReservoirSampleDeleteParamObj(std::string& logicalSourceName, std::string& physicalSourceName, std::string& fieldName, std::string& statCollectorType);
		};

	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_STATCOLLECTORPARAMOBJFACTORY_HPP_

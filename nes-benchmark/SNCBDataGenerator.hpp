#pragma once

#include <DataGeneration/DataGenerator.hpp>
#include <API/Schema.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>

namespace NES::Benchmark::DataGeneration {

/**
 * @brief SNCB Data Generator for geospatial benchmarking
 * Generates data matching the real SNCB schema with:
 * - Code1, Code2 (error codes)
 * - speed (velocity)
 * - longitude, latitude (geospatial coordinates)
 * - timestamp (temporal data)
 */
class SNCBDataGenerator : public DataGenerator {
private:
    uint64_t minSpeed;
    uint64_t maxSpeed;
    double minLongitude;
    double maxLongitude;
    double minLatitude;
    double maxLatitude;
    
public:
    explicit SNCBDataGenerator(uint64_t minSpeed = 0, 
                               uint64_t maxSpeed = 120,
                               double minLongitude = 4.0,  // Belgium longitude range
                               double maxLongitude = 6.5,
                               double minLatitude = 49.5,  // Belgium latitude range  
                               double maxLatitude = 51.5);

    /**
     * @brief Generates SNCB-style data buffers
     */
    std::vector<Runtime::TupleBuffer> generateTupleBuffers(uint64_t numberOfBuffers) override;

    /**
     * @brief Returns the SNCB schema matching your real data
     */
    SchemaPtr getSchema() override;

    /**
     * @brief Returns the schema type configuration
     */
    Configurations::SchemaTypePtr getSchemaType() override;

    /**
     * @brief Returns generator name
     */
    std::string getName() override;

    /**
     * @brief String representation
     */
    std::string toString() override;
};

}// namespace NES::Benchmark::DataGeneration 
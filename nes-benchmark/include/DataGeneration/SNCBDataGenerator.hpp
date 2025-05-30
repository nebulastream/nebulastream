#pragma once

#include <DataGeneration/DataGenerator.hpp>
#include <API/Schema.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <vector>
#include <string>

namespace NES::Benchmark::DataGeneration {

/**
 * @brief SNCB Data Generator for geospatial benchmarking
 * Reads real SNCB data from CSV file and generates buffers
 */
class SNCBDataGenerator : public DataGenerator {
private:
    struct SNCBRecord {
        uint64_t time_utc;
        uint64_t device_id;
        double Vbat;
        double PCFA_mbar;
        double PCFF_mbar;
        double PCF1_mbar;
        double PCF2_mbar;
        double T1_mbar;
        double T2_mbar;
        uint64_t Code1;
        uint64_t Code2;
        double gps_speed;
        double gps_lat;
        double gps_lon;
    };
    
    std::vector<SNCBRecord> csvData;
    size_t currentRow;
    std::string csvFilePath;
    
    uint64_t minSpeed;
    uint64_t maxSpeed;
    double minLongitude;
    double maxLongitude;
    double minLatitude;
    double maxLatitude;
    
    void loadCSVData();
    
public:
    explicit SNCBDataGenerator(const std::string& csvPath = "/media/psf/Home/Parallels/nebulaQueryAPI/data/selected_columns_df.csv",
                               uint64_t minSpeed = 0, 
                               uint64_t maxSpeed = 120,
                               double minLongitude = 4.0,  // Belgium longitude range
                               double maxLongitude = 6.5,
                               double minLatitude = 49.5,  // Belgium latitude range  
                               double maxLatitude = 51.5);

    /**
     * @brief Generates SNCB-style data buffers
     */
    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;

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
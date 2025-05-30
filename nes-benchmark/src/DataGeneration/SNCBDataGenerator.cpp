#include <DataGeneration/SNCBDataGenerator.hpp>
#include <API/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <Util/Logger/Logger.hpp>
#include <random>
#include <chrono>
#include <sstream>
#include <fstream>
#include <iostream>

namespace NES::Benchmark::DataGeneration {

SNCBDataGenerator::SNCBDataGenerator(const std::string& csvPath,
                                     uint64_t minSpeed, 
                                     uint64_t maxSpeed,
                                     double minLongitude,
                                     double maxLongitude,
                                     double minLatitude,
                                     double maxLatitude)
    : DataGenerator(), currentRow(0), csvFilePath(csvPath),
      minSpeed(minSpeed), maxSpeed(maxSpeed),
      minLongitude(minLongitude), maxLongitude(maxLongitude),
      minLatitude(minLatitude), maxLatitude(maxLatitude) {
    loadCSVData();
}

void SNCBDataGenerator::loadCSVData() {
    std::ifstream file(csvFilePath);
    if (!file.is_open()) {
        NES_WARNING("SNCBDataGenerator: Could not open CSV file '{}'. Will generate random data.", csvFilePath);
        return;
    }
    
    std::string line;
    // Skip header
    std::getline(file, line);
    
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string field;
        SNCBRecord record;
        
        // Parse CSV fields
        std::getline(ss, field, ',');
        record.time_utc = std::stoull(field);
        
        std::getline(ss, field, ',');
        record.device_id = std::stoull(field);
        
        std::getline(ss, field, ',');
        record.Vbat = std::stod(field);
        
        std::getline(ss, field, ',');
        record.PCFA_mbar = std::stod(field) / 1000.0; // Convert mbar to bar
        
        std::getline(ss, field, ',');
        record.PCFF_mbar = std::stod(field) / 1000.0;
        
        std::getline(ss, field, ',');
        record.PCF1_mbar = std::stod(field) / 1000.0;
        
        std::getline(ss, field, ',');
        record.PCF2_mbar = std::stod(field) / 1000.0;
        
        std::getline(ss, field, ',');
        record.T1_mbar = std::stod(field) / 1000.0;
        
        std::getline(ss, field, ',');
        record.T2_mbar = std::stod(field) / 1000.0;
        
        std::getline(ss, field, ',');
        record.Code1 = std::stoull(field);
        
        std::getline(ss, field, ',');
        record.Code2 = std::stoull(field);
        
        std::getline(ss, field, ',');
        record.gps_speed = std::stod(field);
        
        std::getline(ss, field, ',');
        record.gps_lat = std::stod(field);
        
        std::getline(ss, field, ',');
        record.gps_lon = std::stod(field);
        
        csvData.push_back(record);
    }
    
    file.close();
    NES_INFO("SNCBDataGenerator: Loaded {} records from CSV file", csvData.size());
}

std::vector<Runtime::TupleBuffer> SNCBDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    auto memoryLayout = getMemoryLayout(bufferSize);
    std::vector<Runtime::TupleBuffer> buffers;
    
    // Random number generators for fields not in CSV
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> bearingDist(0.0, 360.0);
    std::uniform_int_distribution<uint64_t> sizeDist(10, 100);
    std::uniform_int_distribution<uint64_t> typeDist(0, 10);
    
    // No need to initialize lastTimestamp when using CSV data directly
    
    for (uint64_t currentBuffer = 0; currentBuffer < numberOfBuffers; currentBuffer++) {
        auto buffer = allocateBuffer();
        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
        
        for (uint64_t currentRecord = 0; currentRecord < testBuffer.getCapacity(); currentRecord++) {
            // If we have CSV data, use it; otherwise generate random data
            if (!csvData.empty()) {
                // Get current row from CSV data (cycle through if needed)
                const SNCBRecord& record = csvData[currentRow % csvData.size()];
                currentRow++;
                
                // Use the actual timestamp from CSV data
                // The CSV data is already ordered, so timestamps should be monotonic
                uint64_t timestamp = record.time_utc * 1000; // Convert to milliseconds
                uint64_t wstart = timestamp - (timestamp % 1000);
                uint64_t wend = wstart + 1000;
                
                // Map CSV fields to buffer fields
                testBuffer[currentRecord]["device_id"].write<uint64_t>(record.device_id);
                testBuffer[currentRecord]["wstart"].write<uint64_t>(wstart);
                testBuffer[currentRecord]["wend"].write<uint64_t>(wend);
                testBuffer[currentRecord]["bearing"].write<double>(bearingDist(gen));
                testBuffer[currentRecord]["device_speed"].write<uint64_t>(static_cast<uint64_t>(record.gps_speed));
                testBuffer[currentRecord]["coordinate_x"].write<double>(record.gps_lon);
                testBuffer[currentRecord]["coordinate_y"].write<double>(record.gps_lat);
                testBuffer[currentRecord]["Code1"].write<uint64_t>(record.Code1);
                testBuffer[currentRecord]["Code2"].write<uint64_t>(record.Code2);
                testBuffer[currentRecord]["SOG"].write<double>(record.gps_speed / 10.0);
                testBuffer[currentRecord]["COG"].write<double>(bearingDist(gen));
                testBuffer[currentRecord]["Heading"].write<double>(bearingDist(gen));
                testBuffer[currentRecord]["ROT"].write<double>(0.0);
                testBuffer[currentRecord]["NavStat"].write<uint64_t>(0);
                testBuffer[currentRecord]["IMO"].write<uint64_t>(1234567);
                testBuffer[currentRecord]["CallSign"].write<uint64_t>(12345);
                testBuffer[currentRecord]["VesselName"].write<uint64_t>(67890);
                testBuffer[currentRecord]["VesselType"].write<uint64_t>(typeDist(gen));
                testBuffer[currentRecord]["CargoType"].write<uint64_t>(typeDist(gen));
                testBuffer[currentRecord]["Width"].write<double>(sizeDist(gen) / 10.0);
                testBuffer[currentRecord]["Length"].write<double>(sizeDist(gen));
                testBuffer[currentRecord]["Type_of_position_fixing_device"].write<uint64_t>(1);
                testBuffer[currentRecord]["Draught"].write<double>(sizeDist(gen) / 20.0);
                testBuffer[currentRecord]["Destination"].write<uint64_t>(54321);
                testBuffer[currentRecord]["ETA"].write<uint64_t>(timestamp + 3600000);
                testBuffer[currentRecord]["Data_source_type"].write<uint64_t>(1);
                testBuffer[currentRecord]["SizeA"].write<double>(sizeDist(gen) / 10.0);
                testBuffer[currentRecord]["SizeB"].write<double>(sizeDist(gen) / 10.0);
                testBuffer[currentRecord]["SizeC"].write<double>(sizeDist(gen) / 10.0);
                testBuffer[currentRecord]["SizeD"].write<double>(sizeDist(gen) / 10.0);
                testBuffer[currentRecord]["Geofence_lat"].write<double>(record.gps_lat);
                testBuffer[currentRecord]["Geofence_lon"].write<double>(record.gps_lon);
                testBuffer[currentRecord]["T1_bar"].write<double>(record.T1_mbar);
                testBuffer[currentRecord]["T2_bar"].write<double>(record.T2_mbar);
                testBuffer[currentRecord]["T3_bar"].write<double>(record.T1_mbar); // Use T1 for T3
                testBuffer[currentRecord]["T4_bar"].write<double>(record.T2_mbar); // Use T2 for T4
                testBuffer[currentRecord]["speed"].write<uint64_t>(static_cast<uint64_t>(record.gps_speed));
                testBuffer[currentRecord]["PCF1_bar"].write<double>(record.PCF1_mbar);
                testBuffer[currentRecord]["PCF2_bar"].write<double>(record.PCF2_mbar);
                testBuffer[currentRecord]["PCA_bar"].write<double>((record.PCF1_mbar + record.PCF2_mbar) / 2.0);
                testBuffer[currentRecord]["PCFA_bar"].write<double>(record.PCFA_mbar);
                testBuffer[currentRecord]["PCFF_bar"].write<double>(record.PCFF_mbar);
                
                // Add the fields used in queries
                testBuffer[currentRecord]["longitude"].write<double>(record.gps_lon);
                testBuffer[currentRecord]["latitude"].write<double>(record.gps_lat);
                testBuffer[currentRecord]["timestamp"].write<uint64_t>(timestamp);
            } else {
                // Fallback to random data generation if no CSV data
                std::uniform_int_distribution<uint64_t> speedDist(minSpeed, maxSpeed);
                std::uniform_int_distribution<uint64_t> codeDist(0, 10);
                std::uniform_real_distribution<double> lonDist(minLongitude, maxLongitude);
                std::uniform_real_distribution<double> latDist(minLatitude, maxLatitude);
                std::uniform_real_distribution<double> barDist(2.5, 4.5);
                std::uniform_real_distribution<double> pcDist(0.0, 1.0);
                
                // For random data, use current time plus offset
                auto baseTime = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                uint64_t timestamp = baseTime + (currentBuffer * testBuffer.getCapacity() + currentRecord) * 10;
                uint64_t wstart = timestamp - (timestamp % 1000);
                uint64_t wend = wstart + 1000;
                
                // Generate random data for all fields
                testBuffer[currentRecord]["device_id"].write<uint64_t>(3000 + typeDist(gen));
                testBuffer[currentRecord]["wstart"].write<uint64_t>(wstart);
                testBuffer[currentRecord]["wend"].write<uint64_t>(wend);
                testBuffer[currentRecord]["bearing"].write<double>(bearingDist(gen));
                testBuffer[currentRecord]["device_speed"].write<uint64_t>(speedDist(gen));
                testBuffer[currentRecord]["coordinate_x"].write<double>(lonDist(gen));
                testBuffer[currentRecord]["coordinate_y"].write<double>(latDist(gen));
                testBuffer[currentRecord]["Code1"].write<uint64_t>(codeDist(gen));
                testBuffer[currentRecord]["Code2"].write<uint64_t>(codeDist(gen));
                testBuffer[currentRecord]["SOG"].write<double>(speedDist(gen) / 10.0);
                testBuffer[currentRecord]["COG"].write<double>(bearingDist(gen));
                testBuffer[currentRecord]["Heading"].write<double>(bearingDist(gen));
                testBuffer[currentRecord]["ROT"].write<double>(0.0);
                testBuffer[currentRecord]["NavStat"].write<uint64_t>(0);
                testBuffer[currentRecord]["IMO"].write<uint64_t>(1234567);
                testBuffer[currentRecord]["CallSign"].write<uint64_t>(12345);
                testBuffer[currentRecord]["VesselName"].write<uint64_t>(67890);
                testBuffer[currentRecord]["VesselType"].write<uint64_t>(typeDist(gen));
                testBuffer[currentRecord]["CargoType"].write<uint64_t>(typeDist(gen));
                testBuffer[currentRecord]["Width"].write<double>(sizeDist(gen) / 10.0);
                testBuffer[currentRecord]["Length"].write<double>(sizeDist(gen));
                testBuffer[currentRecord]["Type_of_position_fixing_device"].write<uint64_t>(1);
                testBuffer[currentRecord]["Draught"].write<double>(sizeDist(gen) / 20.0);
                testBuffer[currentRecord]["Destination"].write<uint64_t>(54321);
                testBuffer[currentRecord]["ETA"].write<uint64_t>(timestamp + 3600000);
                testBuffer[currentRecord]["Data_source_type"].write<uint64_t>(1);
                testBuffer[currentRecord]["SizeA"].write<double>(sizeDist(gen) / 10.0);
                testBuffer[currentRecord]["SizeB"].write<double>(sizeDist(gen) / 10.0);
                testBuffer[currentRecord]["SizeC"].write<double>(sizeDist(gen) / 10.0);
                testBuffer[currentRecord]["SizeD"].write<double>(sizeDist(gen) / 10.0);
                testBuffer[currentRecord]["Geofence_lat"].write<double>(latDist(gen));
                testBuffer[currentRecord]["Geofence_lon"].write<double>(lonDist(gen));
                testBuffer[currentRecord]["T1_bar"].write<double>(barDist(gen));
                testBuffer[currentRecord]["T2_bar"].write<double>(barDist(gen));
                testBuffer[currentRecord]["T3_bar"].write<double>(barDist(gen));
                testBuffer[currentRecord]["T4_bar"].write<double>(barDist(gen));
                testBuffer[currentRecord]["speed"].write<uint64_t>(speedDist(gen));
                testBuffer[currentRecord]["PCF1_bar"].write<double>(barDist(gen));
                testBuffer[currentRecord]["PCF2_bar"].write<double>(barDist(gen));
                testBuffer[currentRecord]["PCA_bar"].write<double>(barDist(gen));
                testBuffer[currentRecord]["PCFA_bar"].write<double>(pcDist(gen));
                testBuffer[currentRecord]["PCFF_bar"].write<double>(pcDist(gen));
                testBuffer[currentRecord]["longitude"].write<double>(lonDist(gen));
                testBuffer[currentRecord]["latitude"].write<double>(latDist(gen));
                testBuffer[currentRecord]["timestamp"].write<uint64_t>(timestamp);
            }
        }
        
        testBuffer.setNumberOfTuples(testBuffer.getCapacity());
        buffers.emplace_back(buffer);
    }
    
    return buffers;
}

SchemaPtr SNCBDataGenerator::getSchema() {
    return Schema::create()
        ->addField("device_id", BasicType::UINT64)
        ->addField("wstart", BasicType::UINT64)
        ->addField("wend", BasicType::UINT64)
        ->addField("bearing", BasicType::FLOAT64)
        ->addField("device_speed", BasicType::UINT64)
        ->addField("coordinate_x", BasicType::FLOAT64)
        ->addField("coordinate_y", BasicType::FLOAT64)
        ->addField("Code1", BasicType::UINT64)
        ->addField("Code2", BasicType::UINT64)
        ->addField("SOG", BasicType::FLOAT64)
        ->addField("COG", BasicType::FLOAT64)
        ->addField("Heading", BasicType::FLOAT64)
        ->addField("ROT", BasicType::FLOAT64)
        ->addField("NavStat", BasicType::UINT64)
        ->addField("IMO", BasicType::UINT64)
        ->addField("CallSign", BasicType::UINT64)
        ->addField("VesselName", BasicType::UINT64)
        ->addField("VesselType", BasicType::UINT64)
        ->addField("CargoType", BasicType::UINT64)
        ->addField("Width", BasicType::FLOAT64)
        ->addField("Length", BasicType::FLOAT64)
        ->addField("Type_of_position_fixing_device", BasicType::UINT64)
        ->addField("Draught", BasicType::FLOAT64)
        ->addField("Destination", BasicType::UINT64)
        ->addField("ETA", BasicType::UINT64)
        ->addField("Data_source_type", BasicType::UINT64)
        ->addField("SizeA", BasicType::FLOAT64)
        ->addField("SizeB", BasicType::FLOAT64)
        ->addField("SizeC", BasicType::FLOAT64)
        ->addField("SizeD", BasicType::FLOAT64)
        ->addField("Geofence_lat", BasicType::FLOAT64)
        ->addField("Geofence_lon", BasicType::FLOAT64)
        ->addField("T1_bar", BasicType::FLOAT64)
        ->addField("T2_bar", BasicType::FLOAT64)
        ->addField("T3_bar", BasicType::FLOAT64)
        ->addField("T4_bar", BasicType::FLOAT64)
        ->addField("speed", BasicType::UINT64)
        ->addField("PCF1_bar", BasicType::FLOAT64)
        ->addField("PCF2_bar", BasicType::FLOAT64)
        ->addField("PCA_bar", BasicType::FLOAT64)
        ->addField("PCFA_bar", BasicType::FLOAT64)
        ->addField("PCFF_bar", BasicType::FLOAT64)
        ->addField("longitude", BasicType::FLOAT64)
        ->addField("latitude", BasicType::FLOAT64)
        ->addField("timestamp", BasicType::UINT64);
}

Configurations::SchemaTypePtr SNCBDataGenerator::getSchemaType() {
    const char* dataTypeUI64 = "UINT64";
    const char* dataTypeF64 = "FLOAT64";
    std::vector<Configurations::SchemaFieldDetail> schemaFieldDetails;
    
    schemaFieldDetails.emplace_back("device_id", dataTypeUI64);
    schemaFieldDetails.emplace_back("wstart", dataTypeUI64);
    schemaFieldDetails.emplace_back("wend", dataTypeUI64);
    schemaFieldDetails.emplace_back("bearing", dataTypeF64);
    schemaFieldDetails.emplace_back("device_speed", dataTypeUI64);
    schemaFieldDetails.emplace_back("coordinate_x", dataTypeF64);
    schemaFieldDetails.emplace_back("coordinate_y", dataTypeF64);
    schemaFieldDetails.emplace_back("Code1", dataTypeUI64);
    schemaFieldDetails.emplace_back("Code2", dataTypeUI64);
    schemaFieldDetails.emplace_back("SOG", dataTypeF64);
    schemaFieldDetails.emplace_back("COG", dataTypeF64);
    schemaFieldDetails.emplace_back("Heading", dataTypeF64);
    schemaFieldDetails.emplace_back("ROT", dataTypeF64);
    schemaFieldDetails.emplace_back("NavStat", dataTypeUI64);
    schemaFieldDetails.emplace_back("IMO", dataTypeUI64);
    schemaFieldDetails.emplace_back("CallSign", dataTypeUI64);
    schemaFieldDetails.emplace_back("VesselName", dataTypeUI64);
    schemaFieldDetails.emplace_back("VesselType", dataTypeUI64);
    schemaFieldDetails.emplace_back("CargoType", dataTypeUI64);
    schemaFieldDetails.emplace_back("Width", dataTypeF64);
    schemaFieldDetails.emplace_back("Length", dataTypeF64);
    schemaFieldDetails.emplace_back("Type_of_position_fixing_device", dataTypeUI64);
    schemaFieldDetails.emplace_back("Draught", dataTypeF64);
    schemaFieldDetails.emplace_back("Destination", dataTypeUI64);
    schemaFieldDetails.emplace_back("ETA", dataTypeUI64);
    schemaFieldDetails.emplace_back("Data_source_type", dataTypeUI64);
    schemaFieldDetails.emplace_back("SizeA", dataTypeF64);
    schemaFieldDetails.emplace_back("SizeB", dataTypeF64);
    schemaFieldDetails.emplace_back("SizeC", dataTypeF64);
    schemaFieldDetails.emplace_back("SizeD", dataTypeF64);
    schemaFieldDetails.emplace_back("Geofence_lat", dataTypeF64);
    schemaFieldDetails.emplace_back("Geofence_lon", dataTypeF64);
    schemaFieldDetails.emplace_back("T1_bar", dataTypeF64);
    schemaFieldDetails.emplace_back("T2_bar", dataTypeF64);
    schemaFieldDetails.emplace_back("T3_bar", dataTypeF64);
    schemaFieldDetails.emplace_back("T4_bar", dataTypeF64);
    schemaFieldDetails.emplace_back("speed", dataTypeUI64);
    schemaFieldDetails.emplace_back("PCF1_bar", dataTypeF64);
    schemaFieldDetails.emplace_back("PCF2_bar", dataTypeF64);
    schemaFieldDetails.emplace_back("PCA_bar", dataTypeF64);
    schemaFieldDetails.emplace_back("PCFA_bar", dataTypeF64);
    schemaFieldDetails.emplace_back("PCFF_bar", dataTypeF64);
    schemaFieldDetails.emplace_back("longitude", dataTypeF64);
    schemaFieldDetails.emplace_back("latitude", dataTypeF64);
    schemaFieldDetails.emplace_back("timestamp", dataTypeUI64);
    
    return Configurations::SchemaType::create(schemaFieldDetails);
}

std::string SNCBDataGenerator::getName() {
    return "SNCB";
}

std::string SNCBDataGenerator::toString() {
    std::ostringstream oss;
    oss << getName() << " (speed: " << minSpeed << "-" << maxSpeed 
        << ", lon: " << minLongitude << "-" << maxLongitude
        << ", lat: " << minLatitude << "-" << maxLatitude << ")";
    return oss.str();
}

}// namespace NES::Benchmark::DataGeneration 
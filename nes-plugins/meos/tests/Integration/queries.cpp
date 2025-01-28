// Query 1 (Geofencing) --------------------------------------------------------
// Location-Based Alert Filtering
// The system determines whether a train is within a maintenance area. 
// If confirmed, alerts such as speed violations or equipment malfunctions are filtered out, 
// as they are redundant during maintenance.

auto query = Query::from("sncb")
            .filter(
                // Check if train is in maintenance area
                teintersects(Attribute("longitude", BasicType::FLOAT64),
                    Attribute("latitude", BasicType::FLOAT64),
                    Attribute("timestamp", BasicType::UINT64)) == 1
                && 
                // Only process records with valid equipment codes
                (Attribute("Code1") != 0 || Attribute("Code2") != 0)
            )
            .window(SlidingWindow::of(EventTime(Attribute("timestamp", BasicType::UINT64)), 
                                    Seconds(30), Seconds(30)))
            .apply(Sum(Attribute("speed")));



// Query 2 (Geofencing) --------------------------------------------------------
// Location-Based Noise Monitoring
// The query constantly monitors the sound levels outside the train
// using speed and break pressure data.

auto query = Query::from("sncb")
            .filter(
                tpointatstbox(Attribute("longitude", BasicType::FLOAT64),
                    Attribute("latitude", BasicType::FLOAT64),
                    Attribute("timestamp", BasicType::UINT64)) == 1
                && 
                Attribute("speed") > 0 && Attribute("PCFA_mbar") > 0
                &&
                Attribute("speed") * 0.5 + Attribute("PCFA_mbar") * 0.5 > 10)
            .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(20)))
            .project( Attribute("latitude"),
                      Attribute("longitude"),
                      Attribute("timestamp"),
                      Attribute("speed"),
                      Attribute("PCFA_mbar"));



// Query 3 (Geofencing) --------------------------------------------------------
// Dynamic Speed Limit
// We can suggest speed restrictions dynamically, adapting to specific zones, 
// such as curves and other construction zones.

auto subquery = Query::from("highriskArea")
            .map(Attribute("highriskArea$timestamp") = Attribute("timestamp"))
            .filter(tedwithin(Attribute("longitude", BasicType::FLOAT64),
                              Attribute("latitude", BasicType::FLOAT64),
                              Attribute("timestamp", BasicType::UINT64)) == 1
                    && Attribute("timestamp", BasicType::UINT64) > 0);


auto query = Query::from("sncb")
            .joinWith(subquery)
            .where(Attribute("timestamp") == Attribute("highriskArea$timestamp"))
            .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(30), Seconds(10)))
            .filter(
                (ABS(Attribute("sncb$latitude") - Attribute("highriskArea$latitude")) <= slackDegrees) &&
                (ABS(Attribute("sncb$longitude") - Attribute("highriskArea$longitude")) <= slackDegrees)
            )
            .project(Attribute("timestamp"),
                     Attribute("id"),
                     Attribute("speed"),
                     Attribute("Vbat"));



// Query 4 (Geofencing) ------------------------------------------------------
// Weather-Based Speed Zones
// The system uses integrated weather data to suggest speed limits for 
// conditions such as heavy rain or fog, maintaining safety operations.



// Query 5 (CEP) --------------------------------------------------------
// Battery Monitoring
// monitoring battery usage to prevent overheating and excessive discharge. 
// It queries nearby workshops.




// Query 6 (CEP) --------------------------------------------------------
// Heavy Load of Passengers
// Detect a heavy load of passengers by monitoring train sensors. 
// These sensors include weight detectors that provide real-time data for analysis. 
// The system can adjust temperature controls and lighting to maintain optimal conditions 
// and resource waste in response to heavy loads. 



// Query 7 (CEP) --------------------------------------------------------
// Query Unscheduled Stops
// The system compares the train’s real-time GPS location with known station and workshop zones. 
// If the train’s status is stopped outside these zones, the stop is flagged as unscheduled.
// This alert helps operators act fast, whether they need to send help, investigate mechanical problems, 
// or address other safety concerns. It also prevents unauthorized halts that could disrupt the timetable and affect service reliability.



// Query 8 (CEP) --------------------------------------------------------
// Monitoring Emergency Brake Usage and Brake Pressures
// Trains rely on the correct brake pressure to stop safely. 
// If the pressure is out of range, there is a higher risk of mechanical damage and unsafe braking distances.
// In parallel, frequent emergency brake use can point to hazards or driver errors. 
// Collecting these two data points in real-time and mapping them to the train’s location, 
// the system can detect patterns such as repeated emergency brakes in specific track segments or persistent low-pressure readings on steep inclines. 
// When these anomalies appear, the system sends an alert to help staff take prompt action. 
// This approach ensures safer operations and targeted maintenance in unusual brake activity areas


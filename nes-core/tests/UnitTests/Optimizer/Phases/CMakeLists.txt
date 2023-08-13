# Optimizer Phase Tests
add_nes_unit_test(optimizer-phase-tests
        "TypeInferencePhaseTest.cpp"
        "OriginIdInferencePhaseTest.cpp"
        "MemoryLayoutSelectionPhaseTest.cpp"
        "GlobalQueryPlanUpdatePhaseTest.cpp"
        "SignatureInferencePhaseTest.cpp")

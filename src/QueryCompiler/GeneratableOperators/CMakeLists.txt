add_source_nes(
        GeneratableOperator.cpp
        GeneratableFilterOperator.cpp
        GeneratableMapOperator.cpp
        GeneratableMergeOperator.cpp
        GeneratableSinkOperator.cpp
        GeneratableScanOperator.cpp
        TranslateToGeneratableOperatorPhase
)

add_subdirectory(Windowing)

Below are the state transitions of our `PipelineBuilder` state machine:

``` mermaid
flowchart LR
    A[Start]
    B(BuildingPipeline)
    C(AfterEmit)
    D(SourceCreated)
    E(SinkCreated)
    F{Success}

    A --> | EncounterSource | D

    B --> | DescendChild | B
    B --> | ChildDone | B
    B --> | EncounterCustomScan | B
    B --> | EncounterCustomEmit  | C
    B --> | EncounterSink | E
    B --> | EncounterFusibleOperator | B
    B --> | EncounterEnd | F

    C --> | DescendChild | B

    D --> | DescendChild | D
    D --> | EncounterCustomScan  | B
    D --> | EncounterSink | E
    D --> | EncounterFusibleOperator | B

    E --> | ChildDone | B
```
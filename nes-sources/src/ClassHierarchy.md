# Todo: remove in #75, when completing #69
# Description
This document accompanies the epic issue #69. Upon completing #69 in #75, which is a clean up task,
we convert this document to the final documentation of sources and sinks (after refactoring).

# Version 1
```mermaid
---
title: Sources Implementation Overview
---
classDiagram
    SourceHandle --> Source : takes implementation from
    Source --> PullingSource : implemented by
    Source --> PushingSource : implemented by
    PullingSource --> FileSource : calls 'pull()' in start()
    PushingSource --> SourceTCP : start() -> subscribe, close() -> unsubscribe

    namespace OutsideFacing {
        class SourceHandle {
            - Source sourceImpl
            + virtual void start()
            + virtual void stop()
        }
    }
    
    namespace Internal {
        class Source {
            + void virtual start()
            + void virtual stop()
        }
        
        class PullingSource {
            + void start()
            + void stop()
            - std::jthread thread
            + SourceImplementation sourceImpl
        }
        
        class PushingSource {
            + void start()
            + void stop()
            - std::jthread thread
            + SourceImplementation sourceImpl
        }
        
        class FileSource {
            + void pull()
            - std::string filePath
        }
        
        class SourceTCP {
            + void push()
            - std::string host
            - std::string port
        }
    }
```
(G3): FileSource/SourceTCP should implement a common 'SourceInterface' to reside in the same registry
---
# Version 2
```mermaid
---
title: Sources Implementation Overview
---
classDiagram
    SourceHandle --> Source : takes implementation from
    Source --> SourceData : query start&stop logic implemented by
    Source ..> FileSource : data ingestion implemented by 
    Source ..> SourceTCP : data ingestion implemented by
    SourceData --> FileSource : start() -> connect & pull, stop() -> close
    SourceData --> SourceTCP : start() -> connect & pull, stop() -> close
    
    note for Source "- Interface for PluginRegistry\n - defers start/stop to\nPulling/PushingSource"
    
namespace OutsideFacing {
    class SourceHandle {
        + Source(Pulling/PushingSource) sourceImpl
        + virtual void start()
        + virtual void stop()
    }
}

namespace Internal {
    %% Source is the interface for the PluginRegistry for sources
    class Source {
        + void virtual start()
        + void virtual stop()
    }
    
    class SourceData {
        + void start()
        + void stop()
        + Source(File/TCP/..) sourceImpl
    }
    
    class FileSource {
        + void start()
        + void stop()
        - std::string filePath
    }
    
    class SourceTCP {
        + void start()
        + void stop()
        - std::string host
        - std::string port
    }
}
```
---
# Version 2.5
```mermaid
---
title: Sources Implementation Overview
---
classDiagram
    SourceHandle --> SourceData : uses
    FileSource --> Source : implements
    SourceTCP --> Source  : implements
    SourceData --> FileSource : uses
    SourceData --> SourceTCP : uses

    namespace OutsideFacing {
        class SourceHandle {
            + SourceData sourceData
            + void start()
            + void stop()
            + OriginId getSourceId()
            + std::string toString()
        }
    }
    namespace RegistrySource {
    %% Source is the interface for the PluginRegistry for sources
        class Source {
            + virtual void open()
            + virtual void close()
            + virtual void fillTupleBuffer()
            + virtual SourceType getType()
            + virtual std::string toString()
        }
    }

    namespace Internal {

        class SourceData {
            + void start()
            + void stop()
            + OriginId getSourceId()
            + std::string toString()
            + Source sourceImpl
        }

        class FileSource {
            + void open()
            + void close()
            + void fillTupleBuffer()
            + SourceType getType()
            + std::string toString()
            - std::string filePath
        }

        class SourceTCP {
            + void open()
            + void close()
            + void fillTupleBuffer()
            + SourceType getType()
            + std::string toString()
            - std::string host
            - std::string port
        }
    }
```
TODO #237: Still uses old threading logic.
---
# Version (Vision) 3
```mermaid
---
title: Sources Implementation Overview
---
classDiagram
    SourceHandle --> Source : takes implementation from
    Source --> PullingSource : query start&stop logic implemented by
    Source --> PushingSource : query start&stop logic implemented by
    PullingSource --> FileSource : start() -> connect & pull, stop() -> close
    PushingSource --> SourceTCP : start() -> subscribe, close() -> unsubscribe
    Source ..> FileSource : data ingestion implemented by 
    Source ..> SourceTCP : data ingestion implemented by
    
    note for Source "- Interface for PluginRegistry\n - defers start/stop to\nPulling/PushingSource"
    
namespace OutsideFacing {
    class SourceHandle {
        + Source(Pulling/PushingSource) sourceImpl
        + virtual void start()
        + virtual void stop()
    }
}

namespace Internal {
    %% Source is the interface for the PluginRegistry for sources
    class Source {
        + void virtual start()
        + void virtual stop()
    }
    
    class PullingSource {
        + void start()
        + void stop()
        - std::jthread thread
        + Source(File/TCP/..) sourceImpl
    }
    
    class PushingSource {
        + void start()
        + void stop()
        + Source(File/TCP/..) sourceImpl
    }
    
    class FileSource {
        + void start()
        + void stop()
        - std::string filePath
    }
    
    class SourceTCP {
        + void start()
        + void stop()
        - std::string host
        - std::string port
    }
}
```

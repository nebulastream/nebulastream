# Overview of nes-sources module 
We propose the following solution. Users describe sources by providing the type of the Source, e.g. File or TCP,
the schema and name of the logical source and the input format, e.g. CSV or JSON. During parsing, we create a validated and strongly types **SourceDescriptor**
from the description of the user and attach it to the query plan. During lowering, we give the *SourceDescriptor* to the **SourceProvider**, which constructs
the described **Source** using the **SourceRegistry** and, using the constructed Source, constructs and returns a **SourceHandle** which
becomes part of an executable query plan. The SourceHandel offers a very slim interface, `start()` and `stop()` and thereby hides all the
implementation details from users of sources. Internally, the SourceHandle constructs a **SourceThread** and delegates the start and stop
calls to the *SourceThread*. The SourceThread starts a thread, so one thread per source, which runs the `runningRoutine()`. In the running routine,
the SourceThread repeatedly calls the `fillTupleBuffer` function of the specific *Source* implementation, e.g., of the **TCPSource**.
If `fillTupleBuffer` succeeds, the *SourceThread* returns a TupleBuffer to the runtime via the *EmitFunction*, if not, it returns an
error using the *EmitFunction*.
```mermaid
---
title: Sources Implementation Overview
---
classDiagram
    SourceHandle --> SourceThread : calls start/stop of SourceThread
    SourceThread --> Source : calls fillTupleBuffer in running routine
    Source ..> FileSource : data ingestion implemented by
    Source ..> TCPSource : data ingestion implemented by
    SourceProvider --> SourceRegistry : provide SourceDescriptor
    SourceRegistry --> SourceProvider : return Source
    SourceProvider --> SourceHandle : construct SourceHandle
    SourceDescriptor --> SourceProvider : fully describes Source
    PhysicalInputFormatter --> SourceValidationRegistry : provides string description of source
    SourceValidationRegistry --> PhysicalInputFormatter : validates and constructs typed descriptor
    PhysicalInputFormatter --> SourceDescriptor : constructs SourceDescriptor if valid
    
    namespace Parser {
        class PhysicalInputFormatter {
            SourceDescriptor createSourceDescriptor(unordered_map~string, string~)
        }
    }
    namespace QueryPlan {
        class SourceDescriptor {
            const shared_ptr~Schema~ schema
            const string logicalSourceName
            const string sourceType
            const Configurations::InputFormat inputFormat
        }
        class SourceHandle {
            + virtual void start()
            + virtual void stop()
            - SourceThread sourceThread
        }
    }
    namespace SourceConstruction {
        class SourceProvider {
            + SourceHandle lower(SourceId, SourceDescriptor, BufferPool, EmitFunction)
        }

        class SourceRegistry {
            Source create(SourceDescriptor)
        }

        class SourceValidationRegistry {
            SourceDescriptor create(unordered_map~string, string~)
        }
    }

    namespace SourceInternals {
        class SourceThread {
            + void start()
            + void stop()
            + Source(File/TCP/..) sourceImpl
            - std::thread thread
            - void runningRoutine()
            - void emitWork()
        }
    %% Source is the interface for the PluginRegistry for sources
        class Source {
            + bool fillTupleBuffer(TupleBuffer)
            + void virtual open()
            + void virtual close()
        }

        class FileSource {
            + bool fillTupleBuffer(TupleBuffer)
            + void open()
            + void close()
            - std::string filePath
        }

        class TCPSource {
            + bool fillTupleBuffer(TupleBuffer)
            + void open()
            + void close()
            - string host
            - string port
        }
    }
```

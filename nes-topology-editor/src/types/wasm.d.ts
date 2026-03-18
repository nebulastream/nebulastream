/** Type declarations for the Emscripten-generated WASM validation module. */
declare module '*.mjs' {
  interface NesValidatorModule {
    /** Validate a NES topology YAML string. Returns empty string if valid, error message otherwise. */
    validateTopology(yaml: string): string;

    /** Returns JSON array of registered source type names. */
    getRegisteredSourceTypes(): string;

    /** Returns JSON array of registered sink type names. */
    getRegisteredSinkTypes(): string;

    /** Returns JSON array of config field metadata for a source type. */
    getSourceConfigFields(sourceType: string): string;

    /** Returns JSON array of config field metadata for a sink type. */
    getSinkConfigFields(sinkType: string): string;
  }

  /** Factory function that initializes the WASM module and returns a promise. */
  const createModule: () => Promise<NesValidatorModule>;
  export default createModule;
}

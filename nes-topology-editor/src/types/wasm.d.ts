/** Type declarations for the Emscripten-generated WASM validation module. */
declare module '*.mjs' {
  interface NesValidatorModule {
    /** Validate a NES topology YAML string. Returns empty string if valid, error message otherwise. */
    validateTopology(yaml: string): string;
  }

  /** Factory function that initializes the WASM module and returns a promise. */
  const createModule: () => Promise<NesValidatorModule>;
  export default createModule;
}

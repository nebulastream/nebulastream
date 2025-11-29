{
  lib,
  llvmPackages_19,
  abseil-cpp,
}:

let
  llvm = llvmPackages_19;
  clangStdenv = llvm.stdenv;
  libcxxStdenv = llvm.libcxxStdenv;

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
      compilerFlags ? [ ],
    }:
    let
      stdenv = if useLibcxx then libcxxStdenv else clangStdenv;
      base = abseil-cpp.override { inherit stdenv; };
      concatFlags = flags: if flags == [ ] then "" else lib.concatStringsSep " " flags;
      mergeFlags =
        flags: existing:
        let
          trailing = lib.optional (existing != null && existing != "") existing;
        in
        concatFlags (flags ++ trailing);
    in
    base.overrideAttrs (
      old:
      let
        oldEnv = old.env or { };
      in
      {
        buildInputs = lib.unique ((old.buildInputs or [ ]) ++ extraBuildInputs);
        env = oldEnv // {
          NIX_CFLAGS_COMPILE = mergeFlags compilerFlags (oldEnv.NIX_CFLAGS_COMPILE or "");
        };
      }
    );

  parseWithSanitizerArgs =
    arg:
    if builtins.isList arg then
      {
        extraBuildInputs = arg;
        useLibcxx = false;
        compilerFlags = [ ];
      }
    else if builtins.isAttrs arg then
      {
        extraBuildInputs =
          if arg ? extraBuildInputs then
            arg.extraBuildInputs
          else if arg ? extraPackages then
            arg.extraPackages
          else
            [ ];
        useLibcxx = arg.useLibcxx or false;
        compilerFlags = arg.compilerFlags or [ ];
      }
    else
      {
        extraBuildInputs = [ ];
        useLibcxx = false;
        compilerFlags = [ ];
      };

in
{
  default = build { };
  withSanitizer =
    arg:
    let
      cfg = parseWithSanitizerArgs arg;
    in
    build cfg;
}

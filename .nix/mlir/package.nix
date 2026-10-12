{
  pkgs,
  mlirVersion,
  mlirRelease,
  mlirAssetPrefix,
}:
let
  lib = pkgs.lib;
  stdenv = pkgs.stdenvNoCC;
  platform = pkgs.stdenv.hostPlatform;
  arch =
    if platform.isAarch64 then
      "arm64"
    else if platform.isx86_64 then
      "x64"
    else
      builtins.throw "Unsupported system: ${platform.system}";

  supportedSanitizers = [
    "none"
    "address"
    "thread"
    "undefined"
  ];
  supportedStdlibs = [
    "libcxx"
    "libstdcxx"
  ];

  hashes = {
    x64 = {
      none = {
        libcxx = "e5c602ca1682f9c5082babd602db4ab9dfe20469e353c3a9bd1778c7fc6d7b68";
        libstdcxx = "305b0732ebbc4068dbb7c8066c8d613b2bbd2daf9a91b1a82fc9bb1d1b8a9791";
      };
      address = {
        libcxx = "6975df27dbd1f93c5616e5140cbc669b6af3122d3b31ef047b8723cb0a38362a";
        libstdcxx = "5c6177d3c15a35a78cb0bff7a839f0e3ffeec326f312156463ad0219e0577d76";
      };
      thread = {
        libcxx = "6915cb6a608317e8d0a0ce612fbcb1c1bf7661250dae2f3fcef9c6a1b0f28aac";
        libstdcxx = "c66d01f0f07e863cad316bc25a88bb85922d6c242204ef5b47cadd02385e1eb4";
      };
      undefined = {
        libcxx = "8505009bd11ad06ad09774b72d7fb3dde723659584894843930bb250cf121a82";
        libstdcxx = "4ee6371d796a9955f09a003b0e702ae08dee2a781d1d450f17c1ada9cfee2317";
      };
    };
    arm64 = {
      none = {
        libcxx = "19c8f352522f33cb38191b676c6d998c222f19977fd7f13a083bc2cc0f77f992";
        libstdcxx = "1ce1a977b591866b306d25da30e72e80e9d1f48d74f35121815a42fc5ea53afe";
      };
      address = {
        libcxx = "b1fcb5d69ba3f2bf6dc51331ae065c40ff4c60ce9b27d745f22163d385e26b59";
        libstdcxx = "0284ec68d4f1b1a0b94cb7a094644942e74a4de9c1a4ac9c5a64c98d6ed4aead";
      };
      thread = {
        libcxx = "0c3f02a9adc130bfdcd4fcbf7f71e48f7bc8204c2dd66a4de6caae7b63130f9f";
        libstdcxx = "07c584ff853dadd67e16b1348034e3d3f2316c0061cedf2fce5b9ef99785efa0";
      };
      undefined = {
        libcxx = "0e1ae82c53670fc3f0434fa3e7f25b5d62df027a63f286f3fe68610878e9ac70";
        libstdcxx = "bd47ca6b55bb3f85b5ce3df6198f70be4a2787cee03624d3391d45fcfde5afd4";
      };
    };
  };

  sanitizeSelection =
    list: value:
    if lib.elem value list then value else builtins.throw "Unsupported selection '${value}'";

  hashFor =
    { sanitizer, stdlib }:
    let
      san = sanitizeSelection supportedSanitizers sanitizer;
      std = sanitizeSelection supportedStdlibs stdlib;
      archHashes = hashes.${arch};
    in
    archHashes.${san}.${std};

  mkMlirBinary =
    { sanitizer, stdlib }:
    let
      hash = hashFor { inherit sanitizer stdlib; };
      url = "https://github.com/nebulastream/clang-binaries/releases/download/${mlirRelease}/${mlirAssetPrefix}-${arch}-${sanitizer}-${stdlib}.tar.zstd";
    in
    stdenv.mkDerivation {
      pname = "nes-mlir";
      version = mlirVersion;

      src = pkgs.fetchurl {
        inherit url;
        sha256 = hash;
      };

      nativeBuildInputs = [
        pkgs.zstd
        pkgs.gnutar
      ];
      dontUnpack = true;
      strictDeps = true;

      installPhase = ''
        runHook preInstall
        mkdir -p "$TMPDIR/mlir"
        zstd -d "$src" --stdout | tar -xf - -C "$TMPDIR/mlir"
        mkdir -p "$out"
        cp -r "$TMPDIR/mlir"/clang/* "$out"/
        runHook postInstall
      '';

      meta = with lib; {
        description = "Prebuilt MLIR toolchain used by NebulaStream";
        license = licenses.asl20;
        platforms = platforms.linux;
      };
    };

  normalizeArgs =
    args:
    let
      sanitizer = args.sanitizer or "none";
      stdlib = args.stdlib or "libstdcxx";
    in
    {
      inherit sanitizer stdlib;
    };

in
{
  forOptions =
    args:
    let
      cfg = normalizeArgs args;
    in
    mkMlirBinary cfg;

  mlirBinary = mkMlirBinary {
    sanitizer = "none";
    stdlib = "libstdcxx";
  };
}

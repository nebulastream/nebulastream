{ fetchFromGitHub
, llvmPackages_19
, lib
, cmake
, nix-update-script
}:

let
stdenv = llvmPackages_19.stdenv;
in
stdenv.mkDerivation (finalAttrs: {
pname = "reflect-cpp";
version = "0.23.0";

src = fetchFromGitHub {
owner = "getml";
repo = "reflect-cpp";
rev = "v${finalAttrs.version}";
hash = "sha256-TzkNu0baAeV6BdELOzyTC4P6C7gVt7e+NiXa/NhWjpo=";
};

nativeBuildInputs = [ cmake ];

cmakeFlags = [
(lib.cmakeFeature "REFLECTCPP_INSTALL" "ON")
(lib.cmakeFeature "REFLECTCPP_BUILD_TESTS" "OFF")
(lib.cmakeFeature "REFLECTCPP_BUILD_BENCHMARKS" "OFF")
(lib.cmakeFeature "REFLECTCPP_CHECK_HEADERS" "OFF")
(lib.cmakeFeature "REFLECTCPP_USE_VCPKG" "OFF")
(lib.cmakeFeature "REFLECTCPP_USE_BUNDLED_DEPENDENCIES" "ON")

(lib.cmakeFeature "CMAKE_INSTALL_INCLUDEDIR" "include")
(lib.cmakeFeature "CMAKE_INSTALL_LIBDIR" "lib")
];

# If upstream has tests but you disabled building them anyway:
doCheck = false;

passthru.updateScript = nix-update-script { };

meta = {
description = "Reflect C++ library for C++20";
homepage = "https://github.com/getml/reflect-cpp";
changelog = "https://github.com/getml/reflect-cpp/releases/tag/v${finalAttrs.version}";
license = lib.licenses.mit;
platforms = lib.platforms.all;
};
})

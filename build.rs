use std::path::Path;

fn main() {
    // Build the native C++ library via CMake (Release profile by default).

        println!("cargo:rerun-if-changed=native/CMakeLists.txt");
        println!("cargo:rerun-if-changed=native/include/bsbe_bridge.h");
        println!("cargo:rerun-if-changed=native/src/bsbe_bridge.cc");
        println!("cargo:rerun-if-changed=native/generated");
        println!("cargo:rerun-if-changed=native/schemas/spot_sbe.xml");
        println!("cargo:rerun-if-changed=native/schemas/stream_1_0.xml");

        let need = [
            "native/include/spot_stream/MessageHeader.h",
            "native/include/spot_stream/TradesStreamEvent.h",
        ];

        if need.iter().any(|path| !Path::new(path).exists()) {
            panic!(concat!(
                "Missing SBE headers under native/include/spot_stream/*.h\n",
                "Generate with:\n",
                "  java -Dsbe.target.language=CPP -Dsbe.output.dir=native/include ",
                "-Dsbe.target.namespace=spot_stream native/schemas/spot_sbe.xml\n",
                "  java -Dsbe.target.language=CPP -Dsbe.output.dir=native/include ",
                "-Dsbe.target.namespace=spot_stream native/schemas/stream_1_0.xml\n",
                "(CI does this in 'Generate SBE headers (C++)')"
            ));
        }

        // Build the native C++ library via CMake (Release profile by default).
        let dst = cmake::Config::new("native")
            .profile("Release")
            .build();

        // Propagate link search paths and static library requirements to Rust.
        println!(
            "cargo:rustc-link-search=native={}",
            dst.join("lib").display()
        );
        println!("cargo:rustc-link-lib=static=bsbe_native");
        #[cfg(target_os = "linux")]
        println!("cargo:rustc-link-lib=dylib=stdc++");
        #[cfg(target_os = "macos")]
        println!("cargo:rustc-link-lib=dylib=c++");

    println!("cargo:rerun-if-changed=native/CMakeLists.txt");
    println!("cargo:rerun-if-changed=native/include/bsbe_bridge.h");
    println!("cargo:rerun-if-changed=native/src/bsbe_bridge.cc");
    println!("cargo:rerun-if-changed=native/generated");
    println!("cargo:rerun-if-changed=native/schemas/spot_sbe.xml");
}

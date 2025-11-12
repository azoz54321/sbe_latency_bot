use std::path::Path;

fn main() {
    let header = Path::new("native/include/spot_stream/BoolEnum.h");
    if !header.exists() {
        panic!(
            "Missing SBE headers at native/include/spot_stream/*.h \nGenerate them with:\n  java -jar .tools/sbe-all.jar -Dsbe.target.language=CPP -Dsbe.output.dir=native/include -Dsbe.target.namespace=spot_stream native/schemas/spot_sbe.xml \n(Our GitHub Actions runs this step automatically in 'Generate SBE headers (C++)')"
        );
    }

    let dst = cmake::Config::new("native").build();
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("lib").display()
    );
    println!("cargo:rustc-link-lib=static=bsbe_native");
    #[cfg(target_env = "gnu")]
    println!("cargo:rustc-link-lib=dylib=stdc++");
    println!("cargo:rerun-if-changed=native/CMakeLists.txt");
    println!("cargo:rerun-if-changed=native/include/bsbe_bridge.h");
    println!("cargo:rerun-if-changed=native/src/bsbe_bridge.cc");
    println!("cargo:rerun-if-changed=schemas/binance_sbe.xml");
    println!("cargo:rerun-if-changed=native/generated");
}

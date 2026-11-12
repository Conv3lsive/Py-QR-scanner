//! Сборка cdylib-расширения cargo'ом без maturin: аргументы линка только для
//! финальной dylib (не для proc-macro/зависимостей) и только на macOS.
fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "macos" {
        // Windows/Linux: pyo3 сам находит python-библиотеку (python3.lib на
        // Windows), специальные флаги не нужны — и вредны (LNK1181).
        return;
    }

    // Символы Python C API резолвятся в рантайме — модуль грузится в процесс
    // интерпретатора (эквивалент maturin/setuptools-rust).
    // `-undefined dynamic_lookup` — два отдельных аргумента линкера.
    println!("cargo:rustc-cdylib-link-arg=-undefined");
    println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    // Баг ld-27106 (новый линкер Apple) в классическом формате fixups: strtab
    // символов кладётся с 4-байтовым выравниванием, а dyld macOS 26+ требует
    // 8 байт ("mis-aligned LINKEDIT string pool"). Явный chained-fixups меняет
    // раскладку __LINKEDIT и обходит проблему.
    println!("cargo:rustc-cdylib-link-arg=-Wl,-fixup_chains");
}

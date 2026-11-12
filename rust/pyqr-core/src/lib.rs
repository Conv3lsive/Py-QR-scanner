use pyo3::prelude::*;

mod barcode;
mod csvm;
mod emailm;
mod fileops;
mod state;
mod zipm;

/// Пэд раскладки __LINKEDIT: длина имени этого экспортируемого символа
/// сдвигает экспорт-три/symtab так, что strtab (LC_SYMTAB.stroff) ложится
/// на 8-байтовую границу — новый dyld macOS 26+ иначе роняет dlopen
/// ("mis-aligned LINKEDIT string pool", баг ld-27106). Длину подбирает
/// rust/build.sh после сборки (проверка — rust/fix_linkedit.py --check).
#[no_mangle]
pub extern "C" fn pyqr_pad() {}

#[pymodule]
fn pyqr_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();

    let csv_mod = PyModule::new_bound(py, "csv")?;
    csvm::register(&csv_mod)?;
    m.add_submodule(&csv_mod)?;

    let barcode_mod = PyModule::new_bound(py, "barcode")?;
    barcode::register(&barcode_mod)?;
    m.add_submodule(&barcode_mod)?;

    let fileops_mod = PyModule::new_bound(py, "fileops")?;
    fileops::register(&fileops_mod)?;
    m.add_submodule(&fileops_mod)?;

    let zip_mod = PyModule::new_bound(py, "zip")?;
    zipm::register(&zip_mod)?;
    m.add_submodule(&zip_mod)?;

    let state_mod = PyModule::new_bound(py, "state")?;
    state::register(&state_mod)?;
    m.add_submodule(&state_mod)?;

    let email_mod = PyModule::new_bound(py, "email")?;
    emailm::register(&email_mod)?;
    m.add_submodule(&email_mod)?;

    Ok(())
}

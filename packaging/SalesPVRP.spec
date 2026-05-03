# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules


ROOT = Path(SPECPATH).resolve().parent

datas = [
    (str(ROOT / "config.yaml"), "."),
    (str(ROOT / "README_USER.md"), "."),
    (str(ROOT / "gui" / "resources"), "gui/resources"),
]

for workbook in [
    ROOT / "data" / "input_clients_template.xlsx",
    ROOT / "data" / "sample_clients.xlsx",
]:
    if workbook.exists():
        datas.append((str(workbook), "data"))

hiddenimports = []
for package in ["sklearn", "ortools", "yaml", "folium", "openpyxl"]:
    hiddenimports.extend(collect_submodules(package))

hiddenimports.extend(
    [
        "PySide6.QtCore",
        "PySide6.QtGui",
        "PySide6.QtWidgets",
    ]
)

a = Analysis(
    [str(ROOT / "run_gui.py")],
    pathex=[str(ROOT)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["pytest"],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="SalesPVRP",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="SalesPVRP",
)

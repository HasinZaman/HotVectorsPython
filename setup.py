import os
import subprocess
import shutil
from setuptools import setup
from setuptools.command.build_py import build_py as _build_py

class CustomBuildPy(_build_py):
    def run(self):
        # 1. Run Cargo build
        subprocess.run(["cargo", "build", "--release"], check=True)

        # 2. Copy DLL from target to root
        target_dir = os.path.join("target", "release")
        if os.name == "nt":
            dll_name = "rust_bytes_api.dll"
        elif os.name == "posix":
            dll_name = "librust_bytes_api.so"
        else:
            raise RuntimeError("Unsupported OS")

        src = os.path.join(target_dir, dll_name)
        dst = os.path.join(os.path.dirname(__file__), dll_name)

        print(f"Copying {src} → {dst}")
        shutil.copy(src, dst)

        # 3. Proceed with normal build
        super().run()

setup(
    name="hotvector",
    version="0.1.0",
    py_modules=["HotVector"],
    cmdclass={
        'build_py': CustomBuildPy,  # use our custom hook
    },
    data_files=[(".", ["rust_bytes_api.dll"])],  # include the DLL after it's copied
    zip_safe=False,
)

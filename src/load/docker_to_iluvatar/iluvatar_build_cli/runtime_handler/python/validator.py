import os
import importlib.util
import inspect

def validate_function_directory(function_dir: str) -> None:
    """
    Validate that the function directory:
      - Exists and is a directory.
      - Contains a main.py file.
    """
    if not os.path.isdir(function_dir):
        raise NotADirectoryError(f"{function_dir} is not a valid directory.")

    entry_point = os.path.join(function_dir, "main.py")
    if not os.path.exists(entry_point):
        raise FileNotFoundError("main.py not found in the function directory.")

    # Load the module from main.py.
    module_name = "main"
    spec = importlib.util.spec_from_file_location(module_name, entry_point)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "main"):
        raise ValueError("The entry point (main.py) does not define a main() function.")

    main_func = getattr(module, "main")
    if not callable(main_func):
        raise ValueError("main is not callable.")
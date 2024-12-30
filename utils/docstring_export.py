import inspect
import os
import importlib
import sys

# Добавляем путь к проекту
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_dir)

# Список модулей для обработки
modules = ["main", "config", "db_utils", "forecasting", "get_data", "model_fits", "models_processing", "scheduler"]

# Путь для сохранения файла на рабочий стол
output_file = os.path.join(os.path.expanduser("~"), "Desktop", "docstrings.txt")

# Открываем файл для записи
with open(output_file, "w", encoding="utf-8") as f:
    for module_name in modules:
        try:
            module = importlib.import_module(module_name)
            f.write(f"Module: {module_name}\n")
            f.write("=" * 40 + "\n")

            for name, obj in inspect.getmembers(module):
                if inspect.isfunction(obj) and obj.__module__ == module.__name__:
                    f.write(f"Function: {name}\n")
                    f.write(f"{'-' * 40}\n")
                    f.write(f"{obj.__doc__}\n\n")
        except ModuleNotFoundError:
            f.write(f"Module {module_name} not found.\n\n")

print(f"Докстринги сохранены в файл: {output_file}")

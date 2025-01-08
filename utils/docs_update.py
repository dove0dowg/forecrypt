import os
import subprocess
import sys

# Убедись, что используется Python из venv
python_executable = sys.executable

# Папка с документацией
docs_dir = r"C:\Users\user\Desktop\kode\forecrypt\docs"
index_file = os.path.join(docs_dir, "index.rst")

# Добавить вводное описание в index.rst
intro_text = """
Welcome to ForecrypT's Documentation!
======================================

ForecrypT is a powerful cryptocurrency forecasting tool that combines modern algorithms, real-time data, and seamless database integration. Here's what it can do for you:

- Predict cryptocurrency prices using ARIMA, ETS, and Theta models.
- Store and analyze historical and forecast data in PostgreSQL.
- Automatically update forecasts and retrain models on a schedule.(development)

"""

# Читаем файл index.rst
with open(index_file, "r", encoding="utf-8") as f:
    content = f.read()

# Проверяем, есть ли вводный текст
if intro_text.strip() not in content:
    # Дополняем index.rst только если текста ещё нет
    with open(index_file, "w", encoding="utf-8") as f:
        f.write(intro_text + content)

# Собрать HTML документацию
subprocess.run([
    python_executable, "-m", "sphinx",
    "-b", "html",
    docs_dir, os.path.join(docs_dir, "_build", "html")
])

print("Документация успешно собрана с маркетинговым описанием.")

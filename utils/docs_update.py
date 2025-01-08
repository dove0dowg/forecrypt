import os
import subprocess
import sys
import shutil

# Убедись, что используется Python из venv
python_executable = sys.executable

# Папка с документацией
docs_dir = r"C:\Users\user\Desktop\kode\forecrypt\docs"
build_dir = os.path.join(docs_dir, "_build", "html")
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


# Очистить папку _build/html перед сборкой новой документации
build_dir = os.path.join(docs_dir, "_build", "html")
if os.path.exists(build_dir):
    shutil.rmtree(build_dir)
    print("Папка _build/html очищена.")

# Собрать HTML документацию
subprocess.run([
    python_executable, "-m", "sphinx",
    "-b", "html",
    docs_dir, build_dir
])

print("Документация успешно собрана.")

# Очистить старые .html файлы в docs
for filename in os.listdir(docs_dir):
    if filename.endswith(".html"):
        file_path = os.path.join(docs_dir, filename)
        os.remove(file_path)

# Скопировать новые .html файлы из _build/html в docs
for root, _, files in os.walk(build_dir):
    for file in files:
        if file.endswith(".html"):
            src_path = os.path.join(root, file)
            rel_path = os.path.relpath(src_path, build_dir)
            dest_path = os.path.join(docs_dir, rel_path)

            # Создаем нужные папки в docs, если их нет
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)

            # Копируем файл
            shutil.copy2(src_path, dest_path)

print("HTML файлы успешно скопированы в папку docs.")

# Удалить старую папку static, если она существует
static_dest = os.path.join(docs_dir, "static")
if os.path.exists(static_dest):
    shutil.rmtree(static_dest)

# Копировать папку static
static_src = os.path.join(build_dir, "_static")
shutil.copytree(static_src, static_dest)

print("Папка static успешно скопирована.")

# Заменить ссылки на _static в .html файлах
for root, _, files in os.walk(docs_dir):
    for file in files:
        if file.endswith(".html"):
            file_path = os.path.join(root, file)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            content = content.replace("_static", "static")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)

print("Ссылки на static успешно обновлены.")


#import os
#import subprocess
#import sys
#
## Убедись, что используется Python из venv
#python_executable = sys.executable
#
## Папка с документацией
#docs_dir = r"C:\Users\user\Desktop\kode\forecrypt\docs"
#index_file = os.path.join(docs_dir, "index.rst")
#
## Добавить вводное описание в index.rst
#intro_text = """
#Welcome to ForecrypT's Documentation!
#======================================
#
#ForecrypT is a powerful cryptocurrency forecasting tool that combines modern algorithms, real-time data, and seamless database integration. Here's what it can do for you:
#
#- Predict cryptocurrency prices using ARIMA, ETS, and Theta models.
#- Store and analyze historical and forecast data in PostgreSQL.
#- Automatically update forecasts and retrain models on a schedule.(development)
#
#"""
#
## Читаем файл index.rst
#with open(index_file, "r", encoding="utf-8") as f:
#    content = f.read()
#
## Проверяем, есть ли вводный текст
#if intro_text.strip() not in content:
#    # Дополняем index.rst только если текста ещё нет
#    with open(index_file, "w", encoding="utf-8") as f:
#        f.write(intro_text + content)
#
## Собрать HTML документацию
#subprocess.run([
#    python_executable, "-m", "sphinx",
#    "-b", "html",
#    docs_dir, os.path.join(docs_dir, "_build", "html")
#])
#
#print("Документация успешно собрана с маркетинговым описанием.")

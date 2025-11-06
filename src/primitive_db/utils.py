import json
import os


# ---------------- Метаданные ---------------- #

def load_metadata(filepath="db_meta.json"):
    """Загружает метаданные всех таблиц."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        # Если файла нет — создаём базовую структуру
        data = {"tables": {}}
    return data


def save_metadata(data, filepath="db_meta.json"):
    """Сохраняет метаданные всех таблиц."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


# ---------------- Данные таблиц ---------------- #

def load_table_data(table_name):
    """Загружает данные конкретной таблицы."""
    filepath = os.path.join("data", f"{table_name}.json")
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def save_table_data(table_name, data):
    """Сохраняет данные конкретной таблицы."""
    os.makedirs("data", exist_ok=True)
    filepath = os.path.join("data", f"{table_name}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)



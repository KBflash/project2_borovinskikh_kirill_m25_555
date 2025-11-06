import os
from .utils import save_metadata, load_table_data, save_table_data
from .decorators import handle_db_errors, confirm_action, log_time


# ----------------- Создание таблицы ----------------- #
@handle_db_errors
def create_table(metadata, table_name, columns):
    """Создает таблицу в базе данных."""
    if table_name in metadata["tables"]:
        raise ValueError(f"Таблица '{table_name}' уже существует.")

    metadata["tables"][table_name] = {"columns": columns, "rows": 0}
    os.makedirs("data", exist_ok=True)
    save_table_data([], table_name)
    save_metadata(metadata)
    print(f"Таблица '{table_name}' успешно создана.")


# ----------------- Удаление таблицы ----------------- #
@confirm_action("удаление таблицы")
@handle_db_errors
def drop_table(metadata, table_name):
    """Удаляет таблицу."""
    if table_name not in metadata["tables"]:
        raise KeyError(table_name)

    os.remove(f"data/{table_name}.json")
    del metadata["tables"][table_name]
    save_metadata(metadata)
    print(f"Таблица '{table_name}' удалена.")


# ----------------- Добавление записи ----------------- #
@handle_db_errors
@log_time
def insert(metadata, table_name, values):
    """Добавляет запись в таблицу."""
    if table_name not in metadata["tables"]:
        raise KeyError(table_name)

    table_info = metadata["tables"][table_name]
    columns = table_info["columns"]

    if len(values) != len(columns):
        raise ValueError("Количество значений не совпадает с количеством столбцов.")

    rows = load_table_data(table_name)
    row_id = len(rows) + 1
    record = {"ID": row_id}
    for (col_name, col_type), value in zip(columns.items(), values):
        if col_type == "int":
            value = int(value)
        elif col_type == "bool":
            value = str(value).lower() in ("true", "1", "yes")
        record[col_name] = value

    rows.append(record)
    save_table_data(rows, table_name)
    metadata["tables"][table_name]["rows"] += 1
    save_metadata(metadata)
    print(f"Запись с ID={row_id} успешно добавлена в таблицу '{table_name}'.")


# ----------------- Чтение записей ----------------- #
@handle_db_errors
@log_time
def select(metadata, table_name, condition=None):
    """Выводит записи таблицы с фильтрацией."""
    from .decorators import create_cacher
    cache = create_cacher()

    def read_data():
        rows = load_table_data(table_name)
        if condition:
            column, value = condition
            return [r for r in rows if str(r.get(column)) == str(value)]
        return rows

    rows = cache(f"{table_name}:{condition}", read_data)

    if not rows:
        print("Нет данных.")
        return

    columns = ["ID"] + list(metadata["tables"][table_name]["columns"].keys())
    widths = [max(len(str(row.get(col, ""))) for row in rows + [{col: col}]) for col in columns]

    header = " | ".join(col.ljust(w) for col, w in zip(columns, widths))
    print("+" + "+".join("-" * (w + 2) for w in widths) + "+")
    print("| " + header + " |")
    print("+" + "+".join("-" * (w + 2) for w in widths) + "+")
    for row in rows:
        print("| " + " | ".join(str(row.get(col, "")).ljust(w) for col, w in zip(columns, widths)) + " |")
    print("+" + "+".join("-" * (w + 2) for w in widths) + "+")


# ----------------- Обновление записей ----------------- #
@handle_db_errors
def update(metadata, table_name, set_col, new_value, where_col, where_value):
    """Обновляет записи."""
    if table_name not in metadata["tables"]:
        raise KeyError(table_name)

    rows = load_table_data(table_name)
    updated = 0
    for row in rows:
        if str(row.get(where_col)) == str(where_value):
            row[set_col] = new_value
            updated += 1

    save_table_data(rows, table_name)
    print(f"Обновлено записей: {updated}")


# ----------------- Удаление записей ----------------- #
@confirm_action("удаление записей")
@handle_db_errors
def delete(metadata, table_name, where_col, where_value):
    """Удаляет записи по условию."""
    if table_name not in metadata["tables"]:
        raise KeyError(table_name)

    rows = load_table_data(table_name)
    new_rows = [r for r in rows if str(r.get(where_col)) != str(where_value)]
    save_table_data(new_rows, table_name)
    metadata["tables"][table_name]["rows"] = len(new_rows)
    save_metadata(metadata)
    print(f"Удалено записей: {len(rows) - len(new_rows)}")


# ----------------- Информация о таблице ----------------- #
@handle_db_errors
def info(metadata, table_name):
    """Выводит информацию о таблице."""
    if table_name not in metadata["tables"]:
        raise KeyError(table_name)

    table = metadata["tables"][table_name]
    print(f"Таблица: {table_name}")
    print("Столбцы:")
    for col, typ in table["columns"].items():
        print(f"  - {col}: {typ}")
    print(f"Количество записей: {table['rows']}")


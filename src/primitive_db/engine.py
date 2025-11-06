import re
from .core import create_table, drop_table, insert, select, update, delete, info
from .utils import load_metadata


def run():
    print("\n*** База данных ***\n")

    metadata = load_metadata()

    print("\n***Операции с данными***\n")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print()
    print("<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...) - создать запись")
    print("<command> select from <имя_таблицы> where <столбец> = <значение> - прочитать записи по условию")
    print("<command> select from <имя_таблицы> - прочитать все записи")
    print("<command> update <имя_таблицы> set <столбец1> = <новое_значение1> where <столбец_условия> = <значение_условия> - обновить записи")
    print("<command> delete from <имя_таблицы> where <столбец> = <значение> - удалить записи")
    print("<command> info <имя_таблицы> - информация о таблице")
    print()
    print("Общие команды:")
    print("<command> help - справочная информация")
    print("<command> exit - выход из программы")

    while True:
        command = input("\nВведите команду: ").strip()

        if not command:
            continue

        if command == "exit":
            print("До встречи!")
            break

        elif command == "help":
            print("Команды описаны выше.")
            continue

        elif command == "list_tables":
            print("Существующие таблицы:")
            for name in metadata["tables"]:
                print(f" - {name}")
            continue

        elif command.startswith("create_table"):
            parts = command.split()
            table_name = parts[1]
            columns = {}
            for col in parts[2:]:
                name, typ = col.split(":")
                columns[name] = typ
            create_table(metadata, table_name, columns)

        elif command.startswith("drop_table"):
            _, table_name = command.split(maxsplit=1)
            drop_table(metadata, table_name)

        elif command.startswith("insert into"):
            match = re.match(r"insert into (\w+) values \((.+)\)", command)
            if not match:
                print("Ошибка: неверный синтаксис insert.")
                continue
            table_name, values_str = match.groups()
            values = [v.strip().strip('"').strip("'") for v in values_str.split(",")]
            insert(metadata, table_name, values)

        elif command.startswith("select from"):
            if "where" in command:
                match = re.match(r"select from (\w+) where (\w+) = (.+)", command)
                if not match:
                    print("Ошибка: неверный синтаксис select.")
                    continue
                table_name, col, val = match.groups()
                val = val.strip().strip('"').strip("'")
                select(metadata, table_name, (col, val))
            else:
                _, _, table_name = command.split()
                select(metadata, table_name)

        elif command.startswith("update"):
            match = re.match(
                r"update (\w+) set (\w+) = (.+) where (\w+) = (.+)", command
            )
            if not match:
                print("Ошибка: неверный синтаксис update.")
                continue
            table_name, set_col, new_val, where_col, where_val = match.groups()
            update(metadata, table_name, set_col, new_val.strip('"').strip("'"),
                   where_col, where_val.strip('"').strip("'"))

        elif command.startswith("delete from"):
            match = re.match(r"delete from (\w+) where (\w+) = (.+)", command)
            if not match:
                print("Ошибка: неверный синтаксис delete.")
                continue
            table_name, col, val = match.groups()
            delete(metadata, table_name, col, val.strip('"').strip("'"))

        elif command.startswith("info"):
            _, table_name = command.split(maxsplit=1)
            info(metadata, table_name)

        else:
            print("Неизвестная команда. Введите help для справки.")





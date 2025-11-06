import time
from functools import wraps


def handle_db_errors(func):
    """Декоратор для централизованной обработки ошибок."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print("Ошибка: Файл данных не найден. Возможно, база данных не инициализирована.")
        except KeyError as e:
            print(f"Ошибка: Таблица или столбец {e} не найден.")
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
    return wrapper


def confirm_action(action_name):
    """Фабрика-декоратор для подтверждения действий пользователя."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            answer = input(f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: ')
            if answer.lower() != 'y':
                print("Операция отменена пользователем.")
                return
            return func(*args, **kwargs)
        return wrapper
    return decorator


def log_time(func):
    """Декоратор для измерения времени выполнения функции."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        end = time.monotonic()
        print(f"Функция {func.__name__} выполнилась за {end - start:.3f} секунд.")
        return result
    return wrapper


def create_cacher():
    """Создает функцию для кэширования результатов."""
    cache = {}

    def cache_result(key, value_func):
        if key in cache:
            print(f"[cache] Используется кэш для запроса: {key}")
            return cache[key]
        result = value_func()
        cache[key] = result
        print(f"[cache] Результат сохранён для ключа: {key}")
        return result

    return cache_result

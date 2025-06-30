import sys
import pandas as pd
from dbf import Table, READ_WRITE

path = sys.argv[1]
def excel_to_dbf(excel_path, dbf_path=None):
    if dbf_path is None:
        if excel_path.endswith('.xlsx'):
            dbf_path = excel_path[:-5] + '.dbf'
        else:
            dbf_path = excel_path + '.dbf'
    else:
        if dbf_path.endswith('.dbf') is False:
            dbf_path += '.dbf'

    df = pd.read_excel(excel_path)
    df = df.dropna(axis=1, how='all')  # Удаляем полностью пустые столбцы

    field_limits = {}
    dbf_structure = []
    max_number_size = 1e20  # Максимальное допустимое число в DBF

    for col in df.columns:
        dtype = df[col].dtype

        if dtype == "object":
            # Обработка строковых полей
            non_null = df[col].dropna()
            field_len = 1 if non_null.empty else min(254, int(non_null.str.len().max()))
            dbf_structure.append(f"{col} C({field_len})")
            field_limits[col] = {'type': 'C', 'max_len': field_len}

        elif dtype in ("int64", "int32", "float64", "float32"):
            # Обработка числовых полей
            non_null = df[col].dropna()

            if non_null.empty:
                int_len, dec_len = 10, 0
            else:
                max_val = non_null.abs().max()

                # Проверяем, не превышает ли число максимально допустимое значение
                if max_val > max_number_size:
                    # Для слишком больших чисел создаем поле стандартного размера
                    field_spec = f"{col} N(20,0)"
                    dbf_structure.append(field_spec)
                    field_limits[col] = {
                        'type': 'N',
                        'max_allowed': max_number_size,
                        'overflow_action': 'null',  # или 'zero'
                    }
                else:
                    # Для чисел в допустимом диапазоне сохраняем дробную часть
                    int_len = min(18, len(str(int(max_val)))) if not pd.isna(max_val) else 10

                    if dtype in ("int64", "int32"):
                        dec_len = 0
                        field_spec = f"{col} N({int_len},0)"
                        dbf_structure.append(field_spec)
                    elif dtype in ("float64", "float32"):
                        dec_len = min(4, max(2, len(str(non_null.iloc[0]).split('.')[1]) if '.' in str(non_null.iloc[0]) else 2))
                        field_spec = f"{col} N({int_len + dec_len + 1},{dec_len})"
                        dbf_structure.append(field_spec)
                    field_limits[col] = {
                        'type': 'N',
                        'int_len': int_len,
                        'dec_len': dec_len,
                        'max_allowed': max_number_size,
                        'overflow_action': 'null'  # или 'zero'
                    }

        elif "datetime" in str(dtype):
            dbf_structure.append(f"{col} D")
            field_limits[col] = {'type': 'D'}

        elif dtype == "bool":
            dbf_structure.append(f"{col} L")
            field_limits[col] = {'type': 'L'}

    # Создание и заполнение DBF таблицы
    try:
        table = Table(
            f"{dbf_path}",
            field_specs=";".join(dbf_structure),
            codepage="cp1251"
        )

        with table.open(mode=READ_WRITE):
            for _, row in df.iterrows():
                processed_row = []
                for col in df.columns:
                    value = row[col]
                    limits = field_limits.get(col, {})
                    if pd.isna(value):
                        processed_row.append(None)
                    elif limits.get('type') == 'C':
                        processed_row.append(str(value)[:limits.get('max_len', 254)])
                    elif limits.get('type') == 'N':
                        try:
                            if pd.isna(value):
                                processed_row.append(None)
                            else:
                                num = float(value)
                                # Проверяем на слишком большие числа
                                if abs(num) > limits.get('max_allowed', max_number_size):
                                    if limits.get('overflow_action') == 'zero':
                                        processed_row.append(0)
                                    else:
                                        processed_row.append(None)
                                else:
                                    # Сохраняем дробную часть, если поле не помечено как целочисленное
                                    processed_row.append(num)
                        except (ValueError, TypeError):
                            processed_row.append(0)
                    elif limits.get('type') == 'D':
                        processed_row.append(value.date() if hasattr(value, 'date') else value)
                    elif limits.get('type') == 'L':
                        processed_row.append(bool(value))
                    else:
                        processed_row.append(str(value)[:254])

                table.append(tuple(processed_row))

            print(f"Файл успешно конвертирован в {dbf_path}")

    except Exception as e:
        print(f"Ошибка при создании DBF файла: {str(e)}")
        if 'table' in locals():
            print("Количество полей в DBF:", len(table.field_names))
        print("Количество столбцов в DataFrame:", len(df.columns))
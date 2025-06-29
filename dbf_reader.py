import sys
from dbf import Table, READ_ONLY


def print_dbf(filepath):
    try:
        # Открываем DBF-файл в режиме только для чтения
        table = Table(filename=filepath)
        table.open(mode=READ_ONLY)  # 'ro' - read only
        
        # Выводим заголовок с информацией о файле
        print(f"\nDBF File: {filepath}")
        print(f"Records: {len(table)}")
        print(f"Fields: {len(table.field_names)}")
        print("-" * 80)
        
        # Выводим названия полей
        header = "|".join(f"{name:<15}" for name in table.field_names)
        print(header)
        print("-" * len(header))
        
        # Выводим данные построчно
        for record in table:
            row = "|".join(f"{str(value):<15}"[:15] for value in record)
            print(row)
            
    except Exception as e:
        print(f"Error reading DBF file: {e}")
    finally:
        if 'table' in locals():
            table.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python dbf_viewer.py <path_to_dbf_file>")
        sys.exit(1)

    dbf_file = sys.argv[1]
    print_dbf(dbf_file)
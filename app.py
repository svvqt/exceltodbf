from service.exceltodbf import excel_to_dbf
import sys

if __name__ == "__main__":
    excelpath = sys.argv[1]
    if len(sys.argv) not in (2, 3):
        print("Использвание: python app.py <excelfile.xlsx> [output.dbf]")
        sys.exit(1)
    if sys.argv[1].endswith('.xlsx') is False:
        sys.argv[1] += '.xlsx'
    excel_to_dbf(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)

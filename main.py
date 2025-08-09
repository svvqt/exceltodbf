from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import os
from service.exceltodbf import excel_to_dbf
import uuid
import logging
from typing import Optional

app = FastAPI()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Подключаем статические файлы
app.mount("/static", StaticFiles(directory="static"), name="static")

# Папка для загрузок
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Глобальная переменная для хранения пути к последнему созданному DBF файлу
last_dbf_path: Optional[str] = None

@app.get("/")
async def main():
    return FileResponse("static/index.html")

@app.post("/convert/")
async def convert_excel_to_dbf(file: UploadFile = File(...)):
    global last_dbf_path
    
    file_id = str(uuid.uuid4())
    excel_path = os.path.join(UPLOAD_DIR, f"{file_id}.xlsx")
    dbf_path = os.path.join(UPLOAD_DIR, f"{file_id}.dbf")
    
    try:
        # Удаляем предыдущий DBF файл, если он существует
        if last_dbf_path and os.path.exists(last_dbf_path):
            try:
                os.remove(last_dbf_path)
                logger.info(f"Удален предыдущий временный файл {last_dbf_path}")
            except Exception as e:
                logger.warning(f"Не удалось удалить предыдущий файл {last_dbf_path}: {str(e)}")
        
        # Сохраняем загруженный файл
        logger.info(f"Сохранение файла {file.filename} как {excel_path}")
        with open(excel_path, "wb") as buffer:
            buffer.write(await file.read())
        
        # Конвертируем
        logger.info(f"Конвертация {excel_path} в {dbf_path}")
        excel_to_dbf(excel_path, dbf_path)
        
        # Проверяем, что файл был создан
        if not os.path.exists(dbf_path):
            error_msg = f"Файл {dbf_path} не был создан в процессе конвертации"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        
        # Удаляем временный Excel файл
        try:
            os.remove(excel_path)
            logger.info(f"Удален временный файл {excel_path}")
        except Exception as e:
            logger.warning(f"Не удалось удалить файл {excel_path}: {str(e)}")
        
        # Сохраняем путь к DBF файлу для последующего удаления
        last_dbf_path = dbf_path
        
        logger.info(f"Конвертация успешно завершена, возвращаем файл {dbf_path}")
        return FileResponse(
            dbf_path,
            filename="converted.dbf",
            media_type="application/x-dbf"
        )
        
    except Exception as e:
        error_msg = f"Ошибка при конвертации: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)

@app.on_event("shutdown")
def cleanup():
    """Удаление временных файлов при завершении работы приложения"""
    global last_dbf_path
    if last_dbf_path and os.path.exists(last_dbf_path):
        try:
            os.remove(last_dbf_path)
            logger.info(f"Удален временный файл {last_dbf_path} при завершении работы")
        except Exception as e:
            logger.warning(f"Не удалось удалить файл {last_dbf_path}: {str(e)}")
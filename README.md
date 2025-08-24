# PythonQrscanner

Утилита для массовой обработки сканов работ с QR-кодами, нужна для автоматизированного распределения файлов по студентам, архивации и рассылки по email.

## Возможности
- Переименование файлов по QR-кодам
- Перенос файлов по CSV-таблице
- Архивация работ студентов
- Массовая рассылка архивов по email
- Проверка валидности email
- Параллельная обработка с помощью опции `--threads`

## Быстрый старт
1. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
2. Запустите скрипт с нужными параметрами:
   ```bash
   python main.py --action 1 --image-folder ./images --csv-path ./students.csv --name-fields Фамилия Имя --output-folder ./output --threads 6
   ```

## Аргументы командной строки
- `--action` — режим работы (0: переименование, 1: перенос по CSV, 2: архивировать, 3: email-рассылка, 4: проверка email)
- `--image-folder` — папка с изображениями
- `--csv-path` — путь к CSV-файлу
- `--name-fields` — поля ФИО в CSV
- `--output-folder` — папка для вывода
- `--move-mode` — copy или move
- `--threads` — количество потоков/процессов (по умолчанию 6)

## Конфигурация SMTP
Укажите свои данные в `sendconfig.py`:
```
SMTP_EMAIL = "your@email.com"
SMTP_PASSWORD = "your_password"
SMTP_HOST = "smtp.example.com"
SMTP_PORT = 465
```


import argparse
import logging
from barcode_utils import find_barcodes, file_renamer
from barcode_utils import split_by_student_folders
from csv_utils import read_csv
from file_utils import move_files, move_clear, move_unfound


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=level
    )

def main():
    parser = argparse.ArgumentParser(description="Обработка изображений с баркодами.")
    parser.add_argument('--image-folder', required=False, help='Путь к папке с изображениями')
    parser.add_argument('--action', type=int, choices=[0, 1, 2, 3], required=True,
                        help='0 — переименование, 1 — перенос по CSV, 2 — архивировать, 3 — рассылка по email')
    parser.add_argument('--csv-path', help='Путь к CSV (для action=1)')
    parser.add_argument('--name-fields', nargs='*', help='Названия колонок ФИО через пробел (для action=1)')
    parser.add_argument('--code-field', default='код', help="Название колонки с кодами (по умолчанию 'код')")
    parser.add_argument('--output-folder', help='Папка для вывода (для action=1)')
    parser.add_argument('--move-mode', choices=['copy', 'move'], default='copy', help='copy или move (для action=1)')
    parser.add_argument('--log-level', default='INFO', help='Уровень логирования (DEBUG, INFO, WARNING, ERROR)')
    args = parser.parse_args()

    setup_logging(getattr(logging, args.log_level.upper(), logging.INFO))

    logging.info("Поиск баркодов...")
    barcodes = find_barcodes(args.image_folder)
    if args.action == 0:
        if not args.image_folder:
            parser.error("Для action=0 и 1 требуется --image-folder")
        file_renamer(args.image_folder, barcodes)
        logging.info("Готово! Файлы переименованы.")
    elif args.action == 1:
        if not args.image_folder:
            parser.error("Для action=0 и 1 требуется --image-folder")
        if not all([args.csv_path, args.name_fields, args.output_folder]):
            parser.error("Для action=1 нужны --csv-path, --name-fields и --output-folder")
        data = read_csv(args.csv_path, args.code_field, args.name_fields)
        # move_clear(args.output_folder, args.image_folder, [p for v in barcodes.values() for p in v], args.move_mode)
        # move_unfound(barcodes, data, args.output_folder, args.move_mode)
        split_by_student_folders(barcodes, data, args.output_folder)
        logging.info("Готово! Все файлы обработаны.")
    elif args.action == 2:
        if not args.output_folder:
            parser.error("Для action=2 требуется --output-folder")
        from zip_utils import zip_student_folders
        zip_student_folders(args.output_folder)
        logging.info("Архивирование завершено.")
    elif args.action == 3:
        if not args.csv_path or not args.output_folder or not args.name_fields:
            parser.error("Для action=3 нужны --csv-path, --output-folder и --name-fields")
        from email_utils import send_email_smtp
        from zip_utils import zip_student_folders
        from csv_utils import read_csv_with_email

        archives = zip_student_folders(args.output_folder)
        _, emails = read_csv_with_email(args.csv_path, args.code_field, args.name_fields)

        for student, zip_path in archives.items():
            recipient = emails.get(student)
            if recipient:
                send_email_smtp(recipient, "Ваша работа", "Пожалуйста, проверьте архив", zip_path)
            else:
                logging.warning(f"Email не найден для {student}")

if __name__ == '__main__':
    main()
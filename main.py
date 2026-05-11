
import argparse
import logging
import os
import time


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=level
    )

def _run_watch_mode(args):
    from pipeline import WATCH_EXTENSIONS
    from processing_state import ProcessingState
    from watch_mode import FolderWatcherService

    if not all([args.image_folder, args.csv_path, args.name_fields, args.output_folder]):
        raise ValueError('Для action=5 нужны --image-folder, --csv-path, --name-fields и --output-folder')

    os.makedirs(args.output_folder, exist_ok=True)
    state_path = os.path.join(args.output_folder, 'state', 'processed.sqlite')
    state = ProcessingState(state_path)

    service = FolderWatcherService(
        input_folder=args.image_folder,
        csv_path=args.csv_path,
        name_fields=args.name_fields,
        output_folder=args.output_folder,
        code_field=args.code_field,
        csv_delimiter=args.csv_delimiter,
        move_mode=args.move_mode,
        threads=args.threads,
        state=state,
        debounce_sec=args.watch_debounce_sec,
        stable_checks=args.watch_stable_checks,
        stable_interval=args.watch_stable_interval,
        poll_interval=args.watch_poll_interval,
        process_existing_on_start=args.watch_process_existing,
        requeue_unstable=not args.watch_no_requeue_unstable,
        detailed_stats=args.watch_detailed_stats,
        sample_limit=args.watch_sample_limit,
        on_stats=lambda stats: logging.info('Статистика наблюдения: %s', stats),
    )
    service.start()
    logging.info('Watcher запущен. Ожидание файлов с расширениями: %s', ', '.join(sorted(WATCH_EXTENSIONS)))
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info('Остановка watcher...')
        service.stop()

def main():

    parser = argparse.ArgumentParser(description="Обработка изображений с баркодами.")
    parser.add_argument('--image-folder', required=False, help='Путь к папке с изображениями')
    parser.add_argument('--action', type=int, choices=[0, 1, 2, 3, 4, 5], required=False,
                        help='0 — переименование, 1 — перенос по CSV, 2 — архивировать, 3 — рассылка по email, 4 — проверка email, 5 — watch mode')
    parser.add_argument('--csv-path', help='Путь к CSV (для action=1)')
    parser.add_argument('--name-fields', nargs='*', help='Названия колонок ФИО через пробел (для action=1)')
    parser.add_argument('--code-field', default='код', help="Название колонки с кодами (по умолчанию 'код')")
    parser.add_argument('--email-field', default='email', help="Название колонки с email (по умолчанию 'email')")
    parser.add_argument('--csv-delimiter', default='auto', help="Разделитель CSV: auto, ';', ',', '\\t' (по умолчанию auto)")
    parser.add_argument('--output-folder', help='Папка для вывода (для action=1)')
    parser.add_argument('--move-mode', choices=['copy', 'move'], default='copy', help='copy или move (для action=1)')
    parser.add_argument('--log-level', default='INFO', help='Уровень логирования (DEBUG, INFO, WARNING, ERROR)')
    parser.add_argument('--threads', type=int, default=6, help='Количество потоков/процессов для параллельной обработки (по умолчанию 6)')
    parser.add_argument('--gui', action='store_true', help='Запустить графический интерфейс')
    parser.add_argument('--watch-debounce-sec', type=float, default=2.0, help='Watcher: задержка перед обработкой нового файла (сек)')
    parser.add_argument('--watch-stable-checks', type=int, default=2, help='Watcher: число проверок стабильности файла')
    parser.add_argument('--watch-stable-interval', type=float, default=1.0, help='Watcher: интервал между проверками стабильности (сек)')
    parser.add_argument('--watch-poll-interval', type=float, default=0.5, help='Watcher: период опроса очереди (сек)')
    parser.add_argument('--watch-process-existing', action='store_true', help='Watcher: обработать уже существующие файлы при запуске')
    parser.add_argument('--watch-no-requeue-unstable', action='store_true', help='Watcher: не возвращать нестабильные файлы обратно в очередь')
    parser.add_argument('--watch-detailed-stats', action='store_true', help='Watcher: подробная статистика batch в логах')
    parser.add_argument('--watch-sample-limit', type=int, default=5, help='Watcher: сколько имён файлов показывать в sample-логах')
    args = parser.parse_args()

    setup_logging(getattr(logging, args.log_level.upper(), logging.INFO))

    if args.gui:
        from gui_app import run_gui

        run_gui()
        return

    if args.action is None:
        parser.error('Укажите --action или используйте --gui')

    try:
        if args.action == 5:
            _run_watch_mode(args)
        else:
            from pipeline import run_action

            result = run_action(
                action=args.action,
                image_folder=args.image_folder,
                csv_path=args.csv_path,
                name_fields=args.name_fields,
                code_field=args.code_field,
                email_field=args.email_field,
                csv_delimiter=args.csv_delimiter,
                output_folder=args.output_folder,
                move_mode=args.move_mode,
                threads=args.threads,
            )
            logging.info('Результат: %s', result)
    except ValueError as e:
        parser.error(str(e))


if __name__ == '__main__':
    main()
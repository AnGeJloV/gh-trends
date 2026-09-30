# Скачивание hourly-архивов GH Archive: https://data.gharchive.org/YYYY-MM-DD-H.json.gz
# По умолчанию качает 3 дневных часа (16:00-18:00 UTC — пик активности) с 2026-09-20:
#   python scripts/download_data.py
# ВАЖНО: 2026-09-01 аномально пуст (1,4 МБ вместо обычных ~18 МБ) — не берите его для тестов!
# N часов подряд с указанного часа UTC:
#   python scripts/download_data.py --hours 24 --hour 0
# Произвольный период (включительно, целые дни с 00:00):
#   python scripts/download_data.py --from 2026-09-01 --to 2026-09-15
# Файлы кладутся в data/raw/YYYY-MM-DD-H.json.gz. Уже скачанные пропускаются (можно докачать повторным запуском).

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry

BASE_URL = "https://data.gharchive.org"


def make_session() -> requests.Session:
    """Сессия с авто-повторами: 3 попытки при обрыве сети / ошибках 429, 500+."""
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def iter_hours(from_day: date, to_day: date):
    """Все даты и часы диапазона включительно."""
    day = from_day
    while day <= to_day:
        for hour in range(24):
            yield day, hour
        day += timedelta(days=1)


def iter_hours_count(from_day: date, hours: int, start_hour: int):
    """N часов подряд, начиная с указанного часа UTC (с переходом через полночь)."""
    day, hour = from_day, start_hour
    for _ in range(hours):
        yield day, hour
        hour += 1
        if hour == 24:
            hour = 0
            day += timedelta(days=1)


def download(session: requests.Session, url: str, dest: Path) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    with session.get(url, timeout=60, stream=True) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length", 0))
        done = 0
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                done += len(chunk)
                if total:
                    pct = done * 100 // total
                    print(f"\r  {pct:3d}%  {done / 1e6:7.1f} MB", end="", flush=True)
    tmp.rename(dest)


def main() -> None:
    p = argparse.ArgumentParser(description="Скачивает архивы GH Archive. Без аргументов — 3 дневных часа с 2026-09-20")
    p.add_argument("--from", dest="from_", default="2026-09-20", help="начало, YYYY-MM-DD (по умолчанию 2026-09-20; период проекта — с 2026-09-01, но этот день почти пуст)")
    p.add_argument("--to", dest="to_", default=None, help="конец, YYYY-MM-DD (включительно); если не указан — используем --hours")
    p.add_argument("--hours", type=int, default=3, help="сколько часов качать, если --to не задан (по умолчанию 3)")
    p.add_argument("--hour", type=int, default=16, help="стартовый час UTC (по умолчанию 16 — дневной пик активности)")
    p.add_argument("--out", default="data/raw", help="куда складывать (по умолчанию data/raw)")
    args = p.parse_args()

    from_day = date.fromisoformat(args.from_)
    if args.hours < 1 or not 0 <= args.hour <= 23:
        sys.exit("Ошибка: --hours должен быть >= 1, --hour — от 0 до 23")

    if args.to_ is not None:
        to_day = date.fromisoformat(args.to_)
        if from_day > to_day:
            sys.exit("Ошибка: --from позже --to")
        hour_iter = iter_hours(from_day, to_day)
    else:
        hour_iter = iter_hours_count(from_day, args.hours, args.hour)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    session = make_session()
    total = 0
    skipped = 0
    failed = 0
    for day, hour in hour_iter:
        name = f"{day.isoformat()}-{hour}.json.gz"
        dest = out_dir / name
        url = f"{BASE_URL}/{name}"
        if dest.exists():
            skipped += 1
            continue
        print(f"{name} <- {url}")
        try:
            download(session, url, dest)
            total += 1
        except Exception as e:
            failed += 1
            print(f"  ОШИБКА: {e} (файл пропущен, перезапусти скрипт — докачает)")

    print(f"\nГотово. Скачано: {total}, уже было: {skipped}, ошибок: {failed}.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

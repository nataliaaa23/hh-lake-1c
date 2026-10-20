# Анализ ИТ-вакансий (вариант 1: Программист, разработчик на 1С)

Практическая работа № 3 по дисциплине «Инженерия данных».

Выполнила: Андреева Наталия Леонидовна, группа J3310.

## Что сделано

Вакансии варианта лежат в объектном хранилище RustFS (бакет hh-lake, объект raw/it_vacancies_2024.csv). Частоты навыков, пар и троек посчитаны двумя способами: собственным MapReduce с чтением из S3 чанками по 8 МиБ и SQL-запросами DuckDB. Результаты обоих способов совпали полностью. Дополнительно сделаны анализ влияния навыков на медианную зарплату, сравнение трёх регионов, профилирование времени и памяти и оптимизация MapReduce.

## Условие варианта

```
role = 'Программист, разработчик'
AND regexp_matches(lower(name), '(^|[^0-9])1\s?[cс]')
AND NOT (lower(name) LIKE '%битрикс%' OR lower(name) LIKE '%bitrix%')
```

Вакансий варианта: 20 953, из них с навыками: 13 452.

## Структура

- `src/config.py` — настройки подключения и условие варианта
- `src/test_s3.py` — проверка чтения из хранилища (boto3, DuckDB)
- `src/mapreduce.py` — собственный MapReduce (чтение чанками, повтор упавшего чанка)
- `src/mapreduce_opt.py` — оптимизированная версия (параллельная загрузка чанков)
- `src/duckdb_skills.py` — подсчёт в DuckDB
- `src/verify.py`, `src/verify_opt.py` — сверка результатов
- `src/salary.py`, `src/salary2.py`, `src/salary_check.py`, `src/salary3.py` — анализ зарплат
- `src/regions.py` — сравнение регионов
- `src/profile.py`, `src/bench_io.py` — профилирование и замер скорости чтения
- `results/` — сводки и таблицы; полные списки навыков, пар и троек (skills_*.csv) лежат в бакете hh-lake
- `report/otchet_dz3.pdf` — отчёт

## Запуск

Нужны Docker и запущенный RustFS (S3 на порту 9100, ключи rustfsadmin) с бакетом hh-lake и объектом raw/it_vacancies_2024.csv.

```
docker build -t hhlake .
docker run --rm -v ${PWD}:/app hhlake python src/mapreduce.py
docker run --rm -v ${PWD}:/app hhlake python src/mapreduce.py --fail-chunk 5 --no-upload
docker run --rm -v ${PWD}:/app hhlake python src/duckdb_skills.py
docker run --rm -v ${PWD}:/app hhlake python src/verify.py
docker run --rm -v ${PWD}:/app hhlake python src/mapreduce_opt.py
docker run --rm -v ${PWD}:/app hhlake python src/verify_opt.py
docker run --rm -v ${PWD}:/app hhlake python src/salary3.py
docker run --rm -v ${PWD}:/app hhlake python src/regions.py
```

## Основные результаты

| Показатель | Значение |
| --- | --- |
| Уникальных навыков / пар / троек | 2 406 / 32 298 / 198 426 |
| MapReduce и DuckDB | совпали полностью |
| MapReduce, время (среднее по 3 запускам) | 80,4 с, пик памяти 519,3 МБ |
| DuckDB, время (среднее по 3 запускам) | 79,9 с, пик памяти 335,7 МБ |
| MapReduce после оптимизации | 24,6 с, пик памяти 706,0 МБ |
| Медиана зарплаты: Москва / Санкт-Петербург / Екатеринбург | 229 885 / 189 655 / 172 414 руб. |

Подробности, скриншоты и выводы команд — в отчёте.

# Анализ логов

## Описание проекта с требованиями

Анализ логов

Общая задача: создать скрипт для формирования витрины на основе логов web-сайта.

<details>
  <summary>Подробное описание задачи</summary>


Разработать скрипт формирования витрины следующего содержания:
1. Суррогатный ключ устройства
1. Название устройства
1. Количество пользователей
1. Доля пользователей данного устройства от общего числа пользователей.
1. Количество совершенных действий для данного устройства
1. Доля совершенных действий с данного устройства, относительно других устройств
1. Список из 5 самых популярных браузеров, используемых на данном устройстве различными пользователями, с указанием доли использования для данного браузера относительно остальных браузеров. 
1. Количество ответов сервера отличных от 200 на данном устройстве
1. Для каждого из ответов сервера, отличных от 200, сформировать поле, в котором будет содержаться количество ответов данного типа

Источники:

https://disk.yandex.ru/d/BsdiH3DMTHpPrw

</details>

## План реализации

### Используемые технологии
Технологический стек – Apache Spark, Cassandra, Python.

В качестве файловой системы используется обычная файловая система хостовой машины.

### Схема

![Diagram1](./images/diagram.drawio.png)

Программа может работать в двух режимах

1. Инициализирующий

    Если в **parser.py** установить значение переменной **data_loading_mode=DataLoadingMode.Initializing**, то **access.log** файл будет сконвертирован в csv файл, для последующей загрузки средствами Cassandra (методом COPY).

2. Инкрементальный

    Если в **parser.py** установить значение переменной **data_loading_mode=DataLoadingMode.Incremental**, то **access.log** файл будет загружаться в Cassandra прямо из парсера.


После загрузки в Cassandra, Spark уже может брать данные и строить витрины, сохраняя результат.


### Настройка и запуск

Полезные ссылки:
- https://cassandra.apache.org/doc/latest/cassandra/getting_started/installing.html
- https://docs.datastax.com/en/developer/python-driver/3.25/getting_started/
- https://github.com/datastax/spark-cassandra-connector

```bash
bin/cassandra
bin/nodetool status
bin/cqlsh
```

Создание таблицы:

```sql
CREATE KEYSPACE my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE my_keyspace;

CREATE TABLE apache_logs (
    remote_host text,
    remote_logname text,
    remote_user text,
    request_time timestamp,
    request_line text,
    final_status int,
    bytes_sent int,
    user_agent text,
    PRIMARY KEY (remote_host, request_time)
);
```

Загрузка CSV в случае инициализирующей загрузки:

```sql
COPY apache_logs(remote_host, remote_logname, remote_user, request_time, request_line, final_status, bytes_sent, user_agent) FROM 'apache logs .csv' WITH DELIMITER=',' AND HEADER=TRUE;
```

## Результаты разработки
В результате был создан проект со следующей структурой:
```bash
.
├── cassandra                  # cassandra commands
├── data                       # data files
├── docs                       # documentation
├── images                     # screenshots
├── python                     # python source files
├── spark                      # spark source files
└── README.md
```
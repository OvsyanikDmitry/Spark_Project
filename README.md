# Организации Data Lake
### HDFS Spark Datalake

### Описание

# Kоманда хочет лучше изучить аудиторию соцсети, чтобы в будущем запустить монетизацию. Для этого было решено провести геоаналитику:
    -Выяснить, где находится большинство пользователей по количеству сообщений, лайков и подписок из одной точки.
    -Посмотреть, в какой точке Австралии регистрируется больше всего новых пользователей.
    -Определить, как часто пользователи путешествуют и какие города выбирают.

# В проекте есть только два изменения:
    -В таблицу событий добавились два поля — широта и долгота исходящих сообщений. Обновлённая таблица уже находится в HDFS по этому пути: /user/master/data/geo/events.
    -У нас теперь есть координаты городов Австралии, которые аналитики собрали в одну таблицу — geo.csv

# Шаг 1. Обновить структуру Data Lake

# Шаг 2. Создать витрину в разрезе пользователей
    1)Определить, в каком городе было совершено событие.
    2)После определения геопозиции последнего сообщения создать витрину с тремя полями:  
        user_id — идентификатор пользователя.
        act_city — актуальный адрес. Это город, из которого было отправлено последнее сообщение.
        home_city — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней.
    3)Выяснить, сколько пользователь путешествует. Добавить в витрину два поля: 
        travel_count — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
        travel_array — список городов в порядке посещения.
    4)Добавить в витрину последний атрибут — местное время (local_time) события (сообщения или других событий, если вы их разметите на основе сообщений). Местное время события — время последнего события пользователя, о котором у нас есть данные с учётом таймзоны геопозициии этого события. Данные, которые вам при этом пригодятся: 
        TIME_UTC — время в таблице событий. Указано в UTC+0.
        timezone — актуальный адрес. Атрибуты содержатся в виде Australia/Sydney.
# Расположение витрины
hdfs:/user/mityaov/data_marts/users_analitics_{date}_{d}

# Шаг 3. Создать витрину в разрезе зон.
    1)Нужно посчитать количество событий в конкретном городе за неделю и месяц. Значит, витрина будет содержать следующие поля:
        month — месяц расчёта;
        week — неделя расчёта;
        zone_id — идентификатор зоны (города);
        week_message — количество сообщений за неделю;
        week_reaction — количество реакций за неделю;
        week_subscription — количество подписок за неделю;
        week_user — количество регистраций за неделю;
        month_message — количество сообщений за месяц;
        month_reaction — количество реакций за месяц;
        month_subscription — количество подписок за месяц;
        month_user — количество регистраций за месяц.
    В этой витрине учитываются не только отправленные сообщения, но и другие действия — подписки, реакции, регистрации (рассчитываются по первым сообщениям).

# Расположение витрины
hdfs:/user/mityaov/data_marts/geo_analitics_{date}_{d}

# Шаг 4. Построить витрину для рекомендации друзей
    Витрина будет содержать следующие атрибуты:
        user_left — первый пользователь;
        user_right — второй пользователь;
        processed_dttm — дата расчёта витрины;
        zone_id — идентификатор зоны (города);
        local_time — локальное время.

# Расположение витрины
hdfs:/user/mityaov/data_marts/friends_recomendation{date}_{d}

# Шаг 5. Автоматизировать обновление витрин


# Слои хранилища
### RAW - /user/master/data/geo/events/ - данные по событиям 
### STG - /user/mityaov/data/events/ 
### DDS - geo - /user/mityaov/data/cities/geo.csv 
### DDS - events - /user/mityaov/data/events/date=yyyy-mm-dd 
### Data_marts - /user/mityaov/data/data_marts 

# Partition
geo - разрезе зон

users - в разрезе пользователей


# Структура репозитория

Внутри `src` расположены две папки:
- `/src/dags` - DAG - файлы
	- dag.py - 
	
- `/src/sqripts` - py файлы c job-ами
	- `calculating_ua.py` — Job расчета пользовательских метрик и сохранения витрины.
	- `calculating_ga.py` — Job расчета geo метрик и сохранения витрины.
	- `friends_recomendation.py` - Job расчета метрик ветрины рекомендации друзей.


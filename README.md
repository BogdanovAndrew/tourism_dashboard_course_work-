## Тестовая система управления данными туристической компании

Интерактивное Streamlit-приложение, подключающееся к локальной БД MySQL (`tink1`) и использующее существующие таблицы (`users`, `users_credentials`, `tourism_attractions`, `ratings`, `tourism_packages`, `package_places`, `user_preferences`). Приложение демонстрирует авторизацию, управление предпочтениями, персональные рекомендации, интеллектуальный поиск туров и аналитику популярности направлений.

### Требования

- Python 3.10+
- Доступ к MySQL (локально или по сети)

```bash
pip install -r requirements.txt
```

### Переменные окружения

Укажите параметры подключения к MySQL (при отсутствии используются значения из задания):

```bash
set MYSQL_HOST=localhost
set MYSQL_PORT=3306
set MYSQL_USER=root
set MYSQL_PASSWORD=DataAnalyst2025!
set MYSQL_DB=tink1
```

### Запуск

```bash
streamlit run app.py
```

### Функциональные модули

- `services/auth.py` — авторизация по таблице `users_credentials` с поддержкой хешей.
- `services/preferences.py` — чтение профиля, предпочтений и оценок.
- `services/recommendations.py` — использует SQL-функцию `get_recommendation_score(user_id, place_id)` для скоринга, при недоступности функции пробует `get_recommendations`, затем fallback на Python.
- `services/search.py` — конструктор поиска турпакетов с ранжированием по предпочтениям.
- `services/analytics.py` — агрегации популярности и материалы для админ/аналитик-дэшбордов.
- `services/ratings.py` — управление оценками пользователей (добавление/удаление).
- `db.py` — управление пулом соединений MySQL.

### Пользовательские роли

- **Обычный пользователь** — персональные предпочтения, рекомендации, поиск туров.
- **Администратор (`admin`)** — отдельный экран с метриками по пользователям и пакетам, журналом оценок и обслуживанием кэша.
- **Аналитик (`analyst`)** — расширенный дашборд с глобальными тенденциями по городам, категориям, ценовым сегментам и активности пользователей.

### Дополнительные изменения БД (рекомендации)

1. Создать индексы для ускорения выборок:
   - `CREATE INDEX idx_ratings_user ON ratings(user_id);`
   - `CREATE INDEX idx_ratings_place ON ratings(place_id);`
   - `CREATE INDEX idx_preferences_user ON user_preferences(user_id, preference_type);`
   - `CREATE INDEX idx_packages_city ON tourism_packages(city);`
2. Добавить в `users_credentials` колонку `is_blocked TINYINT(1) DEFAULT 0`, чтобы администратор мог отключать доступ.
3. Убедиться, что в `users_credentials` есть поле `password_hash` (SHA256) и заполнено для всех пользователей.
4. Создать или обновить функцию `get_recommendation_score(p_user_id INT, p_place_id INT)` (листинг из задания); при необходимости дополнительно реализовать процедуру `get_recommendations` для обратной совместимости.
5. Создать представление `vw_package_prices` (сумма цен входящих аттракций) для ускорения расчета бюджета в поиске.
6. Настроить регулярное обновление агрегатов популярности (материализованная таблица или событие MySQL), чтобы графики в Streamlit загружались быстрее на больших объемах данных.


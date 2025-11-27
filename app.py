from __future__ import annotations

from typing import Dict, Optional

from io import BytesIO

import pandas as pd
import plotly.express as px
import streamlit as st
from mysql.connector import Error

from services.auth import authenticate
from services.preferences import (
    build_preference_vector,
    get_user_preferences,
    get_user_profile,
    get_user_ratings,
)
from services.recommendations import get_recommendations
from services.search import (
    get_available_categories,
    get_available_cities,
    search_packages,
)
from services.analytics import (
    get_category_satisfaction,
    get_city_demand,
    get_popular_places,
    get_price_segments,
    get_ratings_timeline,
    get_entity_counts,
    get_users_overview,
    get_recent_ratings,
    get_ratings_by_category,
    get_ratings_by_city,
    get_user_activity,
    get_package_coverage,
)
from services.ratings import delete_rating, list_attractions, upsert_rating
from services.admin import get_credentials_overview, set_user_block_status
from utils.ui import render_kpi, render_profile_card, render_section


ROLE_LABELS = {
    "admin": "Администратор",
    "analyst": "Аналитик",
    "user": "Пользователь",
}


COLUMN_RU = {
    "user_id": "ID пользователя",
    "login": "Логин",
    "location": "Локация",
    "age": "Возраст",
    "preference_type": "Тип предпочтения",
    "preference_key": "Ключ",
    "preference_value": "Вес",
    "place_id": "ID места",
    "place_name": "Название места",
    "category": "Категория",
    "city": "Город",
    "price": "Цена",
    "time_minutes": "Минуты",
    "overall_rating": "Рейтинг по каталогу",
    "rating": "Оценка пользователя",
    "rated_at": "Дата оценки",
    "package_id": "ID пакета",
    "package_name": "Название пакета",
    "description": "Описание",
    "itinerary": "Маршрут",
    "categories": "Категории в пакете",
    "total_price": "Суммарная цена",
    "avg_rating": "Средний рейтинг",
    "avg_user_rating": "Средняя пользовательская оценка",
    "ranking_score": "Индекс персонализации",
    "stops": "Количество остановок",
    "rating_count": "Количество оценок",
    "price_segment": "Ценовой сегмент",
    "attractions": "Количество объектов",
    "cnt": "Количество объектов",
    "avg_price": "Средняя цена",
    "rated_date": "Дата",
    "recommendation_score": "Скор рекомендации",
    "score": "Скор рекомендации",
    "source": "Источник",
    "package_count": "Количество пакетов",
    "total_stops": "Всего посещений",
    "is_blocked": "Заблокирован",
}

PREFERENCE_TYPE_DESCRIPTIONS = {
    "category_preference": "Приоритет категорий достопримечательностей",
    "city_preference": "Предпочитаемые города",
    "price_preference": "Желаемый ценовой сегмент",
    "duration_preference": "Комфортная длительность визитов",
}


def inject_global_styles():
    st.markdown(
        """
        <style>
        .stApp {
            background: radial-gradient(circle at top, rgba(59,130,246,0.25), rgba(236,72,153,0.18)), #f5f7fb !important;
            color: #0f172a;
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.3rem;
            background: rgba(255,255,255,0.6);
            padding: 0.4rem 0.6rem;
            border-radius: 999px;
        }
        .stTabs [data-baseweb="tab"] {
            background: transparent;
            border-radius: 999px;
            padding: 0.35rem 1.2rem;
            color: #475569;
            font-weight: 600;
        }
        .stTabs [aria-selected="true"] {
            background: linear-gradient(120deg, #6366f1, #ec4899);
            color: #ffffff;
            box-shadow: 0 10px 25px rgba(99,102,241,0.35);
        }
        [data-testid="metric-container"] {
            background: linear-gradient(135deg, rgba(59,130,246,0.12), rgba(236,72,153,0.12));
            border-radius: 18px;
            padding: 1rem;
            border: 1px solid rgba(255,255,255,0.6);
            box-shadow: 0 15px 35px rgba(15,23,42,0.12);
        }
        [data-testid="stTable"], .stDataFrame {
            background: rgba(255,255,255,0.75);
            border-radius: 20px;
            padding: 0.4rem;
            box-shadow: 0 20px 40px rgba(15,23,42,0.12);
        }
        .stButton>button, .stDownloadButton>button, .stForm button {
            background: linear-gradient(120deg, #2563eb, #7c3aed);
            border: none;
            color: white;
            padding: 0.4rem 1.4rem;
            border-radius: 999px;
            font-weight: 600;
            box-shadow: 0 10px 20px rgba(37,99,235,0.3);
        }
        .stButton>button:hover, .stDownloadButton>button:hover, .stForm button:hover {
            background: linear-gradient(120deg, #1d4ed8, #6d28d9);
        }
        .stForm {
            background: rgba(255,255,255,0.7);
            padding: 1rem 1.4rem;
            border-radius: 18px;
            box-shadow: 0 12px 30px rgba(15,23,42,0.1);
            border: 1px solid rgba(148,163,184,0.2);
        }
        h3 {
            color: #0f172a !important;
            position: relative;
        }
        h3:after {
            content: "";
            display: block;
            width: 60px;
            height: 4px;
            border-radius: 999px;
            margin-top: 6px;
            background: linear-gradient(120deg, #818cf8, #f472b6);
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(244,247,254,0.95)) !important;
            color: #0f172a !important;
            box-shadow: 4px 0 30px rgba(15,23,42,0.1);
        }
        [data-testid="stSidebar"] button {
            background: linear-gradient(120deg, #f97316, #ec4899);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


st.set_page_config(
    page_title="Туристическая аналитика",
    layout="wide",
    page_icon="🌍",
)


@st.cache_data(show_spinner=False)
def cached_user_profile(user_id: int) -> Optional[Dict]:
    return get_user_profile(user_id)


@st.cache_data(show_spinner=False)
def cached_preferences(user_id: int) -> pd.DataFrame:
    return get_user_preferences(user_id)


@st.cache_data(show_spinner=False)
def cached_ratings(user_id: int) -> pd.DataFrame:
    return get_user_ratings(user_id)


@st.cache_data(ttl=300, show_spinner=False)
def cached_cities():
    return get_available_cities()


@st.cache_data(ttl=300, show_spinner=False)
def cached_categories():
    return get_available_categories()


@st.cache_data(ttl=600, show_spinner=False)
def cached_places():
    return list_attractions()


def detect_role(username: str) -> str:
    user_login = (username or "").strip().lower()
    if user_login == "admin":
        return "admin"
    if user_login == "analyst":
        return "analyst"
    return "user"


def format_date(value):
    if value is None:
        return "—"
    if isinstance(value, str):
        return value.split(" ")[0]
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)


def localize_columns(df: pd.DataFrame, extra_map: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    if df.empty:
        return df
    mapping = COLUMN_RU.copy()
    if extra_map:
        mapping.update(extra_map)
    rename_map = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=rename_map)


def df_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Данные")
    output.seek(0)
    return output.getvalue()


def download_button_for_df(df: pd.DataFrame, filename: str, label: str):
    if df.empty:
        return
    st.download_button(
        label=label,
        data=df_to_xlsx_bytes(df),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def login_screen():
    st.markdown(
        """
        <style>
        .login-card {
            max-width: 420px;
            margin: 6rem auto 0;
            background: radial-gradient(circle at top, #f8fafc, #e2e8f0);
            border-radius: 18px;
            padding: 2.2rem 2.6rem;
            box-shadow: 0 25px 60px rgba(15, 23, 42, 0.18);
            border: 1px solid rgba(15, 23, 42, 0.08);
        }
        .login-card h1 {
            font-size: 1.7rem;
            margin-bottom: 0.5rem;
            color: #0f172a;
        }
        .login-card p {
            color: #475569;
            margin-bottom: 1.5rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div class="login-card">
            <h1>Туристическая аналитика</h1>
            <p>В рамках учебного проекта используется упрощённая система аутентификации. Введите логин и пароль.</p>
        """,
        unsafe_allow_html=True,
    )
    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Имя пользователя")
        password = st.text_input("Пароль", type="password")
        submitted = st.form_submit_button("Войти")
        if submitted:
            try:
                user = authenticate(username, password)
            except PermissionError as exc:
                st.error(str(exc))
                return
            except Error as exc:
                st.error(f"Ошибка соединения с БД: {exc}")
                return
            if user:
                role = detect_role(username)
                user["role"] = role
                st.session_state["auth_user"] = user
                st.session_state["role"] = role
                st.success("Успешный вход")
                st.rerun()
            else:
                st.error("Неверные учетные данные")
    st.markdown("</div>", unsafe_allow_html=True)


def render_preferences_tab(user_id: int):
    profile = cached_user_profile(user_id)
    pref_df = cached_preferences(user_id)
    ratings_df = cached_ratings(user_id)

    render_section("Профиль пользователя")
    if profile:
        render_profile_card(profile)
    else:
        st.info("Нет данных по пользователю в таблице `users`.")

    render_section("Предпочтения")
    if pref_df.empty:
        st.warning("Предпочтения не заданы.")
    else:
        pref_df["preference_value"] = pd.to_numeric(pref_df["preference_value"], errors="coerce")
        pref_display = pref_df.copy()
        pref_display["Описание"] = pref_display["preference_type"].map(
            PREFERENCE_TYPE_DESCRIPTIONS
        ).fillna("—")
        pref_display = localize_columns(pref_display)
        st.dataframe(pref_display, use_container_width=True)

        pref_summary = (
            pref_df.groupby("preference_type")["preference_value"]
            .agg(["count", "mean", "max"])
            .reset_index()
        ).rename(
            columns={
                "preference_type": "Код типа",
                "count": "Количество записей",
                "mean": "Среднее значение",
                "max": "Макс. значение",
            }
        )
        pref_summary["Описание"] = pref_summary["Код типа"].map(
            PREFERENCE_TYPE_DESCRIPTIONS
        ).fillna("—")
        st.caption("Сводка по весам предпочтений")
        st.dataframe(pref_summary, use_container_width=True)

    render_section("История оценок")
    if ratings_df.empty:
        st.info("Пользователь пока не ставил оценки.")
    else:
        col1, col2, col3 = st.columns(3)
        city_mode_series = ratings_df["city"].mode()
        favorite_city = city_mode_series.iloc[0] if not city_mode_series.empty else "—"
        with col1:
            render_kpi("Оценок всего", len(ratings_df), help_text="Количество строк в ratings")
        with col2:
            render_kpi(
                "Средняя пользовательская оценка",
                round(ratings_df["rating"].mean(), 2),
                help_text="Среднее значение rating",
            )
        with col3:
            render_kpi(
                "Любимый город",
                favorite_city,
                help_text="Город с наибольшим числом оценок",
            )
        ratings_view = localize_columns(ratings_df.copy())
        st.dataframe(ratings_view, use_container_width=True)
    render_rating_management(user_id, ratings_df)


def render_recommendations_tab(user_id: int, preference_vector: Dict):
    render_section("Персональные предложения")
    try:
        rec_df = get_recommendations(user_id, preference_vector)
    except Error as exc:
        st.error(f"Не удалось получить рекомендации: {exc}")
        return

    if rec_df.empty:
        st.warning("Нет данных для рекомендаций.")
        return

    rec_df = rec_df.drop(columns=["source"], errors="ignore")

    st.markdown(
        """
        <style>
        .recommendation-highlight {
            border-radius: 18px;
            padding: 1.2rem 1.6rem;
            background: linear-gradient(135deg, rgba(59,130,246,0.15), rgba(236,72,153,0.12));
            border: 1px solid rgba(15,23,42,0.08);
            box-shadow: 0 15px 35px rgba(15, 23, 42, 0.12);
            margin-bottom: 1rem;
        }
        .recommendation-highlight h4 {
            margin: 0 0 .35rem 0;
            color: #0f172a;
        }
        .recommendation-highlight span {
            color: #475569;
            font-size: 0.95rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    top_pick = rec_df.iloc[0]
    st.markdown(
        f"""
        <div class="recommendation-highlight">
            <h4>Лучший матч: {top_pick.get('place_name', '—')}</h4>
            <span>Город: {top_pick.get('city', '—')} • Категория: {top_pick.get('category', '—')} • Рейтинг: {round(float(top_pick.get('overall_rating', 0) or 0), 2)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if len(rec_df) > 1:
        cols = st.columns(min(3, len(rec_df)))
        for idx, col in enumerate(cols):
            if idx + 1 >= len(rec_df):
                break
            row = rec_df.iloc[idx + 1]
            with col:
                st.caption("Вариант " + str(idx + 2))
                st.write(f"**{row.get('place_name', '—')}**")
                st.write(f"{row.get('city', '—')} · {row.get('category', '—')}")
                st.write(f"Рейтинг каталога: {round(float(row.get('overall_rating', 0) or 0), 2)}")

    rec_display = localize_columns(rec_df.copy())
    st.dataframe(rec_display, use_container_width=True)


def render_search_tab(user_id: int, preference_vector: Dict):
    render_section("Поиск и ранжирование туров")
    cities = ["Все"] + cached_cities()
    categories = ["Все"] + cached_categories()
    with st.form("search_form"):
        selected_city = st.selectbox("Город", cities)
        selected_category = st.selectbox("Категория", categories)
        col1, col2 = st.columns(2)
        with col1:
            min_price = st.number_input("Мин. бюджет", min_value=0.0, value=0.0, step=100.0)
        with col2:
            max_price = st.number_input("Макс. бюджет", min_value=0.0, value=0.0, step=100.0)
        submitted = st.form_submit_button("Найти туры")

    if submitted:
        city = selected_city if selected_city != "Все" else None
        category = selected_category if selected_category != "Все" else None
        price_range = (
            min_price if min_price > 0 else None,
            max_price if max_price > 0 else None,
        )
        try:
            df = search_packages(city, category, price_range, preference_vector)
        except Error as exc:
            st.error(f"Ошибка поиска: {exc}")
            return
        if df.empty:
            st.info("По заданным условиям туры не найдены.")
        else:
            df_display = localize_columns(df.copy())
            st.dataframe(df_display, use_container_width=True)


def render_analytics_tab():
    render_section("Популярность мест и направлений")
    try:
        popular = get_popular_places()
        cities = get_city_demand()
        categories = get_category_satisfaction()
        price_segments = get_price_segments()
        ratings_timeline = get_ratings_timeline()
    except Error as exc:
        st.error(f"Ошибка аналитики: {exc}")
        return

    if not popular.empty:
        fig = px.bar(popular, x="place_name", y="rating_count", color="city", title="ТОП мест по количеству оценок")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(localize_columns(popular), use_container_width=True)

    if not cities.empty:
        fig = px.scatter(
            cities,
            x="attractions",
            y="avg_rating",
            size="attractions",
            color="city",
            title="Города: насыщенность и средний рейтинг",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(localize_columns(cities), use_container_width=True)

    if not categories.empty:
        fig = px.bar(
            categories,
            x="category",
            y="avg_rating",
            title="Удовлетворенность по категориям",
        )
        st.plotly_chart(fig, use_container_width=True)
        category_df = localize_columns(categories).rename(columns={"cnt": "Количество объектов"})
        st.dataframe(category_df, use_container_width=True)

    if not price_segments.empty:
        fig = px.bar(
            price_segments,
            x="price_segment",
            y="attractions",
            color="avg_rating",
            text="avg_rating",
            title="Распределение объектов по ценовым сегментам",
            labels={"avg_rating": "Средний рейтинг"},
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(localize_columns(price_segments), use_container_width=True)

    if not ratings_timeline.empty:
        fig = px.line(
            ratings_timeline,
            x="rated_date",
            y="avg_rating",
            markers=True,
            title="Динамика средних оценок пользователей",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(localize_columns(ratings_timeline), use_container_width=True)


def render_rating_management(user_id: int, ratings_df: pd.DataFrame):
    render_section("Управление оценками")

    places = cached_places()
    if places:
        place_options = {
            f"{row['place_name']} — {row.get('city', 'не указан')} (ID {row['place_id']})": row[
                "place_id"
            ]
            for row in places
        }
    else:
        place_options = {}

    with st.expander("Добавить или обновить оценку", expanded=False):
        if not place_options:
            st.info("Нет доступных достопримечательностей для оценивания.")
        else:
            place_label = st.selectbox(
                "Достопримечательность",
                options=list(place_options.keys()),
                key="rating_add_place",
            )
            rating_value = st.slider("Оценка", min_value=1.0, max_value=5.0, step=0.5, value=4.0)
            if st.button("Сохранить", key="rating_add_btn"):
                try:
                    upsert_rating(user_id, place_options[place_label], rating_value)
                except Error as exc:
                    st.error(f"Не удалось сохранить оценку: {exc}")
                else:
                    cached_ratings.clear()
                    st.success("Оценка сохранена.")
                    st.rerun()

    with st.expander("Удалить оценку", expanded=False):
        if ratings_df.empty:
            st.info("У пользователя пока нет оценок.")
        else:
            delete_options = {
                f"{row.place_name} — {row.rating} ⭐ ({format_date(row.rated_at)})": row.place_id
                for row in ratings_df.itertuples()
            }
            delete_label = st.selectbox(
                "Выберите оценку",
                options=list(delete_options.keys()),
                key="rating_delete_select",
            )
            if st.button("Удалить", key="rating_delete_btn"):
                try:
                    delete_rating(user_id, delete_options[delete_label])
                except Error as exc:
                    st.error(f"Не удалось удалить оценку: {exc}")
                else:
                    cached_ratings.clear()
                    st.success("Оценка удалена.")
                    st.rerun()


def render_admin_view():
    st.title("Панель администратора")
    counts = get_entity_counts()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_kpi("Пользователи", counts.get("users_count", 0))
    with col2:
        render_kpi("Достопримечательности", counts.get("attractions_count", 0))
    with col3:
        render_kpi("Пакеты", counts.get("packages_count", 0))
    with col4:
        render_kpi("Оценки", counts.get("ratings_count", 0))

    render_section("Пользователи системы")
    users_df = get_users_overview()
    if users_df.empty:
        st.info("Нет данных о пользователях.")
    else:
        users_display = localize_columns(users_df)
        st.dataframe(users_display, use_container_width=True)
        download_button_for_df(users_display, "users_overview.xlsx", "Скачать пользователей в XLSX")

    render_section("Управление доступом")
    credentials_df, block_supported = get_credentials_overview()
    if credentials_df.empty:
        st.info("Нет учетных записей для отображения.")
    else:
        creds_display = localize_columns(credentials_df, {"is_blocked": "Заблокирован"})
        st.dataframe(creds_display, use_container_width=True)
        if block_supported:
            options = {
                f"{row.login} (UID {row.user_id}){' — заблокирован' if str(row.is_blocked).lower() in {'1','true','yes'} else ''}": int(
                    row.user_id
                )
                for row in credentials_df.itertuples()
            }
            selected_label = st.selectbox("Выберите пользователя", list(options.keys()), key="admin_block_user")
            is_block_action = st.radio(
                "Действие",
                ("Разблокировать", "Заблокировать"),
                horizontal=True,
                key="admin_block_action",
            )
            if st.button("Применить", key="admin_block_apply"):
                try:
                    set_user_block_status(
                        options[selected_label],
                        blocked=(is_block_action == "Заблокировать"),
                    )
                except RuntimeError as exc:
                    st.error(str(exc))
                except Error as exc:
                    st.error(f"Не удалось обновить статус: {exc}")
                else:
                    st.success("Статус обновлён.")
                    st.rerun()
        else:
            st.warning(
                "Для блокировки пользователей добавьте колонку `is_blocked TINYINT(1)` в таблицу `users_credentials`."
            )

    render_section("Последние оценки пользователей")
    recent_df = get_recent_ratings()
    if recent_df.empty:
        st.info("Пока нет оценок.")
    else:
        recent_display = localize_columns(recent_df)
        st.dataframe(recent_display, use_container_width=True)
        download_button_for_df(recent_display, "recent_ratings.xlsx", "Скачать оценки в XLSX")
        options = {
            f"UID {row.user_id} → {row.place_name} ({format_date(row.rated_at)})": (row.user_id, row.place_id)
            for row in recent_df.itertuples()
        }
        delete_label = st.selectbox(
            "Удалить оценку",
            options=list(options.keys()),
            key="admin_delete_rating",
        )
        if st.button("Удалить выбранную оценку"):
            uid, pid = options[delete_label]
            try:
                delete_rating(uid, pid)
            except Error as exc:
                st.error(f"Не удалось удалить оценку: {exc}")
            else:
                cached_ratings.clear()
                st.success("Оценка удалена.")
                st.rerun()

    render_section("Обслуживание кэша")
    if st.button("Очистить кэш данных"):
        cached_user_profile.clear()
        cached_preferences.clear()
        cached_ratings.clear()
        cached_cities.clear()
        cached_categories.clear()
        cached_places.clear()
        st.success("Кэш очищен.")


def render_analyst_view():
    st.title("Дашборд аналитика")
    counts = get_entity_counts()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        render_kpi("Пользователи", counts.get("users_count", 0))
    with col2:
        render_kpi("Оценок", counts.get("ratings_count", 0))
    with col3:
        render_kpi("Пакетов", counts.get("packages_count", 0))
    with col4:
        render_kpi("Объектов", counts.get("attractions_count", 0))

    render_section("Динамика оценок по дням")
    timeline = get_ratings_timeline(90)
    if timeline.empty:
        st.info("Недостаточно данных для отображения тренда.")
    else:
        fig = px.line(
            timeline,
            x="rated_date",
            y="avg_rating",
            markers=True,
            title="Средняя оценка пользователей (90 дней)",
        )
        st.plotly_chart(fig, use_container_width=True)
        timeline_display = localize_columns(timeline)
        st.dataframe(timeline_display, use_container_width=True)
        download_button_for_df(timeline_display, "ratings_timeline.xlsx", "Скачать динамику в XLSX")

    col_a, col_b = st.columns(2)
    with col_a:
        render_section("Категории по активности")
        cat_df = get_ratings_by_category()
        if cat_df.empty:
            st.info("Нет категорий для отображения.")
        else:
            fig = px.bar(
                cat_df,
                x="category",
                y="rating_count",
                color="avg_user_rating",
                title="Количество оценок по категориям",
            )
            st.plotly_chart(fig, use_container_width=True)
            cat_display = localize_columns(cat_df)
            st.dataframe(cat_display, use_container_width=True)
            download_button_for_df(cat_display, "categories_activity.xlsx", "Скачать категории")
    with col_b:
        render_section("Города по активности")
        city_df = get_ratings_by_city()
        if city_df.empty:
            st.info("Нет городов для отображения.")
        else:
            fig = px.bar(
                city_df,
                x="city",
                y="rating_count",
                color="avg_user_rating",
                title="Количество оценок по городам",
            )
            st.plotly_chart(fig, use_container_width=True)
            city_display = localize_columns(city_df)
            st.dataframe(city_display, use_container_width=True)
            download_button_for_df(city_display, "cities_activity.xlsx", "Скачать города")

    render_section("Активность пользователей")
    activity_df = get_user_activity()
    if activity_df.empty:
        st.info("Нет данных об активности пользователей.")
    else:
        search_query = st.text_input(
            "Поиск пользователя по ID или локации",
            key="analyst_user_search",
        ).strip()
        filtered_activity = activity_df.copy()
        if search_query:
            filtered_activity = filtered_activity[
                filtered_activity["user_id"].astype(str).str.contains(search_query, case=False, na=False)
                | filtered_activity["location"].astype(str).str.contains(search_query, case=False, na=False)
            ]
            st.caption(f"Найдено записей: {len(filtered_activity)}")
        activity_display = localize_columns(filtered_activity)
        st.dataframe(activity_display, use_container_width=True)
        download_button_for_df(activity_display, "user_activity.xlsx", "Скачать активность пользователей")

    render_section("Покрытие пакетами и ценовые сегменты")
    packages_df = get_package_coverage()
    price_df = get_price_segments()
    col1, col2 = st.columns(2)
    with col1:
        if packages_df.empty:
            st.info("Нет данных по пакетам.")
        else:
            fig = px.bar(
                packages_df,
                x="city",
                y="package_count",
                title="Пакеты по городам",
                text="total_stops",
            )
            st.plotly_chart(fig, use_container_width=True)
            packages_display = localize_columns(packages_df)
            st.dataframe(packages_display, use_container_width=True)
            download_button_for_df(packages_display, "packages_coverage.xlsx", "Скачать покрытие пакетов")
    with col2:
        if price_df.empty:
            st.info("Нет данных по ценовым сегментам.")
        else:
            fig = px.pie(
                price_df,
                names="price_segment",
                values="attractions",
                title="Распределение объектов по сегментам",
            )
            st.plotly_chart(fig, use_container_width=True)
            price_display = localize_columns(price_df)
            st.dataframe(price_display, use_container_width=True)
            download_button_for_df(price_display, "price_segments.xlsx", "Скачать сегменты")

    render_section("ТОП популярные места")
    popular = get_popular_places(15)
    if popular.empty:
        st.info("Нет популярных мест для отображения.")
    else:
        popular_display = localize_columns(popular)
        st.dataframe(popular_display, use_container_width=True)
        download_button_for_df(popular_display, "popular_places.xlsx", "Скачать популярные места")


def dashboard():
    inject_global_styles()
    user = st.session_state.get("auth_user")
    if not user:
        login_screen()
        return

    role = user.get("role") or detect_role(user.get("username"))
    st.sidebar.success(f"{ROLE_LABELS.get(role, 'Пользователь')}: {user['username']}")
    if st.sidebar.button("Выйти"):
        st.session_state.pop("auth_user")
        st.session_state.pop("role", None)
        st.rerun()

    if role == "admin":
        render_admin_view()
        return
    if role == "analyst":
        render_analyst_view()
        return

    pref_df = cached_preferences(user["user_id"])
    preference_vector = build_preference_vector(pref_df)

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "Предпочтения и оценки",
            "Персональные предложения",
            "Поиск туров",
            "Аналитика популярности",
        ]
    )

    with tab1:
        render_preferences_tab(user["user_id"])
    with tab2:
        render_recommendations_tab(user["user_id"], preference_vector)
    with tab3:
        render_search_tab(user["user_id"], preference_vector)
    with tab4:
        render_analytics_tab()


if __name__ == "__main__":
    dashboard()


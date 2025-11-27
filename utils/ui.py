from __future__ import annotations

from html import escape
from typing import Any, Mapping

import streamlit as st


def render_kpi(label: str, value, help_text: str = ""):
    with st.container():
        st.metric(label, value if value is not None else "—", help=help_text or None)


def render_section(title: str):
    st.markdown(f"### {title}")


def render_profile_card(profile: Mapping[str, Any]):
    user_id = escape(str(profile.get("user_id", "—")))
    location = escape(str(profile.get("location", "—")))
    age = escape(str(profile.get("age", "—")))
    card_html = f"""
    <div style="
        border-radius: 16px;
        padding: 1.2rem 1.4rem;
        background: linear-gradient(135deg, #f5f7fa 0%, #e4ecf7 100%);
        border: 1px solid rgba(0,0,0,0.05);
        box-shadow: 0 8px 20px rgba(15,23,42,0.08);
        margin-bottom: 1rem;
    ">
        <div style="font-size: 0.85rem; text-transform: uppercase; letter-spacing: .08em; color: #64748b;">
            Профиль клиента
        </div>
        <div style="display: flex; gap: 1.5rem; flex-wrap: wrap; margin-top: 0.8rem;">
            <div>
                <div style="font-size: 0.8rem; color: #94a3b8;">ID</div>
                <div style="font-size: 1.4rem; font-weight: 600; color: #0f172a;">{user_id}</div>
            </div>
            <div>
                <div style="font-size: 0.8rem; color: #94a3b8;">Локация</div>
                <div style="font-size: 1.1rem; font-weight: 500; color: #0f172a;">{location}</div>
            </div>
            <div>
                <div style="font-size: 0.8rem; color: #94a3b8;">Возраст</div>
                <div style="font-size: 1.4rem; font-weight: 600; color: #0f172a;">{age}</div>
            </div>
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)





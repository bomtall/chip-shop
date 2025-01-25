import streamlit as st

# command to run: streamlit run src/fryer/counter/chip_shop_app/app.py

st.set_page_config(
    page_title="chip-shop",
    page_icon="🍟",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    # 🍟

    Welcome to the **Chip-Shop** app!

    This app is our chip-shop counter where data hungry people eat.

    [![Repo](https://badgen.net/badge/icon/GitHub?icon=github&label)](https://github.com/bomtall/chip-shop)

    """,
    unsafe_allow_html=True,
)

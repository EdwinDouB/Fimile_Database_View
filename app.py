import pandas as pd
import pymysql
import streamlit as st


DEFAULT_DB_CONFIG = {
    "host": "47.253.206.87",
    "port": 3306,
    "user": "webreader",
    "password": "T!Q3-Vy28nb$61598925243649",
    "database": "wyaoo",
}

st.set_page_config(page_title="Fimile-Tweak DB Browser", layout="wide")
st.title("Fimile-Tweak Database Browser")
st.caption("Connected automatically to the Fimile-Tweak database.")

if "conn" not in st.session_state:
    st.session_state.conn = None
if "tables" not in st.session_state:
    st.session_state.tables = []
if "connected_database" not in st.session_state:
    st.session_state.connected_database = ""


def run_query(sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql(sql, st.session_state.conn, params=params)


def load_tables(database_name: str) -> None:
    tables_df = run_query(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME",
        params=[database_name],
    )
    st.session_state.tables = tables_df["TABLE_NAME"].tolist()


def connect_and_load() -> None:
    st.session_state.conn = pymysql.connect(
        host=DEFAULT_DB_CONFIG["host"],
        port=DEFAULT_DB_CONFIG["port"],
        user=DEFAULT_DB_CONFIG["user"],
        password=DEFAULT_DB_CONFIG["password"],
        database=DEFAULT_DB_CONFIG["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

    current_db = run_query("SELECT DATABASE() AS db_name")
    st.session_state.connected_database = current_db.loc[0, "db_name"] or DEFAULT_DB_CONFIG["database"]

    if st.session_state.connected_database:
        load_tables(st.session_state.connected_database)


with st.sidebar:
    st.header("Connection")
    st.text(f"Host: {DEFAULT_DB_CONFIG['host']}")
    st.text(f"Port: {DEFAULT_DB_CONFIG['port']}")
    st.text(f"User: {DEFAULT_DB_CONFIG['user']}")
    st.text(f"Database: {DEFAULT_DB_CONFIG['database']}")
    reconnect_btn = st.button("Reconnect", use_container_width=True)


if st.session_state.conn is None or reconnect_btn:
    try:
        connect_and_load()
        if reconnect_btn:
            st.success("Reconnected successfully.")
    except Exception as e:
        st.session_state.conn = None
        st.error(f"Auto-connection failed: {e}")


if st.session_state.conn:
    left_col, right_col = st.columns([1, 2])

    with left_col:
        st.subheader("Schema")
        st.text(f"Active database: {st.session_state.connected_database}")

        selected_table = st.selectbox("Table", options=st.session_state.tables) if st.session_state.tables else None

        row_limit = st.slider("Rows per page", min_value=10, max_value=500, value=100, step=10)
        page = st.number_input("Page", min_value=1, value=1, step=1)

    with right_col:
        if selected_table:
            st.subheader(f"Table Preview: `{selected_table}`")
            offset = (page - 1) * row_limit
            preview_query = f"SELECT * FROM `{selected_table}` LIMIT {row_limit} OFFSET {offset}"

            try:
                df = run_query(preview_query)
                st.dataframe(df, use_container_width=True, hide_index=True)

                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download current page as CSV",
                    data=csv,
                    file_name=f"{selected_table}_page_{page}.csv",
                    mime="text/csv",
                )

                total_count_df = run_query(f"SELECT COUNT(*) AS total_rows FROM `{selected_table}`")
                total_rows = int(total_count_df.loc[0, "total_rows"])
                st.caption(f"Showing {len(df)} row(s) out of {total_rows:,} total row(s).")
            except Exception as e:
                st.error(f"Failed to read table: {e}")

    st.divider()
    st.subheader("Database Actions")
    st.caption("Run common database operations using buttons and dropdowns instead of manual SQL commands.")

    action_col_1, action_col_2, action_col_3 = st.columns(3)
    show_tables_btn = action_col_1.button("Show Tables", use_container_width=True)
    describe_table_btn = action_col_2.button("Describe Selected Table", use_container_width=True, disabled=not selected_table)
    count_rows_btn = action_col_3.button("Count Rows in Selected Table", use_container_width=True, disabled=not selected_table)

    action_result = None
    action_title = None

    try:
        if show_tables_btn:
            action_title = "Tables"
            action_result = run_query(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME",
                params=[st.session_state.connected_database],
            )

        if describe_table_btn and selected_table:
            action_title = f"Columns in `{selected_table}`"
            action_result = run_query(
                """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                params=[st.session_state.connected_database, selected_table],
            )

        if count_rows_btn and selected_table:
            action_title = f"Row Count for `{selected_table}`"
            action_result = run_query(f"SELECT COUNT(*) AS total_rows FROM `{selected_table}`")

        if action_result is not None:
            st.markdown(f"#### {action_title}")
            st.dataframe(action_result, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Action failed: {e}")
else:
    st.error("Could not connect automatically. Click Reconnect in the sidebar to retry.")

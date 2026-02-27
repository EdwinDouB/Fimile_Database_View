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
if "active_action" not in st.session_state:
    st.session_state.active_action = None
if "action_result" not in st.session_state:
    st.session_state.action_result = None
if "action_title" not in st.session_state:
    st.session_state.action_title = ""
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None


def run_query(sql: str, params=None) -> pd.DataFrame:
    with st.session_state.conn.cursor() as cursor:
        cursor.execute(sql, params or [])
        rows = cursor.fetchall()
        if rows:
            return pd.DataFrame(rows)

        columns = [col[0] for col in (cursor.description or [])]
        return pd.DataFrame(columns=columns)


def run_scalar_query(sql: str, params=None):
    with st.session_state.conn.cursor() as cursor:
        cursor.execute(sql, params or [])
        row = cursor.fetchone()
        if not row:
            return None
        return next(iter(row.values()))


def quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def load_tables(database_name: str) -> None:
    tables_df = run_query(
        """
        SELECT TABLE_NAME AS table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME
        """,
        params=[database_name],
    )
    st.session_state.tables = tables_df["table_name"].tolist() if "table_name" in tables_df else []


def connect_and_load() -> None:␊
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

    current_db = run_scalar_query("SELECT DATABASE() AS db_name")
    st.session_state.connected_database = current_db or DEFAULT_DB_CONFIG["database"]

    if st.session_state.connected_database:
        load_tables(st.session_state.connected_database)


def select_next_table() -> None:
    tables = st.session_state.tables
    if not tables:
        st.session_state.selected_table = None
        return

    current_table = st.session_state.get("selected_table")
    if current_table not in tables:
        st.session_state.selected_table = tables[0]
        return

    current_index = tables.index(current_table)
    next_index = (current_index + 1) % len(tables)
    st.session_state.selected_table = tables[next_index]


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

        if st.session_state.tables:
            if st.session_state.selected_table not in st.session_state.tables:
                st.session_state.selected_table = st.session_state.tables[0]

            table_col, next_btn_col = st.columns([4, 1])
            with table_col:
                selected_table = st.selectbox("Table", options=st.session_state.tables, key="selected_table")
            with next_btn_col:
                st.markdown("<div style='height: 1.8rem;'></div>", unsafe_allow_html=True)
                st.button("Next", use_container_width=True, on_click=select_next_table)
        else:
            st.session_state.selected_table = None
            selected_table = None

        row_limit = st.slider("Rows per page", min_value=10, max_value=500, value=100, step=10)
        page = st.number_input("Page", min_value=1, value=1, step=1)

    with right_col:
        if selected_table:
            st.subheader(f"Table Preview: `{selected_table}`")
            offset = (page - 1) * row_limit
            selected_table_sql = quote_identifier(selected_table)
            preview_query = f"SELECT * FROM {selected_table_sql} LIMIT {row_limit} OFFSET {offset}"

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

                total_count_df = run_query(f"SELECT COUNT(*) AS total_rows FROM {selected_table_sql}")
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

    try:
        if show_tables_btn:
            st.session_state.active_action = "show_tables"
            st.session_state.action_title = "Tables"
            tables_offset = (page - 1) * row_limit
            st.session_state.action_result = run_query(
                """
                SELECT TABLE_NAME AS table_name
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME
                LIMIT %s OFFSET %s
                """,
                params=[st.session_state.connected_database, row_limit, tables_offset],
            )

        if describe_table_btn and selected_table:
            st.session_state.active_action = "describe_table"
            st.session_state.action_title = f"Columns in `{selected_table}`"
            st.session_state.action_result = run_query(
                """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                params=[st.session_state.connected_database, selected_table],
            )

        if count_rows_btn and selected_table:
            st.session_state.active_action = "count_rows"
            st.session_state.action_title = f"Row Count for `{selected_table}`"
            selected_table_sql = quote_identifier(selected_table)
            st.session_state.action_result = run_query(f"SELECT COUNT(*) AS total_rows FROM {selected_table_sql}")

        if st.session_state.active_action == "show_tables" and not show_tables_btn:
            tables_offset = (page - 1) * row_limit
            st.session_state.action_result = run_query(
                """
                SELECT TABLE_NAME AS table_name
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME
                LIMIT %s OFFSET %s
                """,
                params=[st.session_state.connected_database, row_limit, tables_offset],
            )

        if st.session_state.action_result is not None:
            st.markdown(f"#### {st.session_state.action_title}")
            st.dataframe(st.session_state.action_result, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Action failed: {e}")
else:
    st.error("Could not connect automatically. Click Reconnect in the sidebar to retry.")


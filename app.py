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
if "databases" not in st.session_state:
    st.session_state.databases = []


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

    dbs = run_query("SHOW DATABASES")
    st.session_state.databases = dbs.iloc[:, 0].tolist()

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
        selected_db = st.selectbox(
            "Database",
            options=st.session_state.databases,
            index=(
                st.session_state.databases.index(st.session_state.connected_database)
                if st.session_state.connected_database in st.session_state.databases
                else 0
            ),
        )

        if selected_db != st.session_state.connected_database:
            st.session_state.connected_database = selected_db
            st.session_state.conn.select_db(selected_db)
            load_tables(selected_db)

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
    st.subheader("SQL Console (Read-only expected)")
    st.caption("Use SELECT statements to inspect data.")

    query = st.text_area("SQL Query", value="SELECT 1;", height=160)
    run_btn = st.button("Run Query")

    if run_btn:
        try:
            normalized = query.strip().lower()
            if not normalized.startswith("select") and not normalized.startswith("show") and not normalized.startswith("describe"):
                st.warning("Only SELECT/SHOW/DESCRIBE queries are allowed in this viewer.")
            else:
                result_df = run_query(query)
                st.dataframe(result_df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Query failed: {e}")
else:
    st.error("Could not connect automatically. Click Reconnect in the sidebar to retry.")

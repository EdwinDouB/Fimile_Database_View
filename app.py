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

    if not st.session_state.tables:
        st.session_state.selected_table = None
    elif st.session_state.selected_table not in st.session_state.tables:
        st.session_state.selected_table = st.session_state.tables[0]


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

    current_db = run_scalar_query("SELECT DATABASE() AS db_name")
    st.session_state.connected_database = current_db or DEFAULT_DB_CONFIG["database"]

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

        table_col, next_btn_col = st.columns([5, 1])
        with table_col:
            if st.session_state.tables:
                selected_table = st.selectbox("Table", options=st.session_state.tables, key="selected_table")
            else:
                selected_table = None
        with next_btn_col:
            st.write("")
            st.write("")
            next_table_btn = st.button("Next", use_container_width=True, disabled=not st.session_state.tables)

        if next_table_btn and st.session_state.tables:
            current_index = st.session_state.tables.index(st.session_state.selected_table)
            next_index = (current_index + 1) % len(st.session_state.tables)
            st.session_state.selected_table = st.session_state.tables[next_index]
            selected_table = st.session_state.selected_table
            st.rerun()

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


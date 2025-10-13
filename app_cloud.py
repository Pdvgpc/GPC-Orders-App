# ===== GPC Orders System – Cloud versie (login + audit + SQLite) =====
# Baseren op v1.7 met:
# - Login via auth.yaml (plain-text wachtwoorden voor eenvoudige start)
# - Audit-log (wie/wat/wanneer)
# - SQLite opslag (in plaats van CSV) – gelijktijdige writes oké
# - 2-dec prijzen (komma of punt), Enter-navigatie, veilige bewerkmodus
# - Zelfde UI-pagina's: Dashboard / Orders / Customers / Products

import os
import json
import sqlite3
from io import BytesIO
from datetime import datetime
import pandas as pd
import streamlit as st
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter
import yaml
import hashlib, hmac
# ------------------------------------------------------------
# [Start] App Config
# ------------------------------------------------------------
st.set_page_config(page_title="GPC Orders System", layout="wide")

# In cloud of lokaal: code kan in OneDrive staan; DB liever lokaal / werkdir
APP_DIR = os.getcwd()
DATA_DIR = os.path.join(APP_DIR, "GPCOF_data")
os.makedirs(DATA_DIR, exist_ok=True)

# CSV paden (voor eenmalige import, indien aanwezig)
PRODUCTS_CSV  = os.path.join(DATA_DIR, "products.csv")
CUSTOMERS_CSV = os.path.join(DATA_DIR, "customers.csv")
ORDERS_CSV    = os.path.join(DATA_DIR, "orders.csv")

# SQLite DB (één bestand naast de app)
DB_PATH = os.path.join(APP_DIR, "gpc.db")
# ------------------------------------------------------------
# [End] App Config
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Auth (auth.yaml met plain-text wachtwoorden)
# ------------------------------------------------------------
def load_auth():
    # auth.yaml moet naast dit script staan
    auth_path = os.path.join(APP_DIR, "auth.yaml")
    if not os.path.exists(auth_path):
        st.error("auth.yaml niet gevonden. Voeg auth.yaml toe in dezelfde map als app_cloud.py.")
        st.stop()
    with open(auth_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg

def login_panel():
    cfg = load_auth()
    users = cfg.get("credentials", {}).get("usernames", {})
    cookie = cfg.get("cookie", {"name":"gpc_auth","expiry_days":14})
    st.session_state.setdefault("auth_user", None)

    if st.session_state["auth_user"]:
        return st.session_state["auth_user"]

    st.markdown("### 🔐 Login")
    u = st.text_input("Gebruikersnaam")
    p = st.text_input("Wachtwoord", type="password")
    if st.button("Inloggen", type="primary"):
        if u in users:
            rec = users[u]
            # Eenvoudige start: plain-text wachtwoorden uit auth.yaml
            ok = False
            if "password_plain" in rec:
                ok = (str(p) == str(rec["password_plain"]))
            # (Later kun je hier bcrypt/sha256 toevoegen als je wilt)
            if ok:
                display_name = rec.get("name", u)
                st.session_state["auth_user"] = {
                    "username": u,
                    "name": display_name,
                    "email": rec.get("email","")
                }
                st.success(f"Ingelogd als {display_name}")
                st.experimental_rerun()
            else:
                st.error("Ongeldige gebruikersnaam/wachtwoord")
        else:
            st.error("Ongeldige gebruikersnaam/wachtwoord")
    st.stop()

user = login_panel()
st.sidebar.success(f"👤 Ingelogd als **{user['name']}**")
if st.sidebar.button("Logout"):
    st.session_state["auth_user"] = None
    st.experimental_rerun()
# ------------------------------------------------------------
# [End] Auth
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] SQLite storage
# ------------------------------------------------------------
def _db_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    with _db_conn() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS products (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          description TEXT,
          price REAL,
          four_week_availability INTEGER,
          supplier TEXT NOT NULL,
          modified_by TEXT,
          modified_at TEXT
        );
        CREATE TABLE IF NOT EXISTS customers (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT,
          modified_by TEXT,
          modified_at TEXT
        );
        CREATE TABLE IF NOT EXISTS orders (
          id INTEGER PRIMARY KEY,
          customer_id INTEGER,
          product_id INTEGER,
          quantity INTEGER,
          week_number INTEGER,
          year INTEGER,
          notes TEXT,
          sales_price REAL,
          modified_by TEXT,
          modified_at TEXT
        );
        CREATE TABLE IF NOT EXISTS audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          entity TEXT,
          entity_id INTEGER,
          action TEXT,
          user TEXT,
          ts TEXT,
          details TEXT
        );
        """)

def _now_iso():
    return datetime.now().isoformat(timespec="seconds")

def audit(entity, entity_id, action, actor, details: dict):
    with _db_conn() as con:
        con.execute(
            "INSERT INTO audit_log (entity, entity_id, action, user, ts, details) VALUES (?,?,?,?,?,?)",
            (entity, int(entity_id) if entity_id is not None else None, action, actor, _now_iso(), json.dumps(details or {}))
        )
        con.commit()

def _safe_to_sql(df: pd.DataFrame, table: str):
    with _db_conn() as con:
        df.to_sql(table, con, if_exists="replace", index=False)

def load_data():
    """Laad data uit SQLite. Als DB leeg is en CSV's bestaan, importeer eenmalig."""
    init_db()
    with _db_conn() as con:
        # Kijk of tabellen rijen hebben
        def count(table):
            try:
                return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except Exception:
                return 0

        has_any = (count("products") + count("customers") + count("orders")) > 0

        if not has_any:
            # Eenmalig CSV's importeren (als aanwezig)
            if os.path.exists(PRODUCTS_CSV):
                pd.read_csv(PRODUCTS_CSV).to_sql("products", con, if_exists="replace", index=False)
            if os.path.exists(CUSTOMERS_CSV):
                pd.read_csv(CUSTOMERS_CSV).to_sql("customers", con, if_exists="replace", index=False)
            if os.path.exists(ORDERS_CSV):
                pd.read_csv(ORDERS_CSV).to_sql("orders", con, if_exists="replace", index=False)

        prod = pd.read_sql_query("SELECT * FROM products", con)
        cust = pd.read_sql_query("SELECT * FROM customers", con)
        ords = pd.read_sql_query("SELECT * FROM orders", con)

    st.session_state.products = coerce_columns(prod, {
        "id":"Int64","name":"string","description":"string","price":"float",
        "four_week_availability":"Int64","supplier":"string","modified_by":"string","modified_at":"string"
    })
    st.session_state.customers = coerce_columns(cust, {
        "id":"Int64","name":"string","email":"string","modified_by":"string","modified_at":"string"
    })
    st.session_state.orders = coerce_columns(ords, {
        "id":"Int64","customer_id":"Int64","product_id":"Int64","quantity":"Int64",
        "week_number":"Int64","year":"Int64","notes":"string","sales_price":"float",
        "modified_by":"string","modified_at":"string"
    })

def save_data():
    """Schrijf huidige DataFrames naar SQLite."""
    init_db()
    _safe_to_sql(st.session_state.products, "products")
    _safe_to_sql(st.session_state.customers, "customers")
    _safe_to_sql(st.session_state.orders, "orders")
# ------------------------------------------------------------
# [End] SQLite storage
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Helpers (types/id/dates/inputs)
# ------------------------------------------------------------
def week_start_date(year: int, week: int):
    try:
        return datetime.fromisocalendar(int(year), int(week), 1).date()
    except Exception:
        return None

def coerce_columns(df: pd.DataFrame, types: dict) -> pd.DataFrame:
    df = df.copy()
    for col, dtype in types.items():
        if col not in df.columns:
            if dtype in ("Int64","Int32","int"):
                df[col] = pd.Series([], dtype="Int64")
            elif dtype in ("float","Float64"):
                df[col] = pd.Series([], dtype="float")
            else:
                df[col] = pd.Series([], dtype="string")
        else:
            try:
                if dtype in ("Int64","Int32","int"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype in ("float","Float64"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
                else:
                    df[col] = df[col].astype("string")
            except Exception:
                pass
    return df

def ensure_state():
    if "products" not in st.session_state or "customers" not in st.session_state or "orders" not in st.session_state:
        load_data()
    # type safety
    st.session_state.products = coerce_columns(st.session_state.products, {
        "id":"Int64","name":"string","description":"string","price":"float",
        "four_week_availability":"Int64","supplier":"string","modified_by":"string","modified_at":"string"
    })
    st.session_state.customers = coerce_columns(st.session_state.customers, {
        "id":"Int64","name":"string","email":"string","modified_by":"string","modified_at":"string"
    })
    st.session_state.orders = coerce_columns(st.session_state.orders, {
        "id":"Int64","customer_id":"Int64","product_id":"Int64","quantity":"Int64",
        "week_number":"Int64","year":"Int64","notes":"string","sales_price":"float",
        "modified_by":"string","modified_at":"string"
    })

def next_id(df: pd.DataFrame) -> int:
    if df.empty or "id" not in df.columns:
        return 1
    try:
        return int(pd.to_numeric(df["id"], errors="coerce").fillna(0).max()) + 1
    except Exception:
        return 1

def fmt_select_from_df(id_value, df_id_name: pd.DataFrame) -> str:
    try:
        if id_value is None:
            return ""
        iid = int(id_value)
        m = df_id_name.loc[pd.to_numeric(df_id_name["id"], errors="coerce") == iid, "name"]
        return "" if m.empty else str(m.iloc[0])
    except Exception:
        return ""

# Enter-navigatie tussen inputs
import streamlit.components.v1 as components
def enable_enter_navigation(submit_button_label: str):
    components.html(f"""
    <script>
    (function(){{
      const root = window.parent.document;
      function isEditable(el) {{
        if (!el) return false;
        const tag = el.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA') return true;
        if (el.getAttribute && el.getAttribute('contenteditable') === 'true') return true;
        return false;
      }}
      function getInputs() {{
        const all = Array.from(root.querySelectorAll('input, textarea'));
        return all.filter(el => !el.disabled && el.offsetParent !== null);
      }}
      function findSubmit(label) {{
        const btns = Array.from(root.querySelectorAll('button'));
        return btns.find(b => (b.innerText || '').trim() === label.trim());
      }}
      function focusNext(cur) {{
        const inputs = getInputs();
        const idx = inputs.indexOf(cur);
        if (idx === -1) return false;
        const next = inputs[idx+1];
        if (next) {{ next.focus(); return true; }}
        const btn = findSubmit("{submit_button_label}"); if (btn) btn.click(); return true;
      }}
      function handler(e){{
        if (e.key !== 'Enter') return;
        const active = root.activeElement;
        if (active && active.tagName === 'TEXTAREA' && e.shiftKey) return;
        if (isEditable(active)){{ e.preventDefault(); focusNext(active); }}
      }}
      root.addEventListener('keydown', handler, true);
    }})();
    </script>
    """, height=0)

# Geld-invoer: 12,34 of 12.34 -> float(2dp)
def money_input(label: str, value: float = 0.0, key: str = None, help: str = None):
    default_txt = f"{value:.2f}".replace(".", ",")
    raw = st.text_input(label, value=default_txt, key=key, help=help)
    txt = (raw or "").strip().replace("€", "").replace(" ", "").replace(",", ".")
    try:
        val = round(float(txt), 2)
        return val, True
    except Exception:
        return value, False
# ------------------------------------------------------------
# [End] Helpers
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Orders display + Excel export (pivot)
# ------------------------------------------------------------
def build_orders_display_df() -> pd.DataFrame:
    orders    = st.session_state.orders.copy()
    products  = st.session_state.products.copy()
    customers = st.session_state.customers.copy()

    if orders.empty:
        return pd.DataFrame(columns=[
            "Customer","Article","Description","Amount","Price","Sales Price","Supplier",
            "Weeknumber","Date of Weeknumber","Year","_OID","_CID","_PID"
        ])

    if not products.empty:
        prod = products.rename(columns={"id":"_PID_join"})
        orders = orders.merge(
            prod[["_PID_join","name","description","price","supplier"]],
            left_on="product_id", right_on="_PID_join", how="left"
        )
    else:
        orders["_PID_join"] = None
        orders["name"] = ""
        orders["description"] = ""
        orders["price"] = None
        orders["supplier"] = ""

    if not customers.empty:
        cust = customers.rename(columns={"id":"_CID_join"})
        orders = orders.merge(
            cust[["_CID_join","name"]],
            left_on="customer_id", right_on="_CID_join", how="left", suffixes=("","_cust")
        )
        orders["Customer"] = orders["name_cust"].fillna("")
    else:
        orders["_CID_join"] = None
        orders["Customer"] = ""

    orders["Article"] = orders["name"].fillna("")
    orders["Description"] = orders["description"].fillna("")
    orders["Amount"] = pd.to_numeric(orders["quantity"], errors="coerce").fillna(0).astype(int)
    orders["Price"] = pd.to_numeric(orders["price"], errors="coerce")
    orders["Sales Price"] = pd.to_numeric(orders["sales_price"], errors="coerce")
    orders["Supplier"] = orders["supplier"].astype("string").fillna("")
    orders["Weeknumber"] = pd.to_numeric(orders["week_number"], errors="coerce").fillna(0).astype(int)
    orders["Year"] = pd.to_numeric(orders["year"], errors="coerce").fillna(0).astype(int)
    orders["Date of Weeknumber"] = orders.apply(lambda r: week_start_date(r["Year"], r["Weeknumber"]), axis=1)

    orders["_OID"] = pd.to_numeric(orders["id"], errors="coerce").astype("Int64")
    orders["_CID"] = pd.to_numeric(orders["customer_id"], errors="coerce").astype("Int64")
    orders["_PID"] = pd.to_numeric(orders["product_id"], errors="coerce").astype("Int64")

    cols = ["Customer","Article","Description","Amount","Price","Sales Price","Supplier",
            "Weeknumber","Date of Weeknumber","Year","_OID","_CID","_PID"]
    df = orders.reindex(columns=cols).copy()
    for c in ["Customer","Article","Description","Supplier"]:
        df[c] = df[c].astype("string").fillna("")
    return df

def _excel_export_bytes(df: pd.DataFrame, title: str) -> BytesIO:
    df = df.copy().fillna("")
    wb = Workbook(); ws = wb.active; ws.title = "Export"
    title_cell = ws.cell(row=1, column=1, value=title)
    title_cell.font = Font(name="Aptos", bold=True, size=13)
    title_cell.alignment = Alignment(horizontal="left", vertical="center")
    start_row = 3
    for col_idx, col_name in enumerate(df.columns, start=1):
        cell = ws.cell(row=start_row, column=col_idx, value=str(col_name))
        cell.font = Font(name="Aptos", bold=True); cell.alignment = Alignment(vertical="center")
    for r_idx, (_, row) in enumerate(df.iterrows(), start=start_row + 1):
        for c_idx, val in enumerate(row.tolist(), start=1):
            ws.cell(row=r_idx, column=c_idx, value=val)
    data_rows = df.shape[0]; first_row = start_row; last_row = start_row + data_rows; last_col = df.shape[1]
    if data_rows > 0 and last_col > 0:
        ref = f"A{first_row}:{get_column_letter(last_col)}{last_row}"
        tbl = Table(displayName="Table1", ref=ref)
        tbl.tableStyleInfo = TableStyleInfo(name="TableStyleMedium11", showFirstColumn=False,
                                            showLastColumn=False, showRowStripes=True, showColumnStripes=False)
        ws.add_table(tbl)
    max_row = ws.max_row; max_col = ws.max_column
    for r in range(1, max_row + 1):
        for c in range(1, max_col + 1):
            cell = ws.cell(row=r, column=c)
            if cell.font and cell.font.bold:
                cell.font = Font(name="Aptos", bold=True, size=cell.font.sz or 11)
            else:
                cell.font = Font(name="Aptos", size=cell.font.sz or 11)
    for c in range(1, max_col + 1):
        col_letter = get_column_letter(c)
        values = [str(ws.cell(row=r, column=c).value or "") for r in range(1, max_row + 1)]
        max_len = max((len(v) for v in values), default=10)
        ws.column_dimensions[col_letter].width = min(max(12, max_len + 2), 35)
    buf = BytesIO(); wb.save(buf); buf.seek(0); return buf

def make_pivot_amount(df: pd.DataFrame, row_fields: list) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=row_fields)
    tmp = df.copy()
    tmp["Weeknumber"] = pd.to_numeric(tmp["Weeknumber"], errors="coerce").astype("Int64")
    tmp["Amount"]     = pd.to_numeric(tmp["Amount"], errors="coerce").fillna(0).astype(int)
    pvt = tmp.pivot_table(index=row_fields, columns="Weeknumber", values="Amount", aggfunc="sum", dropna=False)
    if isinstance(pvt.columns, pd.MultiIndex):
        pvt.columns = [c[-1] for c in pvt.columns]
    pvt = pvt.reindex(sorted(pvt.columns.dropna()), axis=1)
    pvt = pvt.astype("float").where(pd.notna(pvt), None)
    pvt.columns = [f"W{int(c)}" for c in pvt.columns.tolist()]
    pvt = pvt.reset_index()
    for c in row_fields:
        pvt[c] = pvt[c].astype("string").fillna("")
    return pvt
# ------------------------------------------------------------
# [End] Orders display + Excel export
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Commit helpers (apply editor changes before save)
# ------------------------------------------------------------
def _commit_customers_editor():
    key = "customers_editor_v17"
    if key in st.session_state and isinstance(st.session_state[key], pd.DataFrame):
        edited = st.session_state[key].copy()
        to_save = edited.drop(columns=["Select"], errors="ignore").rename(columns={
            "ID":"id","Name":"name","Email":"email"
        })
        to_save["modified_by"] = user["name"]
        to_save["modified_at"] = _now_iso()
        to_save = coerce_columns(to_save, {"id":"int","name":"str","email":"str","modified_by":"str","modified_at":"str"})
        st.session_state.customers = to_save
        # audit per rij (optioneel)
        for _, r in to_save.iterrows():
            audit("customer", r["id"], "update", user["name"], {"name": r.get("name","")})
        return True
    return False

def _commit_products_editor():
    key = "product_editor_v17"
    if key in st.session_state and isinstance(st.session_state[key], pd.DataFrame):
        edited = st.session_state[key].copy()
        to_save = edited.drop(columns=["Select"], errors="ignore").rename(columns={
            "ID":"id","Name":"name","Description":"description","Price":"price",
            "4w Availability":"four_week_availability","Supplier":"supplier"
        })
        if "price" in to_save.columns:
            to_save["price"] = to_save["price"].astype(str).str.replace(",", ".", regex=False)
            to_save["price"] = pd.to_numeric(to_save["price"], errors="coerce").round(2)
        to_save["modified_by"] = user["name"]
        to_save["modified_at"] = _now_iso()
        to_save = coerce_columns(to_save, {
            "id":"int","name":"str","description":"str","price":"float",
            "four_week_availability":"int","supplier":"str","modified_by":"str","modified_at":"str"
        })
        st.session_state.products = to_save
        for _, r in to_save.iterrows():
            audit("product", r["id"], "update", user["name"], {"name": r.get("name","")})
        return True
    return False

def _commit_orders_editor():
    key = "orders_editor_v17"
    if key in st.session_state and isinstance(st.session_state[key], pd.DataFrame):
        edited = st.session_state[key].copy()
        base = st.session_state.orders.set_index("id").copy()
        changed_ids = []
        for _oid, row in edited.iterrows():
            if _oid in base.index:
                changed = False
                if pd.notna(row.get("Amount")):
                    base.at[_oid, "quantity"] = int(row["Amount"]); changed = True
                if pd.notna(row.get("Weeknumber")):
                    base.at[_oid, "week_number"] = int(row["Weeknumber"]); changed = True
                if pd.notna(row.get("Year")):
                    base.at[_oid, "year"] = int(row["Year"]); changed = True
                sp = row.get("Sales Price")
                if pd.notna(sp) and sp != "":
                    try:
                        sp_norm = float(sp.replace(",", ".")) if isinstance(sp, str) else float(sp)
                        base.at[_oid, "sales_price"] = round(sp_norm, 2); changed = True
                    except Exception:
                        pass
                if changed:
                    base.at[_oid, "modified_by"] = user["name"]
                    base.at[_oid, "modified_at"] = _now_iso()
                    changed_ids.append(_oid)
        st.session_state.orders = base.reset_index()
        for oid in changed_ids:
            audit("order", int(oid), "update", user["name"], {"fields":"row edit"})
        return True
    return False

def commit_all_editors():
    changed = []
    if _commit_customers_editor(): changed.append("customers")
    if _commit_products_editor():  changed.append("products")
    if _commit_orders_editor():    changed.append("orders")
    return changed
# ------------------------------------------------------------
# [End] Commit helpers
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Init + Sidebar
# ------------------------------------------------------------
ensure_state()
st.sidebar.title("🌿 GPC Orders System")
page = st.sidebar.radio("Navigatie", ["Dashboard", "Orders", "Customers", "Products", "Audit"])

if st.sidebar.button("💾 Save now"):
    changed = commit_all_editors()
    save_data()
    if changed:
        st.sidebar.success(f"Wijzigingen in {', '.join(changed)} toegepast en opgeslagen.")
    else:
        st.sidebar.success("Data opgeslagen.")
# ------------------------------------------------------------
# [End] Init + Sidebar
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Pages
# ------------------------------------------------------------
# Dashboard
if page == "Dashboard":
    st.title("📊 Dashboard")
    orders = st.session_state.orders; products = st.session_state.products
    if orders.empty:
        st.info("Nog geen data. Voeg eerst producten/klanten/orders toe.")
    else:
        df = (orders.merge(products[["id","name"]], left_on="product_id", right_on="id", how="left")
                    .rename(columns={"name":"Product"}).drop(columns=["id_y"], errors="ignore"))
        years = sorted(df["year"].dropna().astype(int).unique().tolist())
        sel_year = st.selectbox("Jaar", years,
                                index=years.index(datetime.now().year) if datetime.now().year in years else 0)
        per_prod = (df[df["year"] == sel_year].groupby("Product", dropna=False)["quantity"].sum()
                        .reset_index(name="Total Sold").sort_values("Total Sold", ascending=False))
        st.markdown(f"### Orders per Product in {sel_year}")
        st.dataframe(per_prod, use_container_width=True)

# Orders
elif page == "Orders":
    st.title("📦 Orders")

    st.subheader("➕ Nieuwe order")
    if st.session_state.customers.empty or st.session_state.products.empty:
        st.warning("Je hebt klanten én producten nodig om een order toe te voegen.")
    else:
        with st.form("add_order_form", clear_on_submit=True):
            cA, cB = st.columns(2)
            with cA:
                cust_ids = st.session_state.customers["id"].dropna().astype(int).tolist()
                prod_ids = st.session_state.products["id"].dropna().astype(int).tolist()

                sel_customer = st.selectbox(
                    "Customer *",
                    options=[None] + cust_ids,
                    format_func=lambda i: "" if i is None else fmt_select_from_df(i, st.session_state.customers),
                    index=0,
                )
                sel_product = st.selectbox(
                    "Article (Product) *",
                    options=[None] + prod_ids,
                    format_func=lambda i: "" if i is None else fmt_select_from_df(i, st.session_state.products),
                    index=0,
                )
                amount = st.number_input("Amount *", min_value=1, step=1, value=1)

            with cB:
                sales_price, sp_ok = money_input(
                    "Sales Price (optional)",
                    value=0.00,
                    key="oi_sales_price",
                    help="Gebruik 12,34 of 12.34 (2 decimalen)."
                )
                weeks_txt = st.text_input("Weeknumbers * (comma separated, e.g. 4,8,12)", value="")
                year = st.number_input("Year *", min_value=2020, max_value=2100, step=1, value=datetime.now().year)

            enable_enter_navigation("Order(s) toevoegen")
            submitted = st.form_submit_button("Order(s) toevoegen")

            if submitted:
                errors = []
                if sel_customer is None: errors.append("Kies een Customer.")
                if sel_product is None: errors.append("Kies een Product.")
                if not sp_ok: errors.append("Sales Price is ongeldig. Gebruik 12,34 of 12.34.")

                weeks, bad = [], []
                if not weeks_txt.strip():
                    errors.append("Vul ten minste één weeknummer in.")
                else:
                    for p in [w.strip() for w in weeks_txt.split(",") if w.strip()]:
                        try:
                            w = int(p)
                            if 1 <= w <= 53: weeks.append(w)
                            else: bad.append(p)
                        except Exception:
                            bad.append(p)
                weeks = sorted(list(dict.fromkeys(weeks)))
                if bad:
                    errors.append(f"Ongeldige weeknummers: {', '.join(bad)} (toegestaan: 1..53)")

                if errors:
                    for e in errors: st.error(e)
                else:
                    base_id = next_id(st.session_state.orders)
                    rows = []
                    for idx, w in enumerate(weeks):
                        rows.append({
                            "id": base_id + idx,
                            "customer_id": int(sel_customer),
                            "product_id": int(sel_product),
                            "quantity": int(amount),
                            "sales_price": float(sales_price) if sales_price is not None else None,
                            "week_number": int(w),
                            "year": int(year),
                            "modified_by": user["name"],
                            "modified_at": _now_iso()
                        })
                    st.session_state.orders = pd.concat([st.session_state.orders, pd.DataFrame(rows)], ignore_index=True)
                    save_data()
                    for r in rows:
                        audit("order", r["id"], "create", user["name"], {"customer_id": r["customer_id"], "product_id": r["product_id"]})
                    st.success(f"Toegevoegd: {len(rows)} order(s) voor weken: {', '.join(map(str, weeks))}")
                    st.rerun()

    st.markdown("---")

    base_df = build_orders_display_df()

    with st.expander("🔎 Filters (tabel & export)"):
        f1, f2, f3, f4 = st.columns(4)
        with f1:
            flt_customer = st.multiselect("Customer", options=sorted(base_df["Customer"].dropna().astype(str).unique().tolist()))
        with f2:
            flt_supplier = st.multiselect("Supplier", options=sorted(base_df["Supplier"].dropna().astype(str).unique().tolist()))
        with f3:
            flt_article = st.multiselect("Article", options=sorted(base_df["Article"].dropna().astype(str).unique().tolist()))
        with f4:
            unique_weeks = sorted(base_df["Weeknumber"].dropna().astype(int).unique().tolist())
            flt_weeks = st.multiselect("Weeknumber", options=unique_weeks)

    filtered_df = base_df.copy()
    if flt_customer: filtered_df = filtered_df[filtered_df["Customer"].isin(flt_customer)]
    if flt_supplier: filtered_df = filtered_df[filtered_df["Supplier"].isin(flt_supplier)]
    if flt_article:  filtered_df = filtered_df[filtered_df["Article"].isin(flt_article)]
    if flt_weeks:    filtered_df = filtered_df[filtered_df["Weeknumber"].isin(flt_weeks)]

    if filtered_df.empty:
        st.info("Geen orders gevonden (controleer je filters).")
    else:
        show_cols = ["Customer","Article","Description","Amount","Price","Sales Price","Supplier",
                     "Weeknumber","Date of Weeknumber","Year"]
        display_df = filtered_df[show_cols + ["_OID"]].copy()

        editor_df = display_df.copy()
        editor_df.insert(0, "Select", False)
        editor_df.set_index("_OID", inplace=True)
        for c in ["Customer","Article","Description","Supplier"]:
            editor_df[c] = editor_df[c].astype("string")
        editor_df["Date of Weeknumber"] = editor_df["Date of Weeknumber"].astype(str)
        editor_df["Sales Price"] = (
            editor_df["Sales Price"]
            .apply(lambda v: "" if pd.isna(v) else f"{float(v):.2f}".replace(".", ","))
            .astype("string")
        )

        st.subheader("📋 Orders (bewerken, selecteren en verwijderen)")
        edited = st.data_editor(
            editor_df,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "Select": st.column_config.CheckboxColumn(help="Selecteer voor verwijderen"),
                "Amount": st.column_config.NumberColumn(format="%d", min_value=0),
                "Weeknumber": st.column_config.NumberColumn(format="%d", min_value=1, max_value=53),
                "Year": st.column_config.NumberColumn(format="%d", min_value=2020, max_value=2100),
                "Sales Price": st.column_config.TextColumn(help="Gebruik 12,34 of 12.34"),
                "Price": st.column_config.NumberColumn(format="%.2f", min_value=0.0, step=0.01, disabled=True),
                "Date of Weeknumber": st.column_config.TextColumn(disabled=True),
                "Supplier": st.column_config.TextColumn(disabled=True),
                "Customer": st.column_config.TextColumn(disabled=True),
                "Article": st.column_config.TextColumn(disabled=True),
                "Description": st.column_config.TextColumn(disabled=True),
            },
            hide_index=False,
            disabled=["Customer","Article","Description","Supplier","Date of Weeknumber","Price"],
            key="orders_editor_v17",
        )

        selected_ids = edited.index[edited["Select"] == True].tolist()
        c1, c2, _ = st.columns([1,1,6])

        with c1:
            if st.button("🗑️ Verwijder geselecteerde orders", use_container_width=True):
                if not selected_ids:
                    st.warning("Selecteer eerst één of meer orders.")
                else:
                    st.session_state.orders = st.session_state.orders[~st.session_state.orders["id"].isin(selected_ids)]
                    save_data()
                    for oid in selected_ids:
                        audit("order", int(oid), "delete", user["name"], {})
                    st.success(f"Verwijderd: {selected_ids}"); st.rerun()

        with c2:
            if st.button("💾 Opslaan wijzigingen", use_container_width=True):
                _commit_orders_editor()
                save_data()
                st.success("Wijzigingen opgeslagen."); st.rerun()

        st.markdown("### ⬇️ Export Excel (pivot per week)")
        cust_rows = ["Customer","Article","Description","Sales Price","Supplier"]
        cust_pivot = make_pivot_amount(filtered_df[cust_rows + ["Weeknumber","Amount"]], cust_rows)
        sup_rows  = ["Supplier","Article","Description","Customer"]
        sup_pivot = make_pivot_amount(filtered_df[sup_rows + ["Weeknumber","Amount"]], sup_rows)
        cust_disabled = cust_pivot.empty; sup_disabled = sup_pivot.empty
        cust_file = _excel_export_bytes(cust_pivot, f"GPC Orders {datetime.now().year}") if not cust_disabled else None
        sup_file  = _excel_export_bytes(sup_pivot,  f"GPC Orders {datetime.now().year}") if not sup_disabled else None
        e1, e2 = st.columns(2)
        with e1:
            st.download_button("⬇️ Export Excel Customer",
                data=cust_file.getvalue() if cust_file else b"",
                file_name=f"GPC_Orders_Customer_{datetime.now().year}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, disabled=cust_disabled)
        with e2:
            st.download_button("⬇️ Export Excel Supplier",
                data=sup_file.getvalue() if sup_file else b"",
                file_name=f"GPC_Orders_Supplier_{datetime.now().year}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, disabled=sup_disabled)

# Customers
elif page == "Customers":
    st.title("👥 Customers")

    st.subheader("➕ Nieuwe klant")
    with st.form("add_customer_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            name_c = st.text_input("Naam *")
        with c2:
            email_c = st.text_input("Email")
        enable_enter_navigation("Klant toevoegen")
        ok = st.form_submit_button("Klant toevoegen")

    if ok and name_c.strip():
        new_row = {"id": next_id(st.session_state.customers), "name": name_c.strip(), "email": email_c.strip(),
                   "modified_by": user["name"], "modified_at": _now_iso()}
        st.session_state.customers = pd.concat([st.session_state.customers, pd.DataFrame([new_row])], ignore_index=True)
        save_data()
        audit("customer", new_row["id"], "create", user["name"], {"name": new_row["name"]})
        st.success(f"Klant '{name_c}' toegevoegd."); st.rerun()

    st.markdown("---")

    if st.session_state.customers.empty:
        st.info("Nog geen klanten.")
    else:
        view = st.session_state.customers.copy().rename(columns={"id":"ID","name":"Name","email":"Email"})
        view.insert(0, "Select", False)
        st.subheader("✏️ Bewerken & Verwijderen")
        edited = st.data_editor(
            view, use_container_width=True, hide_index=True, num_rows="dynamic",
            column_config={
                "Select": st.column_config.CheckboxColumn(),
                "ID": st.column_config.NumberColumn(disabled=True),
                "Name": st.column_config.TextColumn(),
                "Email": st.column_config.TextColumn(),
            },
            key="customers_editor_v17"
        )

        c1, c2 = st.columns(2)
        with c1:
            if st.button("💾 Wijzigingen opslaan (Customers)", use_container_width=True):
                _commit_customers_editor()
                save_data()
                st.success("Customer-wijzigingen opgeslagen."); st.rerun()
        with c2:
            sel_ids = edited.loc[edited["Select"] == True, "ID"].tolist()
            if st.button("🗑️ Verwijder geselecteerde klanten", use_container_width=True):
                if not sel_ids:
                    st.warning("Selecteer eerst één of meer klanten.")
                else:
                    st.session_state.customers = st.session_state.customers[~st.session_state.customers["id"].isin(sel_ids)]
                    save_data()
                    for cid in sel_ids:
                        audit("customer", int(cid), "delete", user["name"], {})
                    st.success(f"Verwijderd: {sel_ids}"); st.rerun()

# Products
elif page == "Products":
    st.title("🪴 Products")

    st.subheader("➕ Nieuw product")
    with st.form("add_product_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            name_p = st.text_input("Product Name *")
        with c2:
            price_p, price_ok = money_input("Price (€)", value=0.00, key="pi_price",
                                            help="Gebruik 12,34 of 12.34 (2 decimalen).")
            fourw = st.number_input("4 Week Availability", min_value=0, value=0, step=1)
            supplier = st.text_input("Supplier *")
            description = st.text_area("Description")
        enable_enter_navigation("Product toevoegen")
        ok = st.form_submit_button("Product toevoegen")

    if ok:
        errs = []
        if not name_p.strip(): errs.append("Vul een productnaam in.")
        if not supplier.strip(): errs.append("Vul een supplier in.")
        if not price_ok: errs.append("Price is ongeldig. Gebruik 12,34 of 12.34.")
        if errs:
            for e in errs: st.error(e)
        else:
            new_row = {
                "id": next_id(st.session_state.products),
                "name": name_p.strip(),
                "description": description.strip(),
                "price": float(price_p),
                "four_week_availability": int(fourw),
                "supplier": supplier.strip(),
                "modified_by": user["name"],
                "modified_at": _now_iso()
            }
            st.session_state.products = pd.concat([st.session_state.products, pd.DataFrame([new_row])], ignore_index=True)
            save_data()
            audit("product", new_row["id"], "create", user["name"], {"name": new_row["name"]})
            st.success(f"Product '{name_p.strip()}' toegevoegd."); st.rerun()

    st.markdown("---")

    # Veilige bewerkmodus
    with st.expander("🛟 Veilige bewerkmodus (als wijzigen in de tabel niet lukt)"):
        if st.session_state.products.empty:
            st.info("Geen producten om te bewerken.")
        else:
            _pv = st.session_state.products.copy()
            _pv = coerce_columns(_pv, {
                "id":"int","name":"str","description":"str","price":"float",
                "four_week_availability":"int","supplier":"str","modified_by":"str","modified_at":"str"
            })
            _pv["label"] = _pv.apply(lambda r: f"{r['name']} ({r['supplier']}) [ID {r['id']}]", axis=1)
            _labels = _pv["label"].tolist()
            _id_by_label = dict(zip(_pv["label"], _pv["id"]))

            sel_label = st.selectbox("Kies product", options=_labels)
            sel_id = _id_by_label.get(sel_label)

            if sel_id is not None:
                row = _pv.loc[_pv["id"] == sel_id].iloc[0]

                with st.form(f"safe_edit_product_{sel_id}", clear_on_submit=False):
                    c1, c2 = st.columns(2)
                    with c1:
                        new_name = st.text_input("Name", value=row["name"])
                        new_supplier = st.text_input("Supplier", value=row["supplier"])
                        new_fourw = st.number_input("4 Week Availability", min_value=0, step=1,
                                                    value=int(row["four_week_availability"]))
                    with c2:
                        new_price, ok_price = money_input("Price (€)", value=float(row["price"] or 0.0),
                                                          key=f"safep_price_{sel_id}",
                                                          help="Gebruik 12,34 of 12.34")
                        new_desc = st.text_area("Description", value=row["description"] or "")

                    submit_safe = st.form_submit_button("💾 Opslaan (veilige modus)")

                if submit_safe:
                    errs = []
                    if not new_name.strip(): errs.append("Naam mag niet leeg zijn.")
                    if not new_supplier.strip(): errs.append("Supplier mag niet leeg zijn.")
                    if not ok_price: errs.append("Price is ongeldig. Gebruik 12,34 of 12.34.")
                    if errs:
                        for e in errs: st.error(e)
                    else:
                        base = st.session_state.products.copy()
                        base = coerce_columns(base, {
                            "id":"int","name":"str","description":"str","price":"float",
                            "four_week_availability":"int","supplier":"str","modified_by":"str","modified_at":"str"
                        })
                        idx = base.index[base["id"] == sel_id]
                        if len(idx) == 1:
                            i = idx[0]
                            base.at[i, "name"] = new_name.strip()
                            base.at[i, "supplier"] = new_supplier.strip()
                            base.at[i, "four_week_availability"] = int(new_fourw)
                            base.at[i, "description"] = (new_desc or "").strip()
                            base.at[i, "price"] = float(new_price)
                            base.at[i, "modified_by"] = user["name"]
                            base.at[i, "modified_at"] = _now_iso()
                            st.session_state.products = base
                            save_data()
                            audit("product", int(sel_id), "update", user["name"], {"fields":"safe editor"})
                            st.success("Product bijgewerkt en opgeslagen ✅")
                            st.rerun()
                        else:
                            st.error("Kon de rij niet uniek vinden op ID.")

    if st.session_state.products.empty:
        st.info("Nog geen producten.")
    else:
        prod_view = st.session_state.products.copy()
        prod_view = coerce_columns(prod_view, {
            "id":"int","name":"str","description":"str","price":"float","four_week_availability":"int","supplier":"str",
            "modified_by":"str","modified_at":"str"
        })
        prod_view = prod_view.rename(columns={
            "id":"ID","name":"Name","description":"Description","price":"Price",
            "four_week_availability":"4w Availability","supplier":"Supplier",
            "modified_by":"Modified By","modified_at":"Modified At"
        })
        prod_view.insert(0, "Select", False)
        prod_view["ID"] = pd.to_numeric(prod_view["ID"], errors="coerce").fillna(0).astype(int)
        prod_view["4w Availability"] = pd.to_numeric(prod_view["4w Availability"], errors="coerce").fillna(0).astype(int)
        for _c in ["Name","Description","Supplier","Modified By","Modified At"]:
            prod_view[_c] = prod_view[_c].astype("string").fillna("")
        prod_view["Price"] = (
            pd.to_numeric(prod_view["Price"], errors="coerce")
              .apply(lambda v: "" if pd.isna(v) else f"{float(v):.2f}".replace(".", ","))
              .astype("string")
        )

        st.subheader("✏️ Bewerken & Verwijderen")
        edited = st.data_editor(
            prod_view,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config={
                "Select": st.column_config.CheckboxColumn(),
                "ID": st.column_config.NumberColumn(disabled=True),
                "Name": st.column_config.TextColumn(),
                "Description": st.column_config.TextColumn(),
                "Price": st.column_config.TextColumn(help="Gebruik 12,34 of 12.34"),
                "4w Availability": st.column_config.NumberColumn(format="%d", min_value=0, step=1),
                "Supplier": st.column_config.TextColumn(),
                "Modified By": st.column_config.TextColumn(disabled=True),
                "Modified At": st.column_config.TextColumn(disabled=True),
            },
            key="product_editor_v17",
        )

        c1, c2 = st.columns(2)
        with c1:
            if st.button("💾 Wijzigingen opslaan (Products)", use_container_width=True):
                _commit_products_editor()
                save_data(); st.success("Product-wijzigingen opgeslagen."); st.rerun()
        with c2:
            del_ids = edited.loc[edited["Select"] == True, "ID"].tolist()
            if st.button("🗑️ Verwijder geselecteerde producten", use_container_width=True):
                if not del_ids:
                    st.warning("Selecteer eerst één of meer producten.")
                else:
                    st.session_state.products = st.session_state.products[~st.session_state.products["id"].isin(del_ids)]
                    save_data()
                    for pid in del_ids:
                        audit("product", int(pid), "delete", user["name"], {})
                    st.success(f"Verwijderd: {del_ids}"); st.rerun()

# Audit
elif page == "Audit":
    st.title("🧾 Audit log")
    with _db_conn() as con:
        try:
            logs = pd.read_sql_query("SELECT id, ts, user, entity, entity_id, action, details FROM audit_log ORDER BY id DESC LIMIT 500", con)
            if logs.empty:
                st.info("Nog geen audit entries.")
            else:
                st.dataframe(logs, use_container_width=True)
        except Exception as e:
            st.error(f"Kan audit log niet lezen: {e}")
# ------------------------------------------------------------
# [End] Pages
# ------------------------------------------------------------


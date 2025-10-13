# ===== GPC Orders (cloud, GitHub storage) =====
import os
from io import BytesIO, StringIO
from datetime import datetime
import base64, json, requests
from typing import Optional

import pandas as pd
import streamlit as st
import yaml

import hashlib, hmac
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter
import streamlit.components.v1 as components

# ---- Secrets (vereist voor GitHub storage) ----
from streamlit.runtime.secrets import secrets

# ------------------------------------------------------------
# [Start] App Config
# ------------------------------------------------------------
st.set_page_config(page_title="GPC Orders System", layout="wide")

HERE = os.path.dirname(__file__)
AUTH_YAML = os.path.join(HERE, "auth.yaml")  # auth.yaml blijft in de repo
# ------------------------------------------------------------
# [End] App Config
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] GitHub storage helpers (CSV’s in repo/branch main)
# ------------------------------------------------------------
def _gh_headers():
    return {
        "Authorization": f"Bearer {secrets['GITHUB_TOKEN']}",
        "Accept": "application/vnd.github+json",
    }

def _gh_api(path: str) -> str:
    owner = secrets["GITHUB_OWNER"]
    repo  = secrets["GITHUB_REPO"]
    return f"https://api.github.com/repos/{owner}/{repo}{path}"

def _gh_get_text(path_in_repo: str) -> Optional[str]:
    """Leest een bestand (text) uit de repo. Retourneert None als het niet bestaat."""
    url = _gh_api(f"/contents/{path_in_repo}")
    r = requests.get(url, headers=_gh_headers())
    if r.status_code == 200:
        data = r.json()
        return base64.b64decode(data["content"]).decode("utf-8", errors="ignore")
    if r.status_code == 404:
        return None
    st.error(f"GitHub read error {r.status_code}: {r.text[:200]}")
    return ""

def _gh_put_text(path_in_repo: str, content_text: str, msg: str):
    """Schrijft/maakt text-bestand naar de repo (branch main)."""
    url = _gh_api(f"/contents/{path_in_repo}")
    r = requests.get(url, headers=_gh_headers())
    sha = r.json().get("sha") if r.status_code == 200 else None

    payload = {
        "message": msg,
        "content": base64.b64encode(content_text.encode("utf-8")).decode("ascii"),
        "branch": "main",
    }
    if sha:
        payload["sha"] = sha

    r2 = requests.put(url, headers=_gh_headers(), data=json.dumps(payload))
    if r2.status_code not in (200, 201):
        st.error(f"GitHub write error {r2.status_code}: {r2.text[:200]}")

def _gh_get_csv(path_in_repo: str) -> Optional[pd.DataFrame]:
    """Leest CSV in als DataFrame. None wanneer het bestand niet bestaat."""
    txt = _gh_get_text(path_in_repo)
    if txt is None:
        return None
    if not txt.strip():
        return pd.DataFrame()
    try:
        return pd.read_csv(StringIO(txt))
    except Exception:
        return pd.DataFrame()

def _gh_put_csv(path_in_repo: str, df: pd.DataFrame, msg: str):
    """Schrijft DataFrame als CSV naar repo."""
    csv_txt = df.to_csv(index=False)
    _gh_put_text(path_in_repo, csv_txt, msg)
# ------------------------------------------------------------
# [End] GitHub storage helpers
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Auth helpers
# ------------------------------------------------------------
def load_auth() -> dict:
    try:
        with open(AUTH_YAML, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        # Auth als YAML niet te lezen is, toon melding
        st.error("auth.yaml niet gevonden of ongeldig. Voeg auth.yaml toe aan de repo.")
        return {}

def login_panel():
    cfg = load_auth()
    users = cfg.get("credentials", {}).get("usernames", {})
    st.session_state.setdefault("auth_user", None)

    if st.session_state["auth_user"]:
        return st.session_state["auth_user"]

    st.markdown("### 🔐 Login")
    u = st.text_input("Gebruikersnaam")
    p = st.text_input("Wachtwoord", type="password")

    if st.button("Inloggen", type="primary"):
        rec = users.get(u)
        if rec:
            ok = False
            # 1) Veilig: SHA-256 hash check
            if isinstance(rec.get("password_sha256"), str) and rec["password_sha256"]:
                try:
                    entered = hashlib.sha256(str(p).encode("utf-8")).hexdigest()
                    ok = hmac.compare_digest(entered, rec["password_sha256"])
                except Exception:
                    ok = False
            # 2) Fallback: plain-text
            elif "password_plain" in rec:
                ok = (str(p) == str(rec["password_plain"]))

            if ok:
                st.session_state["auth_user"] = {
                    "username": u,
                    "name": rec.get("name", u),
                    "email": rec.get("email", "")
                }
                st.success(f"Ingelogd als {st.session_state['auth_user']['name']}")
                st.rerun()
            else:
                st.error("Ongeldige gebruikersnaam/wachtwoord")
        else:
            st.error("Ongeldige gebruikersnaam/wachtwoord")

    st.stop()
# ------------------------------------------------------------
# [End] Auth helpers
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Helpers: types, load/save, id's, date helpers
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
            if dtype in ("Int64", "Int32", "int"):
                df[col] = pd.Series([], dtype="Int64")
            elif dtype in ("float", "Float64"):
                df[col] = pd.Series([], dtype="float")
            else:
                df[col] = pd.Series([], dtype="string")
        else:
            try:
                if dtype in ("Int64", "Int32", "int"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                elif dtype in ("float", "Float64"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
                else:
                    df[col] = df[col].astype("string")
            except Exception:
                pass
    return df

def load_data():
    """Laadt CSV’s uit GitHub (map uit secrets: DATA_DIR)."""
    repo_dir = secrets.get("DATA_DIR", "data")

    # PRODUCTS
    g = _gh_get_csv(f"{repo_dir}/products.csv")
    if g is None:
        prod = pd.DataFrame(columns=["id","name","description","price","four_week_availability","supplier"])
    else:
        prod = g
    prod = coerce_columns(prod, {
        "id":"Int64","name":"string","description":"string","price":"float",
        "four_week_availability":"Int64","supplier":"string",
    })
    st.session_state.products = prod

    # CUSTOMERS
    g = _gh_get_csv(f"{repo_dir}/customers.csv")
    if g is None:
        cust = pd.DataFrame(columns=["id","name","email"])
    else:
        cust = g
    cust = coerce_columns(cust, {"id":"Int64","name":"string","email":"string"})
    st.session_state.customers = cust

    # ORDERS
    g = _gh_get_csv(f"{repo_dir}/orders.csv")
    if g is None:
        ords = pd.DataFrame(columns=[
            "id","customer_id","product_id","quantity","week_number","year","notes","sales_price"
        ])
    else:
        ords = g
    ords = coerce_columns(ords, {
        "id":"Int64","customer_id":"Int64","product_id":"Int64","quantity":"Int64",
        "week_number":"Int64","year":"Int64","notes":"string","sales_price":"float",
    })
    st.session_state.orders = ords

def save_data():
    """Schrijft CSV’s terug naar GitHub (branch main)."""
    repo_dir = secrets.get("DATA_DIR", "data")
    if "products" in st.session_state:
        _gh_put_csv(f"{repo_dir}/products.csv", st.session_state.products, "update products.csv")
    if "customers" in st.session_state:
        _gh_put_csv(f"{repo_dir}/customers.csv", st.session_state.customers, "update customers.csv")
    if "orders" in st.session_state:
        _gh_put_csv(f"{repo_dir}/orders.csv", st.session_state.orders, "update orders.csv")

def ensure_state():
    if "products" not in st.session_state or "customers" not in st.session_state or "orders" not in st.session_state:
        load_data()
    st.session_state.products = coerce_columns(st.session_state.products, {
        "id":"Int64","name":"string","description":"string","price":"float",
        "four_week_availability":"Int64","supplier":"string",
    })
    st.session_state.customers = coerce_columns(st.session_state.customers, {
        "id":"Int64","name":"string","email":"string",
    })
    st.session_state.orders = coerce_columns(st.session_state.orders, {
        "id":"Int64","customer_id":"Int64","product_id":"Int64","quantity":"Int64",
        "week_number":"Int64","year":"Int64","notes":"string","sales_price":"float",
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
# ------------------------------------------------------------
# [End] Helpers: types, load/save, id's, date helpers
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] UI helper: Enter = volgende veld / submit
# ------------------------------------------------------------
def enable_enter_navigation(submit_button_label: str):
    components.html(f"""
    <script>
    (function() {{
      const root = window.parent.document;
      function isEditable(el) {{
        if (!el) return false;
        const tag = el.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA') return true;
        if (el.getAttribute && el.getAttribute('contenteditable') === 'true') return true;
        return false;
      }}
      function getFocusableInputs() {{
        const all = Array.from(root.querySelectorAll('input, textarea'));
        return all.filter(el => !el.disabled && el.offsetParent !== null);
      }}
      function findSubmitButton(label) {{
        const btns = Array.from(root.querySelectorAll('button'));
        return btns.find(b => (b.innerText || '').trim() === label.trim());
      }}
      function focusNext(current) {{
        const inputs = getFocusableInputs();
        const idx = inputs.indexOf(current);
        if (idx === -1) return false;
        const next = inputs[idx + 1];
        if (next) {{ next.focus(); if (next.setSelectionRange && next.value != null) {{
          const len = next.value.length; try {{ next.setSelectionRange(len, len); }} catch(e) {{}} }} return true; }}
        const btn = findSubmitButton("{submit_button_label}"); if (btn) btn.click(); return true;
      }}
      function handler(e) {{
        if (e.key !== 'Enter') return;
        const active = root.activeElement;
        if (active && active.tagName === 'TEXTAREA' && e.shiftKey) return;
        if (isEditable(active)) {{ e.preventDefault(); focusNext(active); }}
      }}
      root.addEventListener('keydown', handler, true);
    }})();
    </script>
    """, height=0)
# ------------------------------------------------------------
# [End] UI helper
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Helpers: money input (komma/punt, 2 dec)
# ------------------------------------------------------------
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
# [End] Helpers: money input
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Helpers: Orders weergave + Excel export (pivot)
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

    view_cols = ["Customer","Article","Description","Amount","Price","Sales Price","Supplier",
                 "Weeknumber","Date of Weeknumber","Year","_OID","_CID","_PID"]
    df = orders.reindex(columns=view_cols).copy()
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
# [End] Helpers: Orders weergave + Excel export (pivot)
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Init state + Sidebar
# ------------------------------------------------------------
ensure_state()

user = login_panel()

st.sidebar.title("🌿 GPC Orders System")
st.sidebar.success(f"👤 Ingelogd als **{user['name']}**")

page = st.sidebar.radio("Navigatie", ["Dashboard", "Orders", "Customers", "Products"])

if st.sidebar.button("Logout"):
    st.session_state["auth_user"] = None
    st.rerun()

if st.sidebar.button("💾 Save now"):
    save_data()
    st.sidebar.success("Data opgeslagen in GitHub.")
# ------------------------------------------------------------
# [End] Init state + Sidebar
# ------------------------------------------------------------


# ------------------------------------------------------------
# [Start] Dashboard
# ------------------------------------------------------------
if page == "Dashboard":
    st.title("📊 Dashboard")

    orders = st.session_state.orders
    products = st.session_state.products

    if orders.empty:
        st.info("Nog geen data. Voeg eerst producten/klanten/orders toe.")
    else:
        df = (orders.merge(products[["id","name"]], left_on="product_id", right_on="id", how="left")
                    .rename(columns={"name":"Product"}).drop(columns=["id_y"], errors="ignore"))
        years = sorted(df["year"].dropna().astype(int).unique().tolist())
        sel_year = st.selectbox(
            "Jaar",
            years,
            index=years.index(datetime.now().year) if datetime.now().year in years else 0
        )
per_prod = (df[df["year"] == sel_year]
            .groupby("Product", dropna=False)["quantity"].sum()
            .reset_index(name="Total Sold")
            .sort_values("Total Sold", ascending=False))
st.markdown(f"### Orders per Product in {sel_year}")
st.dataframe(per_prod, use_container_width=True)


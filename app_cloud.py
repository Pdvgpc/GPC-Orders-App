
    
  
# ===== GPC Orders (cloud, GitHub storage, st.secrets/SEC) =====
import os
from io import BytesIO, StringIO
from datetime import datetime
import base64
import json
import requests
from typing import Optional

import pandas as pd
import streamlit as st
import yaml

import hashlib
import hmac
from openpyxl.worksheet.table import Table, TableStyleInfo
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter
import streamlit.components.v1 as components

SEC = dict(st.secrets)

st.set_page_config(page_title="GPC Orders Systeem", layout="wide")

# ------------------------------------------------------------
# [Start] Layout styling
# ------------------------------------------------------------
st.markdown("""
<style>
    :root {
        --gpc-green: #1f5a43;
        --gpc-green-soft: #eaf2ee;
        --gpc-border: #d9e1dd;
        --gpc-text: #1f2937;
        --gpc-muted: #6b7280;
        --gpc-bg: #f6f8f7;
        --gpc-white: #ffffff;
    }

    .stApp {
        background: var(--gpc-bg);
    }

    .block-container {
        padding-top: 1.25rem;
        padding-bottom: 2rem;
        max-width: 96%;
    }

    /* Compact application header */
    .gpc-app-header {
        background: var(--gpc-white);
        border: 1px solid var(--gpc-border);
        border-radius: 8px;
        padding: 14px 18px;
        margin-bottom: 10px;
    }

    .gpc-app-header h1 {
        margin: 0;
        color: var(--gpc-text);
        font-size: 1.45rem;
        font-weight: 700;
        line-height: 1.15;
    }

    .gpc-app-header p {
        margin: 4px 0 0 0;
        color: var(--gpc-muted);
        font-size: 0.86rem;
    }

    .gpc-section-title {
        font-size: 1.05rem;
        font-weight: 700;
        color: var(--gpc-text);
        margin: 0 0 0.35rem 0;
    }

    .gpc-muted {
        color: var(--gpc-muted);
        font-size: 0.84rem;
        margin-top: -0.15rem;
        margin-bottom: 0.65rem;
    }

    /* Keep Streamlit widgets compact */
    div[data-testid="stMetric"] {
        border: 1px solid var(--gpc-border);
        border-radius: 8px;
        padding: 10px 12px;
        background: var(--gpc-white);
        box-shadow: none;
    }


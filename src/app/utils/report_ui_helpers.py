import hashlib
import time as pytime
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.monitor import monitor
from src.processor import to_csv, to_excel, to_parquet, to_pdf


def _to_datetime_safe(values):
    """Parse datetime values without pandas mixed-format warning noise."""
    try:
        return pd.to_datetime(values, errors="coerce", format="mixed")
    except Exception:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return pd.to_datetime(values, errors="coerce")

def create_gauge_chart(value, title, height=250):
    try:
        if value is None or not np.isfinite(float(value)):
            value = 0
    except Exception:
        value = 0
    value = float(value)
    title_size = min(18, max(11, int(height * 0.12)))
    fig = go.Figure(go.Indicator(
        mode="gauge",
        value=value,
        title={"text": title, "font": {"size": title_size, "color": "#475569"}},
        domain={"x": [0, 1], "y": [0, 0.82]},
        gauge={
            "axis": {"range": [0, 100]},
            "bar": {"color": "#00AEC7"},
            "steps": [
                {"range": [0, 50], "color": "#ffebee"},
                {"range": [50, 80], "color": "#fff3e0"},
                {"range": [80, 100], "color": "#e8f5e9"},
            ],
        },
    ))
    # Keep gauge dimensions and alignment stable across cards.
    fig.update_layout(
        height=height,
        margin=dict(l=2, r=2, t=32, b=4),
        autosize=True,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    # Center value: +50% size and bold.
    num_size = min(30, max(16, int(height * 0.15)))
    fig.add_annotation(
        x=0.5,
        y=0.09,
        text=f"<b>{value:.0f}</b>",
        showarrow=False,
        font=dict(size=num_size, color="#334155"),
        align="center",
    )
    return fig

def create_donut_chart(data_dict, title, height=300):
    safe_data = {}
    for k, v in (data_dict or {}).items():
        try:
            v = float(v)
        except Exception:
            v = 0
        if np.isfinite(v) and v > 0:
            safe_data[k] = v
    filtered_data = safe_data or {"N/A": 1}
    fig = px.pie(pd.DataFrame(list(filtered_data.items()), columns=['Status', 'Count']), 
                 values='Count', names='Status', title=title, hole=0.6, color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(height=height, margin=dict(l=20, r=20, t=50, b=20))
    return fig

def sanitize_numeric_df(df):
    if df is None or df.empty:
        return df
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            series = pd.to_numeric(df[col], errors="coerce")
            series = series.replace([np.inf, -np.inf], np.nan).fillna(0)
            df[col] = series
    return df

def _format_24h_time_labels(series, include_seconds=False, label_mode="time"):
    ts = _to_datetime_safe(series)
    if ts.isna().all():
        return ts
    mode = str(label_mode or "time").lower()
    if mode == "date":
        fmt = "%Y-%m-%d"
    elif mode == "datetime":
        fmt = "%Y-%m-%d %H:%M:%S" if include_seconds else "%Y-%m-%d %H:%M"
    else:
        fmt = "%H:%M:%S" if include_seconds else "%H:%M"
    return ts.dt.strftime(fmt)

def _dedupe_time_labels_keep_visual(labels):
    """Make duplicate time labels unique using zero-width suffixes (visually unchanged)."""
    try:
        seen = {}
        out = []
        for raw_label in labels:
            label = str(raw_label)
            idx = seen.get(label, 0)
            seen[label] = idx + 1
            if idx <= 0:
                out.append(label)
            else:
                out.append(label + ("\u200b" * idx))
        return out
    except Exception:
        return labels

def render_24h_time_line_chart(
    df,
    time_col,
    value_cols,
    include_seconds=False,
    aggregate_by_label=None,
    label_mode="time",
    x_index_name=None,
):
    try:
        if df is None or df.empty or time_col not in df.columns:
            return
        chart_df = df.copy()
        chart_df[time_col] = _to_datetime_safe(chart_df[time_col])
        chart_df = chart_df.dropna(subset=[time_col])
        if chart_df.empty:
            return
        value_cols = [value_cols] if isinstance(value_cols, str) else list(value_cols or [])
        value_cols = [c for c in value_cols if c in chart_df.columns]
        if not value_cols:
            return
        chart_df = sanitize_numeric_df(chart_df)
        chart_df = chart_df.sort_values(time_col)
        is_multi_day = chart_df[time_col].dt.normalize().nunique() > 1
        label_col = "_x_label"
        chart_df[label_col] = _format_24h_time_labels(
            chart_df[time_col],
            include_seconds=include_seconds,
            label_mode=label_mode,
        )
        chart_df = chart_df.dropna(subset=[label_col])
        if chart_df.empty:
            return
        # For multi-day ranges, grouping by HH:MM collapses all days into one point.
        # Keep per-timestamp series and only dedupe labels invisibly.
        if aggregate_by_label in ("sum", "mean", "last"):
            if is_multi_day:
                if aggregate_by_label == "sum":
                    chart_df = chart_df.groupby(time_col, as_index=False)[value_cols].sum()
                elif aggregate_by_label == "mean":
                    chart_df = chart_df.groupby(time_col, as_index=False)[value_cols].mean()
                elif aggregate_by_label == "last":
                    chart_df = chart_df.groupby(time_col, as_index=False)[value_cols].last()
                chart_df = chart_df.sort_values(time_col)
                chart_df[label_col] = _format_24h_time_labels(
                    chart_df[time_col],
                    include_seconds=include_seconds,
                    label_mode=label_mode,
                )
            else:
                if aggregate_by_label == "sum":
                    chart_df = chart_df.groupby(label_col, as_index=False)[value_cols].sum()
                elif aggregate_by_label == "mean":
                    chart_df = chart_df.groupby(label_col, as_index=False)[value_cols].mean()
                elif aggregate_by_label == "last":
                    chart_df = chart_df.groupby(label_col, as_index=False)[value_cols].last()
        chart_df[label_col] = _dedupe_time_labels_keep_visual(chart_df[label_col].tolist())
        plot_df = chart_df.set_index(label_col)[value_cols]
        if x_index_name:
            plot_df.index.name = str(x_index_name)
        st.line_chart(plot_df)
    except Exception as e:
        monitor.log_error("RENDER", f"Line chart render failed ({time_col})", str(e))

def _apply_report_row_limit(df, label="Rapor"):
    if df is None or df.empty:
        return df
    try:
        max_rows = int(st.session_state.get("rep_auto_row_limit", 50000) or 0)
    except Exception:
        max_rows = 50000
    if max_rows > 0 and len(df) > max_rows:
        st.warning(
            f"{label}: {len(df)} satır bulundu. Otomatik limit nedeniyle ilk {max_rows} satır gösterilip indirilecektir. "
            "Tam rapor için 'Maksimum satır (0=limitsiz)' değerini artırın veya 0 yapın."
        )
        return df.head(max_rows).copy()
    return df

def _safe_state_token(value):
    return "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in str(value or ""))

def _report_result_state_key(report_key):
    return f"_report_result_{_safe_state_token(report_key)}"

def _store_report_result(report_key, df, base_name):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return
    st.session_state[_report_result_state_key(report_key)] = {
        "df": df.copy(),
        "base_name": str(base_name or report_key),
        "rows": int(len(df)),
        "updated_at": pytime.time(),
    }

def _get_report_result(report_key):
    payload = st.session_state.get(_report_result_state_key(report_key))
    if not isinstance(payload, dict):
        return None
    df = payload.get("df")
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    return payload

def _clear_report_result(report_key):
    st.session_state.pop(_report_result_state_key(report_key), None)

def _download_df_signature(df):
    if df is None or not isinstance(df, pd.DataFrame):
        return "none"
    try:
        row_count = int(len(df))
        col_count = int(len(df.columns))
        if row_count == 0:
            return f"rows:0|cols:{col_count}"
        sample_size = min(5, row_count)
        sample_df = pd.concat([df.head(sample_size), df.tail(sample_size)], ignore_index=True)
        sample_json = sample_df.astype(str).to_json(orient="split", date_format="iso")
        digest = hashlib.sha256(sample_json.encode("utf-8", errors="ignore")).hexdigest()[:16]
        return f"rows:{row_count}|cols:{col_count}|sig:{digest}"
    except Exception:
        try:
            return f"rows:{len(df)}|cols:{len(df.columns)}"
        except Exception:
            return "unknown"


def _report_table_view_state_key(report_key):
    return f"_report_table_view_{_safe_state_token(report_key)}"


def _is_datetime_like_column(column_name, series):
    if series is None:
        return False
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    name = str(column_name or "").strip().lower()
    hints = (
        "tarih",
        "date",
        "zaman",
        "saat",
        "time",
        "aralık",
        "aralik",
        "interval",
        "start",
        "end",
        "başlang",
        "baslang",
        "bitiş",
        "bitis",
        "login",
        "logout",
    )
    if any(h in name for h in hints):
        return True
    if not pd.api.types.is_object_dtype(series):
        return False
    sample = series.dropna().astype(str).str.strip()
    sample = sample[(sample != "") & (sample != "-")]
    if sample.empty:
        return False
    # Avoid duration fields like 00:05:12 by requiring date separators.
    has_date_separator = sample.str.contains(r"[-/T.]", regex=True, na=False)
    sample = sample[has_date_separator].head(120)
    if sample.empty:
        return False
    parsed = _to_datetime_safe(sample)
    return (parsed.notna().mean() >= 0.8)


def _format_report_datetime_columns(df):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        series = out[col]
        if not _is_datetime_like_column(col, series):
            continue
        parsed = _to_datetime_safe(series)
        if parsed.notna().sum() == 0:
            continue
        formatted = parsed.dt.strftime("%d.%m.%Y %H:%M")
        result = series.astype(object).copy()
        mask = parsed.notna()
        result.loc[mask] = formatted.loc[mask]
        out[col] = result
    return out


def _normalize_report_table_state(df, state):
    cols = list(df.columns) if isinstance(df, pd.DataFrame) else []
    if not cols:
        return {"order": [], "visible": [], "sort_by": "", "ascending": True}

    raw = state if isinstance(state, dict) else {}
    raw_order = [c for c in (raw.get("order") or []) if c in cols]
    order = list(raw_order)
    for col in cols:
        if col not in order:
            order.append(col)

    raw_visible = [c for c in (raw.get("visible") or []) if c in order]
    visible = list(raw_visible)
    if not visible:
        visible = list(order)
    else:
        # Keep previously hidden columns hidden. Only auto-show truly new columns
        # that did not exist in the persisted column order.
        visible_set = set(visible)
        prior_order_set = set(raw_order)
        for col in order:
            if col not in prior_order_set and col not in visible_set:
                visible.append(col)
                visible_set.add(col)

    sort_by = str(raw.get("sort_by") or "")
    if sort_by not in cols:
        sort_by = ""

    ascending = bool(raw.get("ascending", True))
    return {
        "order": order,
        "visible": visible,
        "sort_by": sort_by,
        "ascending": ascending,
    }


def render_table_with_export_view(df, report_key, help_text=None):
    """Render dataframe with persisted column order/sort controls and return current view.

    Export actions should use the returned DataFrame to match what user sees.
    """
    if df is None or not isinstance(df, pd.DataFrame):
        st.dataframe(df, width='stretch', hide_index=True)
        return df
    if df.empty:
        st.dataframe(df, width='stretch', hide_index=True)
        return df

    state_key = _report_table_view_state_key(report_key)
    state = _normalize_report_table_state(df, st.session_state.get(state_key))
    all_cols = list(df.columns)

    with st.expander("Tablo Gorunumu ve Siralama", expanded=False):
        st.caption(
            help_text
            or "Buradaki kolon sirasi/gorunurluk ve siralama ayarlari Excel/CSV exporta birebir uygulanir."
        )
        st.caption(
            "Not: Tablo basliginda surukle-birak ile yaptigin gecici kolon yeri degisikligi backend tarafina yansimaz. "
            "Exportla birebir eslesmesi icin asagidaki kontrolleri kullan."
        )

        move_col = st.selectbox(
            "Tasiyacagin sutun",
            state["order"],
            key=f"{state_key}_move_col",
        )
        mv_c1, mv_c2, mv_c3 = st.columns([1, 1, 1])
        if mv_c1.button("Yukari", key=f"{state_key}_move_up", width='stretch'):
            order = list(state["order"])
            idx = order.index(move_col)
            if idx > 0:
                order[idx - 1], order[idx] = order[idx], order[idx - 1]
                visible_set = set(state.get("visible", []))
                state["order"] = order
                state["visible"] = [c for c in order if c in visible_set]
                st.session_state[state_key] = state
                st.rerun()
        if mv_c2.button("Asagi", key=f"{state_key}_move_down", width='stretch'):
            order = list(state["order"])
            idx = order.index(move_col)
            if idx < (len(order) - 1):
                order[idx + 1], order[idx] = order[idx], order[idx + 1]
                visible_set = set(state.get("visible", []))
                state["order"] = order
                state["visible"] = [c for c in order if c in visible_set]
                st.session_state[state_key] = state
                st.rerun()
        if mv_c3.button("Sirayi Sifirla", key=f"{state_key}_move_reset", width='stretch'):
            default_order = list(all_cols)
            state = {
                "order": default_order,
                "visible": default_order,
                "sort_by": "",
                "ascending": True,
            }
            st.session_state[state_key] = state
            st.rerun()

        config_df = pd.DataFrame(
            {
                "Sutun": state["order"],
                "Sira": list(range(1, len(state["order"]) + 1)),
                "Goster": [col in set(state["visible"]) for col in state["order"]],
            }
        )
        edited = st.data_editor(
            config_df,
            key=f"{state_key}_cfg",
            column_config={
                "Sutun": st.column_config.TextColumn("Sutun", disabled=True),
                "Sira": st.column_config.NumberColumn("Sira", min_value=1, step=1),
                "Goster": st.column_config.CheckboxColumn("Goster"),
            },
            hide_index=True,
            width='stretch',
            num_rows="fixed",
        )

        parsed = edited.copy() if isinstance(edited, pd.DataFrame) and not edited.empty else config_df.copy()
        if "Sutun" not in parsed.columns:
            parsed = config_df.copy()
        parsed["Sutun"] = parsed["Sutun"].astype(str)
        parsed = parsed[parsed["Sutun"].isin(all_cols)].copy()
        if parsed.empty:
            parsed = config_df.copy()

        default_rank = {col: idx + 1 for idx, col in enumerate(state["order"])}
        parsed["Sira"] = pd.to_numeric(parsed.get("Sira"), errors="coerce")
        parsed["Sira"] = parsed["Sira"].fillna(parsed["Sutun"].map(default_rank))
        parsed["Sira"] = parsed["Sira"].fillna(len(default_rank) + 1).astype(int)
        parsed["Goster"] = parsed.get("Goster", True).astype(bool)
        parsed = parsed.sort_values(["Sira", "Sutun"], kind="mergesort").reset_index(drop=True)

        ordered_cols = [c for c in parsed["Sutun"].tolist() if c in all_cols]
        for col in all_cols:
            if col not in ordered_cols:
                ordered_cols.append(col)

        visible_cols = [c for c in ordered_cols if c in set(parsed.loc[parsed["Goster"], "Sutun"].tolist())]
        if not visible_cols:
            visible_cols = list(ordered_cols)

        sort_options = ["(Siralama Yok)"] + ordered_cols
        sort_current = state["sort_by"] if state["sort_by"] in ordered_cols else "(Siralama Yok)"
        sort_idx = sort_options.index(sort_current) if sort_current in sort_options else 0

        s_col_1, s_col_2 = st.columns([3, 2])
        selected_sort = s_col_1.selectbox(
            "Siralama Kolonu",
            sort_options,
            index=sort_idx,
            key=f"{state_key}_sort_col",
        )
        sort_direction = s_col_2.radio(
            "Siralama Yonu",
            ["Artan (A-Z)", "Azalan (Z-A)"],
            horizontal=True,
            index=0 if state["ascending"] else 1,
            key=f"{state_key}_sort_dir",
        )
        sort_by = "" if selected_sort == "(Siralama Yok)" else selected_sort
        ascending = sort_direction.startswith("Artan")

        state = {
            "order": ordered_cols,
            "visible": visible_cols,
            "sort_by": sort_by,
            "ascending": ascending,
        }
        st.session_state[state_key] = state

    view_df = df.copy()
    if state.get("sort_by"):
        try:
            sort_col = state["sort_by"]
            sort_series = _to_datetime_safe(view_df[sort_col])
            if sort_series.notna().sum() > 0:
                view_df = view_df.assign(_sort_dt=sort_series).sort_values(
                    by="_sort_dt",
                    ascending=bool(state.get("ascending", True)),
                    kind="mergesort",
                    na_position="last",
                ).drop(columns=["_sort_dt"])
            else:
                view_df = view_df.sort_values(
                    by=sort_col,
                    ascending=bool(state.get("ascending", True)),
                    kind="mergesort",
                    na_position="last",
                )
        except Exception as exc:
            monitor.log_error("REPORT_TABLE", "Sort apply failed", str(exc))

    selected_cols = [c for c in state.get("visible", []) if c in view_df.columns]
    if not selected_cols:
        selected_cols = list(view_df.columns)
    view_df = view_df[selected_cols]
    view_df = _format_report_datetime_columns(view_df)

    st.dataframe(view_df, width='stretch', hide_index=True)
    return view_df

def render_downloads(df, base_name, key_base=None):
    fmt_options = {
        "CSV": {"ext": "csv", "mime": "text/csv", "builder": to_csv},
        "Excel": {"ext": "xlsx", "mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "builder": to_excel},
        "Parquet": {"ext": "parquet", "mime": "application/octet-stream", "builder": to_parquet},
        "PDF": {"ext": "pdf", "mime": "application/pdf", "builder": lambda d: to_pdf(d, title=base_name)},
    }
    key_token = _safe_state_token(key_base or base_name)
    safe_key = key_token or "report"
    fmt_key = f"dl_fmt_{safe_key}"
    prep_key = f"dl_prepare_{safe_key}"
    payload_key = f"dl_payload_{safe_key}"
    current_sig = _download_df_signature(df)

    st.caption("İndirme formatı")
    c1, c2, c3 = st.columns([2, 1, 2], gap="small")
    with c1:
        selected_fmt = st.selectbox(
            "İndirme formatı",
            list(fmt_options.keys()),
            key=fmt_key,
            label_visibility="collapsed",
        )
    with c2:
        prepare_clicked = st.button("Hazırla", key=prep_key, width='stretch')
    if prepare_clicked:
        opt = fmt_options[selected_fmt]
        with st.spinner(f"{selected_fmt} hazırlanıyor..."):
            payload = opt["builder"](df)
        st.session_state[payload_key] = {
            "format": selected_fmt,
            "ext": opt["ext"],
            "mime": opt["mime"],
            "data": payload,
            "rows": len(df),
            "sig": current_sig,
            "prepared_at": datetime.now().strftime("%Y%m%d_%H%M%S"),
        }
    with c3:
        payload = st.session_state.get(payload_key) or {}
        is_ready = bool(payload and payload.get("format") == selected_fmt and payload.get("sig") == current_sig)
        prepared_at = (payload.get("prepared_at") if is_ready else datetime.now().strftime("%Y%m%d_%H%M%S"))
        ext = payload.get("ext") if is_ready else fmt_options.get(selected_fmt, {}).get("ext", "bin")
        mime = payload.get("mime") if is_ready else fmt_options.get(selected_fmt, {}).get("mime", "application/octet-stream")
        data = payload.get("data") if is_ready else b""
        st.download_button(
            f"{selected_fmt} indir",
            data=data,
            file_name=f"{base_name}_{prepared_at}.{ext}",
            mime=mime,
            width='stretch',
            disabled=not is_ready,
        )
    if not is_ready:
        st.caption("İndirme için önce format seçip 'Hazırla' butonuna basın.")

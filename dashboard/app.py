"""Streamlit dashboard for San Diego Get It Done 311 data."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pydeck as pdk
import streamlit as st

# ── Parquet paths (relative to repo root, where Streamlit Cloud runs) ──
_REQUESTS = "data/processed/requests.parquet"
_MAP_POINTS = "data/aggregated/map_points.parquet"

# Resolve paths for local dev (running from project root or dashboard/)
_root = Path(__file__).resolve().parent.parent
if (_root / _REQUESTS).exists():
    _REQUESTS = str(_root / _REQUESTS)
    _MAP_POINTS = str(_root / _MAP_POINTS)

st.set_page_config(
    page_title="San Diego Get It Done 311",
    page_icon="\U0001f3d9\ufe0f",
    layout="wide",
)


def query(sql: str, params: list | None = None):
    """Run SQL against parquet files and return a pandas DataFrame."""
    con = duckdb.connect()
    return con.execute(sql, params or []).fetchdf()


def _where_clause(
    year_range: tuple[int, int],
    selected_types: list[str],
    selected_districts: list[int],
) -> str:
    """Build a WHERE clause string from sidebar filter selections."""
    clauses = [f"request_year BETWEEN {year_range[0]} AND {year_range[1]}"]
    if selected_types:
        escaped = ", ".join(f"'{t.replace(chr(39), chr(39)*2)}'" for t in selected_types)
        clauses.append(f"service_name IN ({escaped})")
    if selected_districts:
        clauses.append(f"council_district IN ({', '.join(str(d) for d in selected_districts)})")
    return "WHERE " + " AND ".join(clauses)


# ── Sidebar filters ──
st.sidebar.title("Filters")


@st.cache_data(ttl=3600)
def _sidebar_options():
    types = query(f"""
        SELECT service_name, COUNT(*) AS cnt
        FROM '{_REQUESTS}'
        GROUP BY service_name
        ORDER BY cnt DESC
    """)["service_name"].tolist()

    years = sorted(query(f"""
        SELECT DISTINCT request_year FROM '{_REQUESTS}' ORDER BY request_year
    """)["request_year"].tolist())

    districts = sorted(query(f"""
        SELECT DISTINCT council_district FROM '{_REQUESTS}'
        WHERE council_district IS NOT NULL
        ORDER BY council_district
    """)["council_district"].tolist())

    return types, years, districts


all_types, years, districts = _sidebar_options()

selected_types = st.sidebar.multiselect(
    "Problem Type",
    options=all_types,
    default=None,
    placeholder="All types",
)

if years:
    year_range = st.sidebar.slider(
        "Year Range",
        min_value=int(min(years)),
        max_value=int(max(years)),
        value=(int(min(years)), int(max(years))),
    )
else:
    year_range = (2016, 2026)

DISTRICT_LABELS = {
    1: "D1 - Pacific Beach, La Jolla",
    2: "D2 - Clairemont, Peninsula, OB",
    3: "D3 - Downtown, Uptown, North Park",
    4: "D4 - Encanto, Skyline-Paradise Hills",
    5: "D5 - Rancho Bernardo, Penasquitos",
    6: "D6 - Mira Mesa, Kearny Mesa",
    7: "D7 - Navajo, Linda Vista, Serra Mesa",
    8: "D8 - SE San Diego, Otay Mesa-Nestor",
    9: "D9 - City Heights, Mid-City",
}
district_options = {DISTRICT_LABELS.get(d, f"District {d}"): d for d in districts}
selected_district_labels = st.sidebar.multiselect(
    "Council District",
    options=list(district_options.keys()),
    default=None,
    placeholder="All districts",
)
selected_districts = [district_options[label] for label in selected_district_labels]

# Shared WHERE clause for all queries
WHERE = _where_clause(year_range, selected_types, selected_districts)

# ── Header ──
st.title("San Diego Get It Done 311")
st.markdown(
    "This dashboard explores every non-emergency service request submitted through "
    "the City of San Diego's **Get It Done** program since May 2016. Reports cover "
    "issues like potholes, illegal dumping, graffiti, streetlight outages, and more. "
    "Data is sourced from the city's [open data portal](https://data.sandiego.gov) "
    "and covers the **City of San Diego only** (not the broader San Diego County). "
    "Use the sidebar filters to narrow by problem type, year range, or council district."
)

# ==================================================================
# Tab layout
# ==================================================================
tab_overview, tab_map, tab_response, tab_trends, tab_equity = st.tabs(
    ["Overview", "Map", "Response Times", "Trends", "Equity"]
)

# ── TAB 1: Overview ──
with tab_overview:
    kpi = query(f"""
        SELECT
            COUNT(*)                                          AS total,
            SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) AS closed,
            MEDIAN(resolution_days)                           AS median_res
        FROM '{_REQUESTS}'
        {WHERE}
    """)
    total = int(kpi["total"].iloc[0])
    closed = int(kpi["closed"].iloc[0])
    close_rate = closed * 100 / total if total else 0
    median_res = kpi["median_res"].iloc[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Reports", f"{total:,}")
    col2.metric("Closed", f"{closed:,}")
    col3.metric("Close Rate", f"{close_rate:.1f}%" if total else "N/A")
    col4.metric(
        "Median Resolution (days)",
        f"{median_res:.0f}" if median_res is not None else "N/A",
    )

    # ── Charts row 1: Problem types + Yearly growth ──
    chart_left, chart_right = st.columns(2)

    with chart_left:
        st.subheader("Top 10 Problem Types")
        top10 = query(f"""
            SELECT service_name AS "Problem Type", COUNT(*) AS "Reports"
            FROM '{_REQUESTS}'
            {WHERE}
            GROUP BY service_name
            ORDER BY "Reports" DESC
            LIMIT 10
        """).set_index("Problem Type")
        st.bar_chart(top10, horizontal=True)

    with chart_right:
        st.subheader("Reports by Year")
        yearly = query(f"""
            SELECT request_year, COUNT(*) AS "Reports"
            FROM '{_REQUESTS}'
            {WHERE}
            GROUP BY request_year
            ORDER BY request_year
        """)
        # Exclude current partial year for cleaner trend
        if len(yearly) > 0:
            max_year = yearly["request_year"].max()
            yearly = yearly[yearly["request_year"] < max_year]
        yearly = yearly.rename(columns={"request_year": "Year"})
        yearly["Year"] = yearly["Year"].astype(str)
        st.bar_chart(yearly.set_index("Year"))

    # ── Charts row 2: Submission channel + Top neighborhoods ──
    chart_left2, chart_right2 = st.columns(2)

    with chart_left2:
        st.subheader("How Reports Are Submitted")
        origin = query(f"""
            SELECT case_origin AS "Channel", COUNT(*) AS "Reports"
            FROM '{_REQUESTS}'
            {WHERE} AND case_origin IS NOT NULL
            GROUP BY case_origin
            ORDER BY "Reports" DESC
        """).set_index("Channel")
        st.bar_chart(origin, horizontal=True)

    with chart_right2:
        st.subheader("Top 10 Neighborhoods")
        top_hoods = query(f"""
            SELECT comm_plan_name AS "Neighborhood", COUNT(*) AS "Reports"
            FROM '{_REQUESTS}'
            {WHERE} AND comm_plan_name IS NOT NULL
            GROUP BY comm_plan_name
            ORDER BY "Reports" DESC
            LIMIT 10
        """).set_index("Neighborhood")
        st.bar_chart(top_hoods, horizontal=True)

    # ── Detail table (collapsed by default) ──
    with st.expander("Full Problem Type Breakdown"):
        detail = query(f"""
            SELECT
                service_name,
                COUNT(*)                                          AS total_requests,
                SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) AS closed_requests,
                ROUND(AVG(resolution_days), 1)                    AS avg_resolution_days,
                MEDIAN(resolution_days)                           AS median_resolution_days,
                ROUND(SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) * 100.0
                      / COUNT(*), 1)                              AS close_rate_pct
            FROM '{_REQUESTS}'
            {WHERE}
            GROUP BY service_name
            ORDER BY total_requests DESC
        """)
        st.dataframe(
            detail,
            use_container_width=True,
            hide_index=True,
            column_config={
                "service_name": "Problem Type",
                "total_requests": st.column_config.NumberColumn("Total", format="%d"),
                "closed_requests": st.column_config.NumberColumn("Closed", format="%d"),
                "avg_resolution_days": st.column_config.NumberColumn(
                    "Avg Days", format="%.1f"
                ),
                "median_resolution_days": st.column_config.NumberColumn(
                    "Median Days", format="%.0f"
                ),
                "close_rate_pct": st.column_config.NumberColumn(
                    "Close %", format="%.1f%%"
                ),
            },
        )

# ── TAB 2: Map ──
with tab_map:
    st.subheader("Report Locations")

    # Build WHERE for map_points (same filters, different table)
    map_where = _where_clause(year_range, selected_types, selected_districts)

    map_df = query(f"""
        SELECT lat, lng
        FROM '{_MAP_POINTS}'
        {map_where}
        ORDER BY RANDOM()
        LIMIT 200000
    """)

    st.caption(f"{len(map_df):,} reports visualized as density heatmap")

    layer = pdk.Layer(
        "HeatmapLayer",
        data=map_df,
        get_position=["lng", "lat"],
        radiusPixels=30,
        intensity=1,
        threshold=0.05,
        opacity=0.7,
    )

    view = pdk.ViewState(
        latitude=32.7157,
        longitude=-117.1611,
        zoom=10.5,
        pitch=0,
    )

    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        map_style="light",
    ))

# ── TAB 3: Response Times ──
with tab_response:
    st.subheader("Resolution Time by Neighborhood")

    resp_hood = query(f"""
        SELECT comm_plan_name, MEDIAN(resolution_days) AS median_resolution_days
        FROM '{_REQUESTS}'
        {WHERE} AND comm_plan_name IS NOT NULL
        GROUP BY comm_plan_name
        ORDER BY median_resolution_days DESC
        LIMIT 30
    """).set_index("comm_plan_name")
    st.bar_chart(resp_hood, horizontal=True)

    st.subheader("Resolution Time by Problem Type")
    resp_type = query(f"""
        SELECT service_name, MEDIAN(resolution_days) AS median_resolution_days
        FROM '{_REQUESTS}'
        {WHERE}
        GROUP BY service_name
        ORDER BY median_resolution_days DESC
        LIMIT 20
    """).set_index("service_name")
    st.bar_chart(resp_type, horizontal=True)

# ── TAB 4: Trends ──
with tab_trends:
    st.subheader("Monthly Report Volume")

    monthly = query(f"""
        SELECT
            request_month_start,
            COUNT(*)                AS total_requests,
            MEDIAN(resolution_days) AS median_resolution_days
        FROM '{_REQUESTS}'
        {WHERE}
        GROUP BY request_month_start
        ORDER BY request_month_start
    """)

    trend_pd = monthly.rename(columns={
        "request_month_start": "Month",
        "total_requests": "Reports",
    })[["Month", "Reports"]].set_index("Month")
    st.line_chart(trend_pd)

    st.subheader("Median Resolution Time Trend")
    res_trend = monthly.rename(columns={
        "request_month_start": "Month",
        "median_resolution_days": "Median Days",
    })[["Month", "Median Days"]].set_index("Month")
    st.line_chart(res_trend)

    # Day/hour heatmap
    st.subheader("When Do People Report Problems?")
    dh = query(f"""
        SELECT
            request_dow,
            HOUR(date_requested) AS request_hour,
            COUNT(*)             AS cnt
        FROM '{_REQUESTS}'
        {WHERE}
        GROUP BY request_dow, request_hour
    """)
    dow_labels = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
    hour_labels = {
        h: f"{h % 12 or 12}{'am' if h < 12 else 'pm'}" for h in range(24)
    }
    dh["day_name"] = dh["request_dow"].map(dow_labels)
    dh["hour_label"] = dh["request_hour"].map(hour_labels)
    pivot = dh.pivot_table(
        index="day_name", columns="hour_label", values="cnt", fill_value=0
    )
    day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    hour_order = [hour_labels[h] for h in range(24)]
    pivot = pivot.reindex(
        index=[d for d in day_order if d in pivot.index],
        columns=[h for h in hour_order if h in pivot.columns],
    )
    pivot = pivot.astype(int)
    st.dataframe(pivot, use_container_width=True)

# ── TAB 5: Equity ──
with tab_equity:
    st.subheader("Service Equity by Neighborhood")
    st.caption(
        "Do all neighborhoods get equal service? Compare resolution times "
        "and close rates across communities."
    )

    equity = query(f"""
        SELECT
            comm_plan_name,
            COUNT(*)                                          AS total_requests,
            MEDIAN(resolution_days)                           AS median_resolution_days,
            SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) AS closed_requests,
            ROUND(SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 1)                              AS close_rate_pct
        FROM '{_REQUESTS}'
        {WHERE} AND comm_plan_name IS NOT NULL
        GROUP BY comm_plan_name
        HAVING COUNT(*) >= 100
        ORDER BY median_resolution_days DESC
    """)

    col_left, col_right = st.columns(2)

    equity_col_config = {
        "comm_plan_name": "Neighborhood",
        "median_resolution_days": st.column_config.NumberColumn(
            "Median Days", format="%.0f"
        ),
        "total_requests": st.column_config.NumberColumn(
            "Total Reports", format="%d"
        ),
        "close_rate_pct": st.column_config.NumberColumn(
            "Close %", format="%.1f%%"
        ),
    }
    equity_cols = ["comm_plan_name", "median_resolution_days", "total_requests", "close_rate_pct"]

    with col_left:
        st.markdown("**Slowest Neighborhoods** (median days to resolve)")
        st.dataframe(
            equity.head(10)[equity_cols],
            hide_index=True,
            column_config=equity_col_config,
        )

    with col_right:
        st.markdown("**Fastest Neighborhoods** (median days to resolve)")
        st.dataframe(
            equity.sort_values("median_resolution_days").head(10)[equity_cols],
            hide_index=True,
            column_config=equity_col_config,
        )

    # Scatter: close rate vs resolution time
    st.subheader("Close Rate vs Resolution Time")
    scatter_df = equity[["comm_plan_name", "close_rate_pct", "median_resolution_days", "total_requests"]]
    st.scatter_chart(
        scatter_df,
        x="median_resolution_days",
        y="close_rate_pct",
        size="total_requests",
    )

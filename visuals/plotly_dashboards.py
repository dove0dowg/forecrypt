import logging
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv
from clickhouse_driver import Client
from nicegui import ui

from db.db_utils_clickhouse import update_ch_config, clickhouse_connection

logger = logging.getLogger(__name__)

def plot_forecast_data(ch_config: dict, currency: str = "BTC") -> None:

    updated_ch_config = update_ch_config(ch_config)

    client = Client(
        host=updated_ch_config['host'],
        port=updated_ch_config['port'],
        user=updated_ch_config['user'],
        password=updated_ch_config['password'],
        database=updated_ch_config['database'],
    )

    sql = f"""
    SELECT timestamp, forecast_value, zero_step_ts, model_name_ext
    FROM forecrypt_db.forecast_data
    WHERE currency = '{currency}'
    ORDER BY model_name_ext, zero_step_ts, timestamp
    """
    data = client.execute(sql)
    df = pd.DataFrame(data, columns=['timestamp', 'forecast_value', 'zero_step_ts', 'model_name_ext'])

    fig = go.Figure()
    for (mne, zts), group in df.groupby(['model_name_ext', 'zero_step_ts']):
        fig.add_trace(go.Scatter(
            x=group['timestamp'],
            y=group['forecast_value'],
            mode='lines',
            name=f"{mne} @ {zts}"
        ))

    fig.update_layout(
        title=f"Forecast Data for {currency}",
        xaxis_title="Timestamp",
        yaxis_title="Forecast Value",
        margin=dict(l=40, r=40, t=40, b=100),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5,
            font=dict(size=10),
            bgcolor='rgba(255,255,255,0)' 
        )
    )

    ui.plotly(fig)
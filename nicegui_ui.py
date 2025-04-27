from nicegui import ui
import json
import logging
from pathlib import Path
from config.config_system import PG_DB_CONFIG, CH_DB_CONFIG, START_DATE, FINISH_DATE, CRYPTO_LIST
from config.config_models import MODEL_PARAMETERS
from pipeline import (
    initialize_environment,
    clear_all_databases,
    run_full_cycle,
    prepare_pg_container,
    prepare_ch_container,
    prepare_pg_tables,
    prepare_ch_tables,
)
from visuals.plotly_dashboards import plot_forecast_data

# ──────────────────────────────────── log setup
LOG_PATH = Path("forecrypt.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(),
              logging.FileHandler(LOG_PATH, encoding="utf-8")],
)
logger = logging.getLogger(__name__)

def tail_log(max_lines: int = 30) -> str:
    if not LOG_PATH.exists():
        return "[log file not found]"
    try:
        lines = LOG_PATH.read_text(encoding="utf-8").splitlines()
        return "\n".join(lines[-max_lines:]) or "[log empty]"
    except Exception as e:
        return f"[log read error: {e}]"

# ─────────────────────────────── helper: unified action wrapper
def handle_action(func, label: str, *args, **kwargs):
    with ui.notification(f"{label} started …", type="info", close_button=True):
        pass
    try:
        result = func(*args, **kwargs)
        if result:
            ui.notify(f"{label} completed successfully ✅", type="positive")
            logger.info("%s returned True", func.__name__)
        else:
            ui.notify(f"{label} finished with warnings ⚠️", type="warning")
            logger.warning("%s returned False", func.__name__)
    except Exception as exc:
        logger.exception("error inside %s", func.__name__)
        ui.notify(f"{label} failed: {exc}", type="negative")

# ────────────────────────────────────────────── UI
def build_ui() -> None:
    
    ui.dark_mode()

    ui.add_head_html("""
    <style>
      body { background: #E6E6FA; color: #d0fdfd; }
    </style>
    """)       

    ui.markdown("## **Forecrypt Controls**").classes("text-purple-700 text-2xl")

    with ui.row().classes("w-full gap-8"):
        with ui.column().classes("items-stretch w-1/2"):

            with ui.row().classes("gap-4"):
                ui.button(
                    "Initialize\nContainers",
                    on_click=lambda _: handle_action(
                        initialize_environment, "Initialization",
                        PG_DB_CONFIG, CH_DB_CONFIG
                    ),
                    color="teal-8",
                    icon="build",
                ).classes("stacked-label w-48 h-32")

                with ui.column().classes("items-stretch gap-2"):
                    ui.button(
                        "PG Container",
                        color="teal-3",
                        on_click=lambda _: handle_action(prepare_pg_container, "PG Container"),
                    ).props("size=sm")
                    ui.button(
                        "CH Container",
                        color="teal-3",
                        on_click=lambda _: handle_action(prepare_ch_container, "CH Container"),
                    ).props("size=sm")
                    ui.button(
                        "PG Tables",
                        on_click=lambda _: handle_action(
                            prepare_pg_tables, "PG Tables", PG_DB_CONFIG
                        ),
                        color="teal-3",
                    ).props("size=sm")
                    ui.button(
                        "CH Tables",
                        on_click=lambda _: handle_action(
                            prepare_ch_tables, "CH Tables",
                            PG_DB_CONFIG, CH_DB_CONFIG
                        ),
                        color="teal-3",
                    ).props("size=sm")

                ui.button(
                    "Clear Databases",
                    on_click=lambda _: handle_action(
                        clear_all_databases, "Clear DB",
                        PG_DB_CONFIG, CH_DB_CONFIG
                    ),
                    color="teal-3",
                ).classes("stacked-label w-48 h-32")

                log_area = ui.textarea(value=tail_log()).props("readonly").classes("w-2/5 h-32 text-mono")
            ui.timer(1.0, lambda: log_area.set_value(tail_log()))

            ui.timer(10.0, lambda: LOG_PATH.touch())      # ghost touch
            ui.timer(10.0, lambda: LOG_PATH.open('rb').read(1))  # ghost ping

            ui.separator().classes('h-1 bg-gray-300 my-4')

            with ui.row().classes("gap-4"):

                start_date_area = ui.textarea(
                    label='Start Date',
                    value=json.dumps(START_DATE, indent=2)
                ).props('autosize=false rows=1 color=teal-5').classes('w-2/7 h-12 font-mono')

                finish_date_area = ui.textarea(
                    label='Finish Date',
                    value=json.dumps(FINISH_DATE, indent=2)
                ).props('autosize=false rows=1').classes('w-2/7 h-12 font-mono')

            crypto_list_area = ui.textarea(
                label='Cryptocurrency list',
                value=json.dumps(CRYPTO_LIST),        # no indent => one-line
            ).props('autosize=false rows=1 wrap=off')\
             .classes('w-2/7 font-mono')             # single-line textarea

            
            
            model_params_area = ui.textarea(
                label='Model Parameters (JSON)',
                value=json.dumps(MODEL_PARAMETERS, indent=2)
            ).props('autosize=false rows=12').classes('w-full h-64 font-mono')
            
            def run_full_cycle_from_ui():
                try:
                    ui_start_date = json.loads(start_date_area.value)
                    ui_finish_date = json.loads(finish_date_area.value)
                    ui_model_params = json.loads(model_params_area.value)
                    ui_crypto_list = json.loads(crypto_list_area.value)

                    handle_action(
                        run_full_cycle, "Full Cycle",
                        PG_DB_CONFIG, CH_DB_CONFIG, ui_model_params, ui_start_date, ui_finish_date, ui_crypto_list
                    )
                except json.JSONDecodeError as e:
                    ui.notify(f'JSON parsing error: {e}', type='negative')

            ui.button(
                "Run Full Cycle",
                on_click=run_full_cycle_from_ui,
                color="teal-8"
            ).classes("stacked-label w-full h-12")

            ui.separator().classes('h-1 bg-gray-300 my-4')

    currency_input = (
        ui.input('Currencies (comma-separated)')
          .classes('w-64')
    )

    def draw_graphs():
        graph_container.clear()
        with graph_container:
            for cur in (currency_input.value or '').split(','):
                cur = cur.strip()
                if not cur:
                    continue
                fig = plot_forecast_data(CH_DB_CONFIG, cur)
                if fig is None:
                    continue
                fig.update_layout(
                    height=700,
                    margin=dict(l=40, r=40, t=40, b=40),
                    legend=dict(orientation='h', y=-0.25),
                )
                ui.plotly(fig).classes('w-full').style('height:700px')

    with ui.row().classes('w-full gap-4'):
        ui.button('Plotly graph', on_click=draw_graphs, color='teal-8')\
            .classes('stacked-label w-48 h-12')
        ui.button('Remove graphs', on_click=lambda: graph_container.clear(), color='teal-3')\
            .classes('stacked-label w-48 h-12')

    graph_container = ui.column().classes('w-full')


# ────────────────────────────── entrypoint
if __name__ in {"__main__", "__mp_main__"}:
    build_ui()
    ui.run(title="Forecrypt Controls", reload=False, dark=True)

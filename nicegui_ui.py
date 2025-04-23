from nicegui import ui
import logging
from pathlib import Path

# --- import your existing backend logic -----------------------------
from pipeline import (
    initialize_environment,
    clear_all_databases,
    run_full_cycle,
    prepare_pg_container,
    prepare_ch_container,
    prepare_pg_tables,
    prepare_ch_tables,
) 
# -------------------------------------------------------------------

LOG_PATH = Path("forecrypt.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def tail_log(max_lines: int = 30) -> str:
    if not LOG_PATH.exists():
        return "[log file not found]"
    try:
        lines = LOG_PATH.read_text(encoding="utf-8").splitlines()
        if not lines:
            return "[log empty]"
        return "\n".join(lines[-max_lines:])
    except Exception as e:
        return f"[log read error: {e}]"


# ------------------------- UI LAYOUT -------------------------------

def build_ui() -> None:
    ui.dark_mode()
    ui.add_head_html("""
    <style>
        body { background: #0e0e11; color: #d0fdfd; }
        .nicegui-content { padding: 1rem; }
        textarea, button, .q-btn, .q-field__native {
            background-color: #1a1a1f;
            color: #c0ffb3;
            border-radius: 8px;
        }
        .q-btn.q-btn--outline {
            border: 1px solid #6f00fc !important;
        }
        .q-btn .q-icon {
            color: #adf;
        }
        .vertical-label {
            white-space: pre-line;
        }
        .stacked-label {
            white-space: pre;
            font-size: 1.1rem;
            font-weight: bold;
        }
    </style>
    """)

    ui.markdown("## 🚀 **Forecrypt Dashboard**").classes("text-purple-300 text-2xl")

    with ui.row().classes("w-full gap-8"):
        with ui.column().classes("items-stretch w-1/2"):
            with ui.row().classes("gap-4"):
                ui.button(
                    "Initialize\nContainers",
                    on_click=lambda _: handle_action(initialize_environment, "Initialization"),
                    icon="build",
                ).classes("stacked-label w-48 h-32")
                with ui.column().classes("items-stretch gap-2"):
                    ui.button(
                        "PG Container",
                        on_click=lambda _: handle_action(prepare_pg_container, "PG Container"),
                    ).props("size=sm")
                    ui.button(
                        "CH Container",
                        on_click=lambda _: handle_action(prepare_ch_container, "CH Container"),
                    ).props("size=sm")
                    ui.button(
                        "PG Tables",
                        on_click=lambda _: handle_action(prepare_pg_tables, "PG Tables"),
                    ).props("size=sm")
                    ui.button(
                        "CH Tables",
                        on_click=lambda _: handle_action(prepare_ch_tables, "CH Tables"),
                    ).props("size=sm")

                ui.button(
                    "Clear Databases",
                    on_click=lambda _: handle_action(clear_all_databases, "Clear DB"),
                    color="warning",
                ).classes("stacked-label w-48 h-32")

            log_area = ui.textarea(value=tail_log()).props("readonly").classes("w-2/3 h-120 text-mono")
            ui.timer(1.0, lambda: log_area.set_value(tail_log()))



            def ghost_ping():
                with open('forecrypt.log', 'rb') as f:
                    f.read(1)  # прочитать байт, триггернуть IO

            

            def ghost_touch():
                try:
                    path = LOG_PATH
                    path.touch()
                except Exception as e:
                    logger.debug(f"ghost_touch failed: {e}")

            ui.timer(10.0, ghost_touch)
            ui.timer(10.0, ghost_ping)


            ui.button(
                "Run Full Cycle",
                on_click=lambda _: handle_action(run_full_cycle, "Full Cycle"),
                color="positive",
            ).classes("stacked-label w-48 h-32")

            ui.link("Open Explorer 🔍", "#").classes("mt-4 text-primary")

        with ui.column().classes("w-1/2"):
            ui.markdown(
                "🧠 *Model & system configuration editors will appear here in future iterations.*"
            ).classes("text-sm text-gray-400")


# ----------------------- ACTION HANDLER ----------------------------

def handle_action(func, label: str):
    with ui.notification(f"{label} started …", type="info", close_button=True):
        pass
    try:
        result = func()
        if result:
            ui.notify(f"{label} completed successfully ✅", type="positive")
            logger.info("%s returned True", func.__name__)
        else:
            ui.notify(f"{label} finished with warnings ⚠️ (see log)", type="warning")
            logger.warning("%s returned False", func.__name__)
    except Exception as exc:
        logger.exception("error inside %s", func.__name__)
        ui.notify(f"{label} failed: {exc}", type="negative")


# ---------------------------- main ---------------------------------

if __name__ in {"__main__", "__mp_main__"}:
    build_ui()
    ui.run(title="Forecrypt Dashboard", reload=False)
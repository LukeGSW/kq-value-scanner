"""Loader centralizzato per il file di configurazione.

Espone una funzione `load_settings()` che legge `config/settings.yaml` dalla
root del progetto e restituisce il contenuto come dizionario. Il risultato è
cacheable con `@lru_cache` per evitare letture ripetute dello stesso file.

Esempio d'uso
-------------
>>> from pipeline.config import load_settings
>>> settings = load_settings()
>>> min_mcap = settings['universe']['marketcap_min_usd']
>>> exchanges = settings['universe']['exchanges_us'] + settings['universe']['exchanges_eu']
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


# Root del progetto = directory che contiene la cartella `pipeline/`
_THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = _THIS_FILE.parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "settings.yaml"


@lru_cache(maxsize=1)
def load_settings(path: str | Path | None = None) -> dict[str, Any]:
    """Carica le impostazioni da YAML.

    Parameters
    ----------
    path : str or Path, optional
        Path esplicito al file di configurazione. Default: `config/settings.yaml`
        nella root del progetto.

    Returns
    -------
    dict
        Contenuto parsato del file YAML.
    """
    config_file = Path(path) if path else CONFIG_PATH
    if not config_file.exists():
        raise FileNotFoundError(
            f"File di configurazione non trovato: {config_file}\n"
            f"Root progetto rilevata: {PROJECT_ROOT}"
        )
    with config_file.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_all_exchanges() -> list[dict[str, Any]]:
    """Ritorna la lista completa degli exchange (US + EU) dalla configurazione.

    Ogni elemento contiene: code, name, currency, country, benchmark.
    """
    settings = load_settings()
    universe = settings["universe"]
    return list(universe["exchanges_us"]) + list(universe["exchanges_eu"])


def get_exchange_codes() -> list[str]:
    """Ritorna solo i codici EODHD di tutti gli exchange configurati."""
    return [ex["code"] for ex in get_all_exchanges()]

"""Modulo compute — calcolo metriche fondamentali, tecniche, score compositi."""

from pipeline.compute.technical import (
    compute_technical_all,
    compute_technical_for_series,
)
from pipeline.compute.cash_debt import (
    compute_cash_debt_all,
    compute_cash_debt_for_row,
)
from pipeline.compute.profitability import (
    compute_profitability_all,
    compute_profitability_for_row,
)
from pipeline.compute.valuation import (
    compute_valuation_all,
    compute_pe_percentile_5y,
    compute_sector_relative_all,
)
from pipeline.compute.seasonality import (
    compute_seasonality_for_ticker,
    compute_seasonality_all_json,
)
from pipeline.compute.scores import (
    compute_scores_all,
    compute_altman_z,
    compute_piotroski_f,
    compute_beneish_m,
    compute_kq_value_score,
)
from pipeline.compute.screener import build_screener, ScreenerBuildResult

__all__ = [
    "compute_technical_all",
    "compute_technical_for_series",
    "compute_cash_debt_all",
    "compute_cash_debt_for_row",
    "compute_profitability_all",
    "compute_profitability_for_row",
    "compute_valuation_all",
    "compute_pe_percentile_5y",
    "compute_sector_relative_all",
    "compute_seasonality_for_ticker",
    "compute_seasonality_all_json",
    "compute_scores_all",
    "compute_altman_z",
    "compute_piotroski_f",
    "compute_beneish_m",
    "compute_kq_value_score",
    "build_screener",
    "ScreenerBuildResult",
]

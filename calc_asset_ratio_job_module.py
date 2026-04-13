from pathlib import Path

import pandas as pd

from calc_asset_ratio import calc_asset_ratio


def task_generator(fund_list=None, strategy_type=1, frequency=1, root_dir="", **_kwargs):
    root = Path(root_dir or Path(__file__).resolve().parent)
    fund_nav_df = pd.read_csv(root / "fund_nav_df.csv")
    fund_nav_df["TradingDay"] = pd.to_datetime(fund_nav_df["TradingDay"])
    if fund_list:
        fund_nav_df = fund_nav_df[fund_nav_df["FundID"].isin(fund_list)]
    fund_net_value_pvt = fund_nav_df.pivot(index="TradingDay", columns="FundID", values="AdjustedNav")
    fund_net_value_pvt = fund_net_value_pvt[fund_net_value_pvt.count().sort_values(ascending=False).index.values]
    return [
        {
            "order": order,
            "fund_net_value_series": fund_net_value_series.dropna(),
            "strategy_type": strategy_type,
            "frequency": 0,
            "root_dir": str(root),
        }
        for order, (_, fund_net_value_series) in enumerate(fund_net_value_pvt.items())
    ]


def _load_job_globals(root_dir):
    root = Path(root_dir or Path(__file__).resolve().parent)
    if calc_asset_ratio.bench_mark_yield_df is not None:
        return

    calc_asset_ratio.bench_mark_closeprice_df = pd.read_csv(root / "bench_mark_closeprice_df.csv")
    calc_asset_ratio.bench_mark_closeprice_df["TradingDay"] = pd.to_datetime(calc_asset_ratio.bench_mark_closeprice_df["TradingDay"])
    calc_asset_ratio.bench_mark_closeprice_df.set_index("TradingDay", inplace=True)

    calc_asset_ratio.bench_mark_yield_df = pd.read_csv(root / "bench_mark_yield_df.csv")
    calc_asset_ratio.bench_mark_yield_df["TradingDay"] = pd.to_datetime(calc_asset_ratio.bench_mark_yield_df["TradingDay"])
    calc_asset_ratio.bench_mark_yield_df.set_index("TradingDay", inplace=True)

    calc_asset_ratio.bench_mark_yield_df_weekly = pd.read_csv(root / "bench_mark_yield_df_weekly.csv")
    calc_asset_ratio.bench_mark_yield_df_weekly["TradingDay"] = pd.to_datetime(calc_asset_ratio.bench_mark_yield_df_weekly["TradingDay"])
    calc_asset_ratio.bench_mark_yield_df_weekly.set_index("TradingDay", inplace=True)


def run(order=0, fund_net_value_series=None, strategy_type=1, frequency=0, root_dir="", **_kwargs):
    _load_job_globals(root_dir)
    return {
        "order": int(order),
        "data": calc_asset_ratio.get_fund_asset_ratio(
            fund_net_value_series,
            strategy_type=strategy_type,
            frequency=frequency,
        ),
    }


def handle_data(state, task_item):
    state.append(task_item["result"])


def finalize(state):
    state.sort(key=lambda item: item["order"])
    return [item["data"] for item in state]

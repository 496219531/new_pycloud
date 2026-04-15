from pathlib import Path

import pandas as pd
from calc_asset_ratio import calc_asset_ratio

bench_mark_yield_df=calc_asset_ratio.bench_mark_yield_df
bench_mark_yield_df_weekly=calc_asset_ratio.bench_mark_yield_df_weekly
bench_mark_closeprice_df=calc_asset_ratio.bench_mark_closeprice_df


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


def update_globals(**_kwargs):
    return calc_asset_ratio.update_globals()



def run(order=0, fund_net_value_series=None, strategy_type=1, frequency=0, root_dir="", **_kwargs):
    return {
        "order": int(order),
        "data": calc_asset_ratio.get_fund_asset_ratio(
            fund_net_value_series,
            strategy_type=strategy_type,
            frequency=frequency,
        ),
    }


def handle_data(task_id, result, state=None, **_kwargs):
    if state is None:
        state = {}
    state.setdefault("items", [])
    state["items"].append(result)
    return state


def finalize(state=None, **_kwargs):
    items = list((state or {}).get("items", []))
    items.sort(key=lambda item: item["order"])
    return [item["data"] for item in items]

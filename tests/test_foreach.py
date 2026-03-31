"""中文说明：验证 foreach 的核心语义。"""

import time

from pycloud_parallel import configure, foreach


def _square_or_fail(x):
    if x in (3, 7):
        raise ValueError("boom")
    return x * x


def _setup_runtime(max_workers=4):
    configure(max_workers=max_workers, reset=True)


def test_foreach_skip_errors():
    # 保序返回成功项，失败项落到 errors。
    _setup_runtime(max_workers=4)
    result = foreach(
        range(10),
        _square_or_fail,
    )
    assert result.values == [0, 1, 4, 16, 25, 36, 64, 81]
    assert [e.index for e in result.errors] == [3, 7]

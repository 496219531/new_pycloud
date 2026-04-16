"""中文说明：验证 foreach 的核心语义。"""

import time

from pycloud_parallel.local import configure, foreach
from pycloud_parallel.local_runtime.runtime import _deserialize_callable, _serialize_callable


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


def test_serialize_callable_prefers_pickle_for_top_level_function():
    serializer, payload = _serialize_callable(_square_or_fail)
    restored = _deserialize_callable((serializer, payload))

    assert serializer == "pickle"
    assert restored(2) == 4


def test_serialize_callable_falls_back_to_cloudpickle_for_lambda():
    serializer, payload = _serialize_callable(lambda x: x + 1)
    restored = _deserialize_callable((serializer, payload))

    assert serializer == "cloudpickle"
    assert restored(2) == 3

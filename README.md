# boj-api-client

日本銀行「時系列統計データ検索サイト API」（`https://www.stat-search.boj.or.jp/api/v1`）向けの Python クライアントです。

## 主な特徴

- 同期/非同期クライアント
  - `BojClient`
  - `AsyncBojClient`
- 通信フォーマットは JSON 固定（`format=json`）
- 型付き Query/Response モデル
- `getDataCode` の 250 件超を自動分割して統合
- `NEXTPOSITION` を使った自動ページング
- リトライ/スロットリング
- 途中失敗時の partial result + checkpoint 再開

## 対応 API

- `getDataCode`
- `getDataLayer`
- `getMetadata`

## 要件

- Python 3.11+

## インストール

通常利用（PyPI）:

```bash
pip install boj-api-client
```

2026-02-20 時点で、`boj-api-client==0.1.0` を `pip install boj-api-client` で導入し、
以下の API 呼び出しでデータ取得できることを確認済みです。

- `getMetadata`（`db="FM08"`）
- `getDataCode`（`db="CO"`, `code=["TK99F1000601GCQ01000"]`）

ローカル開発環境で使う場合:

```bash
pip install -e .
```

または

```bash
uv pip install -e .
```

## クイックスタート（同期）

```python
from boj_api_client import BojClient
from boj_api_client.timeseries import DataCodeQuery

with BojClient() as client:
    result = client.timeseries.get_data_code(
        DataCodeQuery(
            db="CO",
            code=["TK99F1000601GCQ01000"],
            lang="JP",
        )
    )

for series in result.series:
    print(series.series_code, len(series.points))
```

## クイックスタート（非同期）

```python
import asyncio

from boj_api_client import AsyncBojClient
from boj_api_client.timeseries import MetadataQuery


async def main() -> None:
    async with AsyncBojClient() as client:
        result = await client.timeseries.get_metadata(
            MetadataQuery(db="FM08", lang="JP")
        )
    print(result.envelope.status, len(result.entries))


asyncio.run(main())
```

## ページ単位で取得する（iter_*）

```python
from boj_api_client import BojClient
from boj_api_client.timeseries import DataLayerQuery

with BojClient() as client:
    for page in client.timeseries.iter_data_layer(
        DataLayerQuery(db="MD10", frequency="Q", layer1="*")
    ):
        print(page.envelope.status, len(page.series))
```

## getDataLayer の auto-partition を有効化する

`getDataLayer` が 1,250 系列上限に達したとき、metadata 経由の fallback を使う設定です。

```python
from boj_api_client import BojClient, BojClientConfig
from boj_api_client.config import TimeSeriesConfig
from boj_api_client.timeseries import DataLayerQuery

config = BojClientConfig(
    timeseries=TimeSeriesConfig(enable_layer_auto_partition=True),
)

with BojClient(config=config) as client:
    result = client.timeseries.get_data_layer(
        DataLayerQuery(db="MD10", frequency="Q", layer1="*", lang="JP")
    )
```

## partial result から再開する

```python
from boj_api_client import BojClient
from boj_api_client.core.errors import BojPartialResultError
from boj_api_client.timeseries import DataCodeQuery

query = DataCodeQuery(db="CO", code=["TK99F1000601GCQ01000"], lang="JP")

with BojClient() as client:
    try:
        result = client.timeseries.get_data_code(query)
    except BojPartialResultError as exc:
        if exc.checkpoint_id is None:
            raise
        result = client.timeseries.get_data_code(
            query,
            checkpoint_id=exc.checkpoint_id,
        )
```

## 設定

```python
from boj_api_client import BojClient, BojClientConfig
from boj_api_client.config import (
    CheckpointConfig,
    RetryConfig,
    ThrottlingConfig,
    TimeSeriesConfig,
    TransportConfig,
)

config = BojClientConfig(
    transport=TransportConfig(timeout_read_seconds=20.0),
    retry=RetryConfig(max_attempts=3, max_backoff_seconds=5.0),
    throttling=ThrottlingConfig(min_wait_interval_seconds=1.0),
    checkpoint=CheckpointConfig(enabled=True, ttl_seconds=86400.0),
    timeseries=TimeSeriesConfig(enable_layer_auto_partition=False),
)

with BojClient(config=config) as client:
    ...
```

## 主な例外

- `BojValidationError`
- `BojServerError`
- `BojUnavailableError`
- `BojTransportError`
- `BojProtocolError`
- `BojPartialResultError`
- `BojClientClosedError`

## live contract test

```bash
BOJ_RUN_LIVE=1 pytest -m live -q tests/contract_live
```

`getDataLayer` live テストを実行する場合:

- `BOJ_LIVE_LAYER1`（必須）
- `BOJ_LIVE_LAYER_DB`（任意、既定 `MD10`）
- `BOJ_LIVE_LAYER_FREQUENCY`（任意、既定 `Q`）

## ドキュメント

- パッケージ構成と責務: `docs/architecture.md`
- API 仕様の実装対応: `docs/api_overview.md`

## 開発者向け

- sync orchestrator は async source から生成:
  - `uv run --extra dev python scripts/generate_sync_orchestrator.py`

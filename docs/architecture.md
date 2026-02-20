# BOJ API Client Architecture

最終更新: 2026-02-20  
対象パッケージ: `boj-api-client`

## 1. 目的

このドキュメントは、`boj-api-client` の現行実装におけるパッケージ構成と責務分割を示す。
設計履歴やロードマップではなく、実装参照用のアーキテクチャ仕様のみを扱う。

## 2. 公開 API

`src/boj_api_client/__init__.py` の公開シンボル:

- `BojClient`
- `AsyncBojClient`
- `BojClientConfig`

`src/boj_api_client/timeseries/__init__.py` の公開シンボル:

- Query: `DataCodeQuery`, `DataLayerQuery`, `MetadataQuery`
- Response/Model:
  - `DataCodeResponse`
  - `DataLayerResponse`
  - `MetadataResponse`
  - `TimeSeries`
  - `TimeSeriesPoint`
  - `MetadataEntry`

## 3. レイヤー構成

### 3.1 Client Layer

- `client.py`: 同期クライアント `BojClient`
- `async_client.py`: 非同期クライアント `AsyncBojClient`
- `client_shared.py`: sync/async 共通の設定検証と checkpoint store 解決

責務:

- ライフサイクル管理（`with` / `async with`）
- close 後アクセス防止（guard wrapper）
- timeseries サービスへの委譲

### 3.2 TimeSeries Layer

- strict 実行:
  - `strict.py`
  - `async_strict.py`
  - `strict_shared.py`
- オーケストレーション:
  - `async_orchestrator.py`（source of truth）
  - `orchestrator.py`（生成物）
- 補助モジュール:
  - `queries.py`
  - `validators.py`
  - `params.py`
  - `parser.py`
  - `planner.py`
  - `selectors.py`
  - `aggregation.py`
- checkpoint 関連:
  - `checkpoint_models.py`
  - `checkpoint_codec.py`
  - `checkpoint_validation.py`
  - `checkpoint_state.py`
  - `checkpoint_shared.py`
  - `checkpoint_manager.py`
  - `async_checkpoint_manager.py`

責務:

- 入力正規化・strict 検証
- API 制約吸収（`code` 250 自動分割、`NEXTPOSITION` ページング）
- `getDataLayer` の任意 auto-partition（設定有効時）
- 途中失敗時の partial result + checkpoint
- JSON から公開ドメインモデルへの変換

### 3.3 Core Layer

- transport:
  - `transport.py`
  - `async_transport.py`
  - `transport_shared.py`
- retry/throttling/pagination:
  - `retry.py`
  - `throttling.py`
  - `async_throttling.py`
  - `pagination.py`
  - `async_pagination.py`
- 共通モデル/エラー:
  - `models.py`
  - `errors.py`
  - `response_parsing.py`
- checkpoint store:
  - `checkpoint_store.py`
  - `async_checkpoint_store.py`

責務:

- HTTP 実行・再試行・待機制御
- HTTP status / body `STATUS` の整合判定
- 例外マッピング
- checkpoint 永続化（メモリ/ファイル）

## 4. パッケージ構成

```text
src/boj_api_client/
  __init__.py
  client.py
  async_client.py
  client_shared.py
  config.py
  py.typed
  core/
    __init__.py
    errors.py
    models.py
    response_parsing.py
    retry.py
    throttling.py
    async_throttling.py
    pagination.py
    async_pagination.py
    transport.py
    async_transport.py
    transport_shared.py
    checkpoint_store.py
    async_checkpoint_store.py
  timeseries/
    __init__.py
    queries.py
    validators.py
    params.py
    models.py
    parser.py
    planner.py
    selectors.py
    aggregation.py
    checkpoint_models.py
    checkpoint_codec.py
    checkpoint_validation.py
    checkpoint_state.py
    checkpoint_shared.py
    checkpoint_manager.py
    async_checkpoint_manager.py
    strict_shared.py
    strict.py
    async_strict.py
    async_orchestrator.py
    orchestrator.py
    _orchestrator_codegen.py
```

## 5. 同期/非同期方針

- `timeseries/async_orchestrator.py` を単一の編集対象とする
- `timeseries/orchestrator.py` は生成物として扱う
- 生成コマンド:
  - `uv run --extra dev python scripts/generate_sync_orchestrator.py`
- CI では生成差分を検証し、drift を禁止する

## 6. エラーモデル

基底例外: `BojApiError`  
主な派生:

- `BojTransportError`
- `BojClientClosedError`
- `BojValidationError`
- `BojServerError`
- `BojUnavailableError`
- `BojProtocolError`
- `BojPartialResultError`

補足:

- `BojPartialResultError` は `partial_result` と `checkpoint_id` を保持する
- `MESSAGEID` / `STATUS` / HTTP status は例外に保持される

## 7. Checkpoint 方針

- store インターフェース: `CheckpointStore` (`save/load/delete`)
- 実装:
  - `MemoryCheckpointStore`（プロセス内）
  - `FileCheckpointStore`（ファイル）
- checkpoint id:
  - `^[0-9a-f]{32}$` を許可
- state は typed dataclass を経由して保存/復元される
- query / config snapshot 不一致時は fail-fast (`BojValidationError`)

## 8. テスト構成

```text
tests/
  unit/
  unit_async/
  integration/
  integration_async/
  contract_live/
```

方針:

- sync/async parity を unit + integration で継続検証
- live contract は `-m live` で明示実行

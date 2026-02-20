# BOJ API Overview

最終更新: 2026-02-20  
対象: `boj-api-client` が対応する BOJ 時系列 API

## 1. 対象エンドポイント

- `GET /getDataCode`
- `GET /getDataLayer`
- `GET /getMetadata`

ベース URL:

- `https://www.stat-search.boj.or.jp/api/v1`

本パッケージは通信フォーマットを `format=json` に固定し、公開 API では常に Python オブジェクトを返す。

## 2. クライアントメソッド対応

| API | 同期 | 非同期 | ページ iterator |
| --- | --- | --- | --- |
| getDataCode | `client.timeseries.get_data_code(query)` | `await client.timeseries.get_data_code(query)` | `iter_data_code` |
| getDataLayer | `client.timeseries.get_data_layer(query)` | `await client.timeseries.get_data_layer(query)` | `iter_data_layer` |
| getMetadata | `client.timeseries.get_metadata(query)` | `await client.timeseries.get_metadata(query)` | なし |

## 3. Query モデル

### 3.1 `DataCodeQuery`

- 必須: `db`, `code`
- 任意: `lang`, `start_date`, `end_date`
- `code` は `Sequence[str]` を受け付け、内部で `tuple[str, ...]` に正規化される

### 3.2 `DataLayerQuery`

- 必須: `db`, `frequency`, `layer1`
- 任意: `lang`, `layer2`-`layer5`, `start_date`, `end_date`
- `layer` は連続指定必須（途中欠落は validation error）

### 3.3 `MetadataQuery`

- 必須: `db`
- 任意: `lang`

## 4. API 制約とクライアント動作

### 4.1 主要制約

- 1 リクエストあたり系列数上限: 250（code API）
- 1 リクエストあたりデータ数上限: 60,000（系列数 x 期数）
- layer API の系列上限: 1,250
- `NEXTPOSITION` によるページング継続

### 4.2 実装上の挙動

- `getDataCode`:
  - `code` が 250 超過なら自動で分割し統合
  - `code` 重複は入力順を維持して dedupe
- `getDataLayer`:
  - 既定では 1,250 超過で `BojValidationError`
  - `TimeSeriesConfig(enable_layer_auto_partition=True)` で metadata 経由 fallback を許可
- `NEXTPOSITION`:
  - `get_*` は全ページを自動取得
  - `iter_*` はページ単位で返却
- 文字列:
  - 公開値はすべて Python `str`（Unicode）
- データなし:
  - `MESSAGEID=M181030I` は成功扱い
  - `series` は空タプルで返却

## 5. レスポンスモデル

共通:

- `ApiEnvelope`: `status`, `message_id`, `message`, `date`

ドメイン:

- `DataCodeResponse`
- `DataLayerResponse`
- `MetadataResponse`
- `TimeSeries`
- `TimeSeriesPoint`
- `MetadataEntry`

## 6. エラーハンドリング

主な対応:

- `STATUS=400` -> `BojValidationError`
- `STATUS=500` -> `BojServerError`
- `STATUS=503` -> `BojUnavailableError`
- 通信失敗 -> `BojTransportError`
- HTTP/body status 不整合や payload 異常 -> `BojProtocolError`
- 途中失敗（部分成功あり）-> `BojPartialResultError`

再試行対象:

- 通信エラー
- 一時的 500 / 503

再試行しない:

- validation error（400 系）

## 7. Checkpoint / Resume

`get_data_code` / `get_data_layer` は `checkpoint_id` を受け付ける。

- 途中失敗時に `BojPartialResultError.checkpoint_id` が返る
- 同一 query で `checkpoint_id` を渡すと再開
- query/config snapshot 不一致は `BojValidationError`
- store:
  - `MemoryCheckpointStore`（既定）
  - `FileCheckpointStore`（任意差し替え）

## 8. live contract test

実行:

```bash
BOJ_RUN_LIVE=1 pytest -m live -q tests/contract_live
```

`getDataLayer` を live 実行する場合:

- `BOJ_LIVE_LAYER1`（必須）
- `BOJ_LIVE_LAYER_DB`（任意、既定 `MD10`）
- `BOJ_LIVE_LAYER_FREQUENCY`（任意、既定 `Q`）

## 9. 参照

- API マニュアル: `https://www.stat-search.boj.or.jp/info/api_manual.pdf`
- API 利用上の注意: `https://www.stat-search.boj.or.jp/info/api_notice.pdf`

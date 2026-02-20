# contract_live tests

実 API への疎通を検証する任意テストです。  
通常の `pytest` では実行対象に含めない運用を想定し、`-m live` で明示実行します。

## ローカル実行

```bash
BOJ_RUN_LIVE=1 pytest -m live -q tests/contract_live
```

`getDataLayer` の live テストは条件が広いと 1,250 件超過になりやすいため、次の環境変数を指定した場合のみ実行されます。

- `BOJ_LIVE_LAYER1`
- 任意: `BOJ_LIVE_LAYER_DB`（既定 `MD10`）
- 任意: `BOJ_LIVE_LAYER_FREQUENCY`（既定 `Q`）

未指定時は `getDataLayer` の live テストを skip します。

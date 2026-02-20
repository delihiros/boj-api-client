# Live Fixture Snapshot (2026-02-19)

このディレクトリは、2026-02-19（JST）に BOJ API へ実リクエストして取得した JSON スナップショットです。
Phase 1 の contract/integration テストで利用します。

## Files

- `get_data_code_success.json`  
  `GET /getDataCode` 正常系（`MESSAGEID=M181000I`）
- `get_data_code_no_data_m181030i.json`  
  `GET /getDataCode` 該当データなし（`MESSAGEID=M181030I`）
- `get_data_layer_page1.json`  
  `GET /getDataLayer` 1ページ目（`NEXTPOSITION=255`）
- `get_data_layer_page2.json`  
  `GET /getDataLayer` 続きページ（`startPosition=255`）
- `get_metadata_success.json`  
  `GET /getMetadata` 正常系（`MESSAGEID=M181000I`）
- `error_missing_db_http400.json`  
  `GET /getMetadata` パラメータ不足（HTTP 400, `MESSAGEID=M181004E`）

## Notes

- フォーマットはすべて JSON（`format=json`）。
- 取得時点の公開データに依存するため、値は将来変わり得ます。
- テストでは配列要素の全量一致ではなく、構造・必須キー・型・`STATUS`/`MESSAGEID` を主対象に検証します。

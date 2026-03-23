# 競艇データ自動収集

ボートレース公式サイト（boatrace.jp）から競艇レースデータを毎日自動収集し、CSVとしてGitHubに保存するシステム。

## 競輪版との違い

| 項目 | 競輪 | 競艇 |
|------|------|------|
| データソース | 楽天Kドリームス | **ボートレース公式** |
| 選手数/レース | 7〜9人 | **常に6人** |
| レース数/日 | 場ごと12R | 場ごと**最大12R** |
| 開催場数/日 | 〜10場 | 〜**24場** |
| CSV列 | 競走得点・脚質 | **コース・ST・勝率** |
| 実行時刻 | JST 9:00 | **JST 10:00** |

## セットアップ手順

### 1. リポジトリ作成 & push

```bash
git clone https://github.com/あなた/kyotei-data.git
cd kyotei-data
git add .
git commit -m "initial commit"
git push
```

### 2. GitHub Secrets 設定

`Settings → Secrets and variables → Actions`:

| Secret名 | 値 |
|----------|-----|
| `DISCORD_WEBHOOK_URL` | Discord Webhook URL |

### 3. 動作確認

`Actions → 競艇データ自動収集 → Run workflow`

---

## CSVのカラム

| カラム | 内容 |
|--------|------|
| race_id | レースID（日付+場コード+R番号） |
| date | 日付（YYYY-MM-DD） |
| jcd | 競艇場コード（01〜24） |
| venue | 競艇場名（桐生・戸田...） |
| race_no | レース番号（1〜12） |
| waku | 枠番（1〜6） |
| reg_no | 選手登録番号 |
| player_name | 選手名 |
| branch | 支部 |
| age | 年齢 |
| grade | 級別（A1/A2/B1/B2） |
| motor_no | モーター番号 |
| boat_no | ボート番号 |
| rank | 着順 |
| result_time | タイム |
| course | 進入コース |
| st | スタートタイミング |
| payout | 払戻金テキスト |
| weather | 天候・風速・波高 |

## 競艇場コード一覧

| コード | 場名 | コード | 場名 |
|--------|------|--------|------|
| 01 | 桐生 | 13 | 尼崎 |
| 02 | 戸田 | 14 | 鳴門 |
| 03 | 江戸川 | 15 | 丸亀 |
| 04 | 平和島 | 16 | 児島 |
| 05 | 多摩川 | 17 | 宮島 |
| 06 | 浜名湖 | 18 | 徳山 |
| 07 | 蒲郡 | 19 | 下関 |
| 08 | 常滑 | 20 | 若松 |
| 09 | 津 | 21 | 芦屋 |
| 10 | 三国 | 22 | 福岡 |
| 11 | びわこ | 23 | 唐津 |
| 12 | 住之江 | 24 | 大村 |

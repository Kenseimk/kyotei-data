"""
auto_runner.py（競艇版）
「どの年月を取得するか」を自動判断してスクレイパーを呼び出す。
Discord通知（開始・完了・エラー）も担当。
"""

import os
import sys
import json
import requests
import subprocess
from datetime import datetime
from pathlib import Path

# ========== 設定 ==========
DATA_DIR       = Path("kyotei_data")
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
START_YEAR     = 2023
START_MONTH    = 1

DISCORD_WEBHOOK  = os.environ.get("DISCORD_WEBHOOK_URL", "")
NOTION_TOKEN     = os.environ.get("NOTION_TOKEN", "")
NOTION_DB_ID     = "8bc881d1-dfd8-47bd-a8c7-2bef02012fc8"  # 🚤 競艇スクレイピングログDB
FORCE_YEAR  = os.environ.get("FORCE_YEAR", "").strip()
FORCE_MONTH = os.environ.get("FORCE_MONTH", "").strip()

# ========== Notion通知 ==========

def notion_log(title, status, year, month, race_count=0, row_count=0, elapsed_min=0, error_msg=""):
    """NotionのスクレイピングログDBにレコードを追加"""
    if not NOTION_TOKEN:
        print(f"[Notion通知スキップ] {title}")
        return
    now = datetime.now()
    payload = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": {
            "タイトル":      {"title": [{"text": {"content": title}}]},
            "ステータス":    {"select": {"name": status}},
            "対象年月":      {"rich_text": [{"text": {"content": f"{year}年{month}月"}}]},
            "取得レース数":  {"number": race_count},
            "総行数":        {"number": row_count},
            "実行日時":      {"date": {"start": now.strftime("%Y-%m-%dT%H:%M:%S"), "time_zone": "Asia/Tokyo"}},
            "実行時間(分)":  {"number": round(elapsed_min, 1)},
            "エラー内容":    {"rich_text": [{"text": {"content": error_msg[:500]}}]},
            "GitHubリポジトリ": {"url": os.environ.get("GITHUB_SERVER_URL", "") + "/" + os.environ.get("GITHUB_REPOSITORY", "") or None},
        }
    }
    if not payload["properties"]["GitHubリポジトリ"]["url"]:
        del payload["properties"]["GitHubリポジトリ"]
    try:
        r = requests.post(
            "https://api.notion.com/v1/pages",
            headers={
                "Authorization": f"Bearer {NOTION_TOKEN}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
        r.raise_for_status()
        print(f"  📝 Notionログ記録: {title}")
    except Exception as e:
        print(f"  Notion通知失敗: {e}")

# ========== Discord通知 ==========

def notify(title, description, color=0x1e90ff):
    if not DISCORD_WEBHOOK:
        print(f"[Discord通知スキップ] {title}: {description}")
        return
    payload = {
        "embeds": [{
            "title": title,
            "description": description,
            "color": color,
            "footer": {"text": f"kyotei-scraper • {datetime.now().strftime('%Y-%m-%d %H:%M JST')}"}
        }]
    }
    try:
        r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"Discord通知失敗: {e}")

def notify_start(year, month, remaining):
    notify(
        title="🚤 競艇データ収集 開始",
        description=(
            f"**対象:** {year}年{month}月\n"
            f"**残り未取得月数:** {remaining}ヶ月\n"
            f"**実行環境:** GitHub Actions"
        ),
        color=0x1e90ff
    )

def notify_done(year, month, race_count, row_count):
    notify(
        title="✅ 収集完了",
        description=(
            f"**対象:** {year}年{month}月\n"
            f"**取得レース数:** {race_count:,}レース\n"
            f"**総行数:** {row_count:,}行\n"
            f"**CSV:** kyotei_data/{year}_{month:02d}_kyotei.csv"
        ),
        color=0x2ecc71
    )

def notify_all_done(total_months, total_races):
    notify(
        title="🎉 全月収集完了！",
        description=(
            f"**収集完了月数:** {total_months}ヶ月\n"
            f"**推定総レース数:** {total_races:,}レース以上\n"
            f"スコアモデルの構築を開始できます。"
        ),
        color=0xf1c40f
    )

def notify_error(year, month, error_msg):
    notify(
        title="❌ エラー発生",
        description=(
            f"**対象:** {year}年{month}月\n"
            f"**エラー:** {error_msg[:300]}\n"
            f"次回の実行で `--resume` により自動再試行します。"
        ),
        color=0xe74c3c
    )

# ========== 取得状況の確認 ==========

def get_target_months():
    now = datetime.now()
    months = []
    year, month = START_YEAR, START_MONTH
    while (year, month) <= (now.year, now.month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months

def is_month_complete(year, month):
    return (DATA_DIR / f"{year}_{month:02d}_kyotei.csv").exists()

def is_month_partial(year, month):
    return (CHECKPOINT_DIR / f"{year}_{month:02d}_checkpoint.json").exists()

def find_next_target():
    for year, month in get_target_months():
        if not is_month_complete(year, month):
            return year, month
    return None

def count_remaining():
    return sum(1 for y, m in get_target_months() if not is_month_complete(y, m))

# ========== メイン ==========

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    if FORCE_YEAR and FORCE_MONTH:
        year  = int(FORCE_YEAR)
        month = int(FORCE_MONTH)
        print(f"[手動指定] {year}年{month}月")
    else:
        target = find_next_target()
        if target is None:
            notify_all_done(
                total_months=len(get_target_months()),
                total_races=len(get_target_months()) * 288  # 24場×12R=288レース/日×開催日数
            )
            print("全月のデータ収集が完了しています。")
            return
        year, month = target

    remaining = count_remaining()
    is_resume = is_month_partial(year, month) and not is_month_complete(year, month)

    print(f"{'='*55}")
    print(f"対象: {year}年{month}月 / 残り: {remaining}ヶ月 / 再開: {is_resume}")
    print(f"{'='*55}")

    notify_start(year, month, remaining)
    notion_log(f"🔄 {year}年{month}月 収集開始", "🔄 実行中", year, month)

    start_time = datetime.now()
    cmd = [
        sys.executable, "scripts/kyotei_scraper.py",
        "--year", str(year),
        "--month", str(month),
    ]
    if is_resume:
        cmd.append("--resume")

    try:
        result = subprocess.run(cmd, check=False, text=True)
        exit_code = result.returncode
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds() / 60
        notify_error(year, month, str(e))
        notion_log(f"❌ {year}年{month}月 エラー", "❌ エラー", year, month,
                   elapsed_min=elapsed, error_msg=str(e))
        raise

    elapsed = (datetime.now() - start_time).total_seconds() / 60

    if exit_code == 1:
        notify_error(year, month, "exit code 1: スクレイパーが異常終了")
        print("⚠️  スクレイパーが異常終了しました。")
    elif exit_code == 2:
        print(f"⏸️  半分取得完了。CSVをpushして次のアクションで残りを取得します。")

    csv_path = DATA_DIR / f"{year}_{month:02d}_kyotei.csv"
    if csv_path.exists():
        import pandas as pd
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        race_count = df['race_id'].nunique() if 'race_id' in df.columns else 0
        notify_done(year, month, race_count, len(df))

        if exit_code == 2:
            notion_log(f"⏸️ {year}年{month}月 半分完了", "🔄 実行中", year, month,
                       race_count=race_count, row_count=len(df), elapsed_min=elapsed,
                       error_msg="前半取得完了。次回resumeで後半を取得します。")
        else:
            notion_log(f"✅ {year}年{month}月 完了", "✅ 完了", year, month,
                       race_count=race_count, row_count=len(df), elapsed_min=elapsed)
            if count_remaining() == 0:
                notify_all_done(
                    total_months=len(get_target_months()),
                    total_races=len(get_target_months()) * 1500
                )
    else:
        print(f"⚠️  CSVが見つかりません: {csv_path}")
        print("チェックポイントは保存されているので次回resumeで再開されます。")
        notify_error(year, month, f"CSVファイルが生成されませんでした（チェックポイントは保存済み）: {csv_path}")
        notion_log(f"❌ {year}年{month}月 CSVなし", "❌ エラー", year, month,
                   error_msg=f"CSVが生成されませんでした: {csv_path}")

if __name__ == "__main__":
    main()

"""
競艇データ収集スクリプト v1.0
ソース: ボートレース公式サイト (boatrace.jp)

URL構造（公式・安定）:
  レース結果一覧: /owpc/pc/race/resultlist?jcd={場コード}&hd={YYYYMMDD}
  レース結果詳細: /owpc/pc/race/raceresult?rno={R番号}&jcd={場コード}&hd={YYYYMMDD}
  出走表:         /owpc/pc/race/racelist?rno={R番号}&jcd={場コード}&hd={YYYYMMDD}
  オッズ(3連単):  /owpc/pc/race/odds3t?rno={R番号}&jcd={場コード}&hd={YYYYMMDD}

取得データ:
  - 選手情報（枠番・選手名・支部・年齢・級別・モーター・ボート・全国/当地勝率・F数・L数・平均ST）
  - レース結果（着順・枠番・選手名・タイム・コース・スタートタイミング・決まり手）
  - 払戻金（3連単・3連複・2連単・2連複・拡連複・単勝・複勝）

使い方:
  pip install requests beautifulsoup4 pandas tqdm lxml
  python kyotei_scraper.py --year 2024 --month 10
  python kyotei_scraper.py --year 2024 --start_month 1 --end_month 12
  python kyotei_scraper.py --year 2024 --month 10 --resume
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time, re, json, random, argparse
from datetime import datetime, timedelta
from tqdm import tqdm
from pathlib import Path

# ========== 設定 ==========
BASE_URL = "https://www.boatrace.jp"
OUTPUT_DIR = Path("./kyotei_data")
CHECKPOINT_DIR = OUTPUT_DIR / "checkpoints"

INTERVAL_MIN = 3.0      # 公式サイトなので余裕を持たせる
INTERVAL_MAX = 6.0
BATCH_SIZE = 20       # 20件ごとにチェックポイント保存
BATCH_REST_MIN = 15
BATCH_REST_MAX = 30
BACKOFF_BASE = 15.0

# 全24競艇場コード
VENUES = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島",
    "05": "多摩川", "06": "浜名湖", "07": "蒲郡",  "08": "常滑",
    "09": "津",    "10": "三国",  "11": "びわこ", "12": "住之江",
    "13": "尼崎",  "14": "鳴門",  "15": "丸亀",  "16": "児島",
    "17": "宮島",  "18": "徳山",  "19": "下関",  "20": "若松",
    "21": "芦屋",  "22": "福岡",  "23": "唐津",  "24": "大村",
}

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
]

def make_headers(ua=None):
    return {
        "User-Agent": ua or random.choice(UA_POOL),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.boatrace.jp/",
    }

# ========== セッション管理 ==========
_session = None
_session_count = 0
SESSION_REFRESH = 80

def new_session():
    s = requests.Session()
    s.headers.update(make_headers())
    try:
        s.get(f"{BASE_URL}/", timeout=10)
        time.sleep(random.uniform(1.5, 3.0))
    except Exception:
        pass
    return s

def get_session():
    global _session, _session_count
    if _session is None or _session_count >= SESSION_REFRESH:
        _session = new_session()
        _session_count = 0
    _session_count += 1
    return _session

def human_wait(extra=0.0):
    time.sleep(random.uniform(INTERVAL_MIN, INTERVAL_MAX) + extra)

def batch_rest(n):
    global _session
    wait = random.uniform(BATCH_REST_MIN, BATCH_REST_MAX)
    print(f"\n  ☕ バッチ{n}完了 - {wait:.0f}秒休憩...")
    time.sleep(wait)
    _session = None

def fetch(url, retries=4):
    session = get_session()
    for attempt in range(retries):
        try:
            resp = session.get(url, headers=make_headers(), timeout=15)
            if resp.status_code == 200:
                resp.encoding = resp.apparent_encoding
                return BeautifulSoup(resp.text, "html.parser")
            elif resp.status_code == 404:
                return None
            elif resp.status_code in (429, 503):
                wait = BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 5)
                print(f"\n  ⚠️  HTTP {resp.status_code} → {wait:.0f}秒後リトライ")
                time.sleep(wait)
                global _session
                _session = None
            else:
                time.sleep(BACKOFF_BASE)
        except requests.exceptions.ConnectionError:
            time.sleep(BACKOFF_BASE * (attempt + 1))
        except requests.exceptions.Timeout:
            time.sleep(BACKOFF_BASE)
        except Exception as e:
            print(f"  エラー: {e}")
            time.sleep(BACKOFF_BASE)
    return None

# ========== チェックポイント ==========

def save_checkpoint(year, month, done_items, rows):
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    cp = {
        "year": year, "month": month,
        "done_items": done_items,
        "saved_at": datetime.now().isoformat(),
    }
    with open(CHECKPOINT_DIR / f"{year}_{month:02d}_checkpoint.json", "w", encoding="utf-8") as f:
        json.dump(cp, f, ensure_ascii=False, indent=2)
    if rows:
        pd.DataFrame(rows).to_csv(
            CHECKPOINT_DIR / f"{year}_{month:02d}_partial.csv",
            index=False, encoding="utf-8-sig"
        )

def load_checkpoint(year, month):
    cp_path = CHECKPOINT_DIR / f"{year}_{month:02d}_checkpoint.json"
    tmp_path = CHECKPOINT_DIR / f"{year}_{month:02d}_partial.csv"
    if not cp_path.exists():
        return [], []
    with open(cp_path, encoding="utf-8") as f:
        cp = json.load(f)
    done_items = [tuple(x) for x in cp.get("done_items", [])]
    rows = []
    if tmp_path.exists():
        rows = pd.read_csv(tmp_path, encoding="utf-8-sig").to_dict(orient="records")
    print(f"  📂 復元: {len(done_items)}件完了済み ({cp['saved_at']})")
    return done_items, rows

# ========== Step1: 開催日程取得 ==========

def get_race_dates_for_month(year, month):
    """
    指定月に開催があった（jcd, date）のペアを返す。
    boatrace.jpの結果一覧ページから各場の開催日を収集。
    """
    if month == 12:
        days = (datetime(year + 1, 1, 1) - datetime(year, 12, 1)).days
    else:
        days = (datetime(year, month + 1, 1) - datetime(year, month, 1)).days

    targets = []  # (jcd, date_str, venue_name)
    print(f"\n📅 {year}年{month}月 開催日程スキャン中...")

    for day in tqdm(range(1, days + 1), desc="日付スキャン"):
        date_obj = datetime(year, month, day)
        date_str = date_obj.strftime("%Y%m%d")

        # その日の全場の結果一覧ページを確認
        url = f"{BASE_URL}/owpc/pc/race/index?hd={date_str}"
        soup = fetch(url)
        if soup is None:
            human_wait()
            continue

        # 開催場のリンクを抽出
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # resultlist?jcd=XX&hd=YYYYMMDD 形式のリンク
            m = re.search(r"resultlist\?.*?jcd=(\d{2}).*?hd=(\d{8})", href)
            if not m:
                m = re.search(r"jcd=(\d{2}).*?hd=(\d{8})", href)
            if m:
                jcd = m.group(1)
                hd  = m.group(2)
                key = (jcd, hd)
                if key not in targets:
                    targets.append(key)

        human_wait()

    print(f"  → {len(targets)}開催発見")
    return targets

# ========== Step2: 1開催分のレースを取得 ==========

def parse_result_list(jcd, hd):
    """
    結果一覧ページから何レースあるか取得（通常1〜12R）
    """
    url = f"{BASE_URL}/owpc/pc/race/resultlist?jcd={jcd}&hd={hd}"
    soup = fetch(url)
    if soup is None:
        return []

    race_nos = []
    for a in soup.find_all("a", href=True):
        m = re.search(r"rno=(\d+)&jcd=\d+&hd=\d+", a["href"])
        if m:
            rno = int(m.group(1))
            if rno not in race_nos:
                race_nos.append(rno)
    return sorted(race_nos)

def parse_race(jcd, hd, rno):
    """
    1レース分のデータを取得。
    出走表・結果・払戻を1レコードにまとめる（選手単位）。
    """
    venue_name = VENUES.get(jcd, jcd)
    date_fmt = f"{hd[:4]}-{hd[4:6]}-{hd[6:8]}"
    race_id = f"{hd}{jcd}{rno:02d}"

    # --- 出走表 ---
    racelist_url = f"{BASE_URL}/owpc/pc/race/racelist?rno={rno}&jcd={jcd}&hd={hd}"
    soup_list = fetch(racelist_url)
    human_wait()

    # --- レース結果 ---
    result_url = f"{BASE_URL}/owpc/pc/race/raceresult?rno={rno}&jcd={jcd}&hd={hd}"
    soup_result = fetch(result_url)
    human_wait()

    if soup_list is None and soup_result is None:
        return []

    # --- 出走表パース ---
    players = {}  # waku → {name, branch, age, grade, motor, boat, win_rate, local_win_rate, f, l, avg_st}
    if soup_list:
        try:
            tbody = soup_list.find("tbody", class_=re.compile(r"is-fs"))
            if not tbody:
                tbody = soup_list.find("table", class_=re.compile(r"is-w[0-9]"))

            # テーブルから選手情報を取得
            tables = pd.read_html(str(soup_list))
            for t in tables:
                cols = " ".join(str(c) for c in t.columns)
                if "選手名" in cols or "登録番号" in cols or len(t.columns) >= 5:
                    if len(t) >= 6:  # 最低6選手
                        for _, row in t.iterrows():
                            vals = [str(v) for v in row.values]
                            # 枠番(1-6)を探す
                            for i, v in enumerate(vals):
                                if re.match(r'^[1-6]$', v.strip()):
                                    waku = v.strip()
                                    players[waku] = {
                                        "waku": waku,
                                        "raw": " ".join(vals),
                                    }
                                    break
                        break
        except Exception:
            pass

        # BeautifulSoupで直接パース（より確実）
        players = {}
        try:
            # 選手名セル: class="is-fs12"など
            player_rows = soup_list.select("table tbody tr") or soup_list.select("tr.is-")
            waku = 1
            for row in soup_list.find_all("tr"):
                tds = row.find_all("td")
                if len(tds) < 3:
                    continue
                # 枠番セル
                waku_td = tds[0].get_text(strip=True)
                if not re.match(r'^[1-6]$', waku_td):
                    continue
                waku = waku_td

                # 選手名・登録番号
                name_td = tds[2].get_text("\n", strip=True) if len(tds) > 2 else ""
                name_parts = name_td.split("\n")
                reg_no = name_parts[0] if name_parts else ""
                name = name_parts[1] if len(name_parts) > 1 else ""

                # 支部・年齢
                branch_age = tds[3].get_text("\n", strip=True) if len(tds) > 3 else ""
                ba_parts = branch_age.split("\n")

                # 各種成績
                motor_no   = tds[4].get_text(strip=True) if len(tds) > 4 else ""
                boat_no    = tds[5].get_text(strip=True) if len(tds) > 5 else ""

                players[waku] = {
                    "waku":       waku,
                    "reg_no":     reg_no,
                    "player_name": name or name_td[:10],
                    "branch":     ba_parts[0] if ba_parts else "",
                    "age":        ba_parts[1] if len(ba_parts) > 1 else "",
                    "grade":      ba_parts[2] if len(ba_parts) > 2 else "",
                    "motor_no":   motor_no,
                    "boat_no":    boat_no,
                }
        except Exception:
            pass

    # --- 結果ページパース ---
    result_lookup = {}   # waku → {rank, time, course, st, finish_type}
    payout_text = ""
    weather_info = {}

    if soup_result:
        try:
            # 着順テーブル
            for row in soup_result.find_all("tr"):
                tds = row.find_all("td")
                if len(tds) < 4:
                    continue
                rank_td = tds[0].get_text(strip=True)
                if not re.match(r'^[1-6]$', rank_td):
                    continue
                waku_td = tds[1].get_text(strip=True)
                name_td = tds[2].get_text(strip=True) if len(tds) > 2 else ""
                time_td = tds[3].get_text(strip=True) if len(tds) > 3 else ""

                result_lookup[waku_td] = {
                    "rank":        rank_td,
                    "result_time": time_td,
                }

            # コース・STテーブル（スタート情報）
            # class="is-colo1"などのSTセル
            st_rows = soup_result.select("table.is-w495 tr") or []
            for row in st_rows:
                tds = row.find_all("td")
                if len(tds) >= 3:
                    course = tds[0].get_text(strip=True)
                    waku_in_st = tds[1].get_text(strip=True)
                    st = tds[2].get_text(strip=True)
                    if re.match(r'^[1-6]$', waku_in_st):
                        if waku_in_st in result_lookup:
                            result_lookup[waku_in_st]["course"] = course
                            result_lookup[waku_in_st]["st"] = st

            # 払戻金
            payout_parts = []
            for table in soup_result.find_all("table"):
                t_text = table.get_text(" ", strip=True)
                if "3連単" in t_text or "3連複" in t_text or "2連単" in t_text:
                    payout_parts.append(t_text[:300])
            payout_text = " | ".join(payout_parts)[:400]

            # 天候・風速・波高
            try:
                weather_div = soup_result.find("div", class_=re.compile(r"weather"))
                if weather_div:
                    weather_info["weather_text"] = weather_div.get_text(" ", strip=True)[:100]
            except Exception:
                pass

        except Exception as e:
            pass

    # --- 統合 ---
    rows = []
    for waku in ["1", "2", "3", "4", "5", "6"]:
        player = players.get(waku, {"waku": waku})
        result = result_lookup.get(waku, {})

        row = {
            "race_id":     race_id,
            "date":        date_fmt,
            "jcd":         jcd,
            "venue":       venue_name,
            "race_no":     rno,
            # 選手情報
            "waku":        waku,
            "reg_no":      player.get("reg_no", ""),
            "player_name": player.get("player_name", ""),
            "branch":      player.get("branch", ""),
            "age":         player.get("age", ""),
            "grade":       player.get("grade", ""),
            "motor_no":    player.get("motor_no", ""),
            "boat_no":     player.get("boat_no", ""),
            # レース結果
            "rank":        result.get("rank", ""),
            "result_time": result.get("result_time", ""),
            "course":      result.get("course", ""),
            "st":          result.get("st", ""),
            # 払戻・天候
            "payout":      payout_text,
            "weather":     weather_info.get("weather_text", ""),
        }
        rows.append(row)

    return rows

# ========== メイン処理 ==========

def scrape_month(year, month, resume=False):
    print(f"\n{'='*55}")
    print(f"🚤 競艇データ収集 v1.0: {year}年{month}月")
    print(f"{'='*55}")

    done_items, all_rows = [], []
    if resume:
        done_items, all_rows = load_checkpoint(year, month)

    # 開催日程取得
    targets = get_race_dates_for_month(year, month)
    if not targets:
        print("⚠️  開催が見つかりませんでした")
        return []

    # 各開催のレース番号を展開
    all_races = []  # (jcd, hd, rno)
    print(f"\n🔍 レース番号取得中...")
    done_set = set(done_items)

    for jcd, hd in tqdm(targets, desc="開催スキャン"):
        race_nos = parse_result_list(jcd, hd)
        for rno in race_nos:
            key = (jcd, hd, str(rno))
            all_races.append(key)
        human_wait()

    remaining = [r for r in all_races if r not in done_set]
    print(f"  未処理: {len(remaining)}件 / 全{len(all_races)}件")

    # 各レース取得
    batch_count = 0
    for i, (jcd, hd, rno) in enumerate(tqdm(remaining, desc="レース取得")):
        try:
            rows = parse_race(jcd, hd, int(rno))
            all_rows.extend(rows)
            done_items.append((jcd, hd, rno))

            if (i + 1) % BATCH_SIZE == 0:
                batch_count += 1
                save_checkpoint(year, month, done_items, all_rows)
                batch_rest(batch_count)

        except KeyboardInterrupt:
            print("\n\n⚡ 中断 → チェックポイント保存中...")
            save_checkpoint(year, month, done_items, all_rows)
            print("  → 次回: --resume で再開")
            raise
        except Exception as e:
            print(f"  ⚠️  スキップ {jcd}/{hd}/{rno}: {e}")
            continue

    save_checkpoint(year, month, done_items, all_rows)
    return all_rows

def save_month_csv(year, month, rows):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fp = OUTPUT_DIR / f"{year}_{month:02d}_kyotei.csv"
    pd.DataFrame(rows).to_csv(fp, index=False, encoding="utf-8-sig")
    print(f"\n💾 保存: {fp} ({len(rows)}行)")
    return fp

def main():
    parser = argparse.ArgumentParser(description="競艇データ収集 v1.0")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int)
    parser.add_argument("--start_month", type=int)
    parser.add_argument("--end_month", type=int)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    if args.month:
        months = [args.month]
    elif args.start_month and args.end_month:
        months = list(range(args.start_month, args.end_month + 1))
    else:
        print("エラー: --month または --start_month/--end_month を指定してください")
        return

    all_total = []
    for month in months:
        rows = scrape_month(args.year, month, args.resume)
        if rows:
            save_month_csv(args.year, month, rows)
            all_total.extend(rows)

    if len(months) > 1 and all_total:
        fp = OUTPUT_DIR / f"{args.year}_all_kyotei.csv"
        pd.DataFrame(all_total).to_csv(fp, index=False, encoding="utf-8-sig")
        print(f"\n✅ 年間統合: {fp} ({len(all_total)}行)")

    print("\n🎉 完了！")

if __name__ == "__main__":
    main()

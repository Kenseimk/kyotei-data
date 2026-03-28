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

INTERVAL_MIN = 0.8      # 短縮（1.5→0.8秒）
INTERVAL_MAX = 1.5      # 短縮（3.0→1.5秒）
BATCH_SIZE = 50         # 拡大（20→50）バッチ休憩の頻度を下げる
BATCH_REST_MIN = 3      # 短縮（5→3秒）
BATCH_REST_MAX = 6      # 短縮（10→6秒）
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

def save_checkpoint(year, month, done_items, rows, all_races=None):
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    cp = {
        "year": year, "month": month,
        "done_items": done_items,
        "saved_at": datetime.now().isoformat(),
    }
    if all_races is not None:
        cp["all_races"] = [list(r) for r in all_races]
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
        return [], [], None
    with open(cp_path, encoding="utf-8") as f:
        cp = json.load(f)
    done_items = [tuple(x) for x in cp.get("done_items", [])]
    all_races = [tuple(x) for x in cp["all_races"]] if cp.get("all_races") else None
    rows = []
    if tmp_path.exists():
        rows = pd.read_csv(tmp_path, encoding="utf-8-sig").to_dict(orient="records")
    print(f"  📂 復元: {len(done_items)}件完了済み ({cp['saved_at']})")
    if all_races:
        print(f"  📂 レースリスト復元: {len(all_races)}件（日程スキャンをスキップ）")
    return done_items, rows, all_races

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

ZEN2HAN = str.maketrans("０１２３４５６７８９", "0123456789")

def z2h(s):
    """全角数字→半角数字"""
    return s.translate(ZEN2HAN)

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
    # 実際のHTML構造:
    #   td[0] = 枠番（全角 '１'〜'６'）
    #   td[2] = <div class="is-fs11">登録番号 / 級別</div>
    #           <div class="is-fs18"><a>選手名</a></div>
    #           <div class="is-fs11">支部/支部 年齢歳/体重kg</div>
    #   td[3] = F数 L数 平均ST
    #   td[6] = モーター番号 2連率 3連率
    #   td[7] = ボート番号 2連率 3連率
    players = {}
    if soup_list:
        try:
            for row in soup_list.find_all("tr"):
                tds = row.find_all("td")
                if len(tds) < 8:
                    continue
                # 枠番は必ず全角（'１'〜'６'）の行のみ処理
                # 半角数字の行はサブ行（展示タイム等）のため除外
                waku_raw = tds[0].get_text(strip=True)
                if waku_raw not in "１２３４５６":
                    continue
                waku = z2h(waku_raw)

                # 選手情報（tds[2]のネストdivから個別取得）
                # divs=0のサブ行（後出コースやST行）はスキップ
                reg_no = ""
                player_name = ""
                branch = ""
                age = ""
                grade = ""
                divs = tds[2].find_all("div")
                if not divs:
                    continue
                if divs:
                    # div[0]: "3839 / B1" → 登録番号・級別
                    d0 = divs[0].get_text(" ", strip=True)
                    m = re.search(r'(\d{4})', d0)
                    if m:
                        reg_no = m.group(1)
                    g = re.search(r'([A-Ba-b]\d)', d0)
                    if g:
                        grade = g.group(1).upper()
                    # div[1]: 選手名
                    if len(divs) > 1:
                        player_name = divs[1].get_text(strip=True)
                    # div[2]: "静岡/静岡 47歳/62.1kg"
                    if len(divs) > 2:
                        info = divs[2].get_text(" ", strip=True)
                        bm = re.search(r'(\S+)/\S+\s+(\d+)歳', info)
                        if bm:
                            branch = bm.group(1)
                            age = bm.group(2)

                # モーター番号（tds[6]の先頭数値）
                motor_no = ""
                m = re.search(r'^(\d+)', tds[6].get_text(strip=True))
                if m:
                    motor_no = m.group(1)

                # ボート番号（tds[7]の先頭数値）
                boat_no = ""
                m = re.search(r'^(\d+)', tds[7].get_text(strip=True))
                if m:
                    boat_no = m.group(1)

                players[waku] = {
                    "waku":        waku,
                    "reg_no":      reg_no,
                    "player_name": player_name,
                    "branch":      branch,
                    "age":         age,
                    "grade":       grade,
                    "motor_no":    motor_no,
                    "boat_no":     boat_no,
                }
        except Exception:
            pass

    # --- 結果ページパース ---
    # is-w495テーブルの構造:
    #   table[0]: 着順テーブル  td[0]=着順(全角), td[1]=枠番, td[2]=登録番号+選手名, td[3]=タイム
    #   table[1]: STテーブル    td[0]="コース番号 .ST" (例: "1 .12")
    #   table[2+]: 払戻テーブル
    result_lookup = {}   # waku → {rank, result_time}
    st_by_course = {}    # course → st  (コース番号→スタートタイム)
    payout_text = ""
    weather_info = {}

    if soup_result:
        try:
            is_tables = soup_result.find_all("table", class_="is-w495")

            # table[0]: 着順テーブル
            if is_tables:
                for row in is_tables[0].find_all("tr"):
                    tds = row.find_all("td")
                    if len(tds) < 4:
                        continue
                    rank = z2h(tds[0].get_text(strip=True))
                    if not re.match(r'^[1-6]$', rank):
                        continue
                    waku_td = tds[1].get_text(strip=True)
                    time_td = tds[3].get_text(strip=True)
                    result_lookup[waku_td] = {
                        "rank":        rank,
                        "result_time": time_td,
                    }

            # table[1]: STテーブル "1 .12" → course=1, st=.12
            if len(is_tables) >= 2:
                for row in is_tables[1].find_all("tr"):
                    tds = row.find_all("td")
                    if len(tds) == 1:
                        cell = tds[0].get_text(strip=True)
                        m = re.match(r'^(\d)\s*\.([\d]+)', cell)
                        if m:
                            st_by_course[m.group(1)] = "." + m.group(2)

            # 払戻金（table[2+]）
            payout_parts = []
            for table in is_tables[2:]:
                t_text = table.get_text(" ", strip=True)
                if any(k in t_text for k in ("3連単", "3連複", "2連単")):
                    payout_parts.append(t_text[:300])
            payout_text = " | ".join(payout_parts)[:400]

            # 天候
            weather_div = soup_result.find("div", class_=re.compile(r"weather"))
            if weather_div:
                weather_info["weather_text"] = weather_div.get_text(" ", strip=True)[:100]

        except Exception:
            pass

    # --- 統合 ---
    # STはコース番号→枠番の対応が取れないため、コース順(1〜6)で枠番に割り当てる
    # ※ボートレースは枠番≠コース番号の場合があるが、暫定的にコース順を使用
    rows = []
    for waku in ["1", "2", "3", "4", "5", "6"]:
        player = players.get(waku, {"waku": waku})
        result = result_lookup.get(waku, {})
        st = st_by_course.get(waku, "")  # コース番号=枠番として暫定割り当て

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
            "st":          st,
            # 払戻・天候
            "payout":      payout_text,
            "weather":     weather_info.get("weather_text", ""),
        }
        rows.append(row)

    return rows

# ========== メイン処理 ==========

def scrape_month(year, month, resume=False, half_mode=True):
    """
    half_mode=True: 全レースの半分取得したらいったん返す
    """
    print(f"\n{'='*55}")
    print(f"🚤 競艇データ収集 v1.0: {year}年{month}月")
    print(f"{'='*55}")

    done_items, all_rows, all_races_cached = [], [], None
    if resume:
        done_items, all_rows, all_races_cached = load_checkpoint(year, month)

    if all_races_cached:
        # チェックポイントからレースリストを復元（日程スキャンをスキップ）
        all_races = all_races_cached
        print(f"  ✅ 日程スキャンをスキップ（キャッシュ済み: {len(all_races)}件）")
    else:
        # 開催日程取得
        targets = get_race_dates_for_month(year, month)
        if not targets:
            print("⚠️  開催が見つかりませんでした")
            return all_rows, False

        # 各開催のレース番号を展開
        all_races = []
        print(f"\n🔍 レース番号取得中...")
        for jcd, hd in tqdm(targets, desc="開催スキャン"):
            race_nos = parse_result_list(jcd, hd)
            for rno in race_nos:
                key = (jcd, hd, str(rno))
                all_races.append(key)
            human_wait()

        # レースリストをチェックポイントに保存（次回スキャンをスキップするため）
        save_checkpoint(year, month, done_items, all_rows, all_races)
        print(f"  💾 レースリストをチェックポイントに保存（{len(all_races)}件）")

    done_set = set(done_items)
    total = len(all_races)
    remaining = [r for r in all_races if r not in done_set]
    print(f"  未処理: {len(remaining)}件 / 全{total}件")

    # 半分モード: 今回の取得上限
    if half_mode:
        half_point = max(total // 2, 1)
        already_done = total - len(remaining)
        limit = max(half_point - already_done, 0)
        if limit <= 0:
            limit = len(remaining)
        print(f"  今回の取得上限: {limit}件（半分モード）")
    else:
        limit = len(remaining)

    # 各レース取得
    batch_count = 0
    for i, (jcd, hd, rno) in enumerate(tqdm(remaining[:limit], desc="レース取得")):
        try:
            rows = parse_race(jcd, hd, int(rno))
            all_rows.extend(rows)
            done_items.append((jcd, hd, rno))

            if (i + 1) % BATCH_SIZE == 0:
                batch_count += 1
                save_checkpoint(year, month, done_items, all_rows, all_races)
                print(f"  📊 進捗: 累計行数={len(all_rows)}")
                batch_rest(batch_count)

        except KeyboardInterrupt:
            print("\n\n⚡ 中断 → チェックポイント保存中...")
            save_checkpoint(year, month, done_items, all_rows, all_races)
            print("  → 次回: --resume で再開")
            raise
        except Exception as e:
            print(f"  ⚠️  スキップ {jcd}/{hd}/{rno}: {e}")
            continue

    save_checkpoint(year, month, done_items, all_rows, all_races)
    print(f"\n📊 集計: 累計行数={len(all_rows)}")

    is_complete = len(done_items) >= total
    print(f"  完了: {is_complete} (残り{total - len(done_items)}件)")
    return all_rows, is_complete

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
        rows, is_complete = scrape_month(args.year, month, args.resume)
        print(f"\n📋 scrape_month 戻り値: {len(rows)}行 / 完了={is_complete}")
        save_month_csv(args.year, month, rows)
        if rows:
            all_total.extend(rows)
        if not is_complete:
            print(f"\n⏸️  半分取得完了。次回resumeで残りを取得します。")
            import sys; sys.exit(2)

    if len(months) > 1 and all_total:
        fp = OUTPUT_DIR / f"{args.year}_all_kyotei.csv"
        pd.DataFrame(all_total).to_csv(fp, index=False, encoding="utf-8-sig")
        print(f"\n✅ 年間統合: {fp} ({len(all_total)}行)")

    print("\n🎉 完了！")

if __name__ == "__main__":
    main()

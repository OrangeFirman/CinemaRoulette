# -*- coding: utf-8 -*-
import json
import random
import urllib.request
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import datetime
import threading
import os
import json as _json_mod
import concurrent.futures
import re as _re

# --- Config & Caching ---
_counter_lock = threading.Lock()
_counter_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'api_counter.json')
_key_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'api_key.json')

_config_cache = None
_key_cache = None

def _load_config():
    global _config_cache
    if _config_cache is None:
        try:
            with open(_counter_file) as f:
                _config_cache = _json_mod.load(f)
        except Exception:
            _config_cache = {}
    return _config_cache

def _save_config(data):
    global _config_cache
    _config_cache = data
    try:
        with open(_counter_file, 'w') as f:
            _json_mod.dump(data, f)
    except Exception:
        pass

def _load_counter():
    cfg = _load_config()
    if cfg.get('date') == str(datetime.date.today()):
        return cfg.get('count', 0)
    return 0

def _save_counter(count):
    cfg = _load_config()
    cfg['date'] = str(datetime.date.today())
    cfg['count'] = count
    _save_config(cfg)

def get_api_key():
    global _key_cache
    if _key_cache is None:
        try:
            with open(_key_file) as f:
                _key_cache = _json_mod.load(f).get('api_key', '')
        except Exception:
            _key_cache = ''
    return _key_cache

def set_api_key(key):
    global _key_cache
    with _counter_lock:
        _key_cache = key.strip()
        try:
            with open(_key_file, 'w') as f:
                _json_mod.dump({'api_key': _key_cache}, f)
        except Exception:
            pass

_api_count = _load_counter()

# --- TMDb ---
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG  = "https://image.tmdb.org/t/p/w500"

def get_tmdb_key():
    global _key_cache
    # _key_cache holds KP key; load tmdb separately from api_key.json
    try:
        with open(_key_file) as f:
            return _json_mod.load(f).get("tmdb_key", "")
    except Exception:
        return ""

def set_tmdb_key(key):
    with _counter_lock:
        try:
            try:
                with open(_key_file) as f:
                    data = _json_mod.load(f)
            except Exception:
                data = {}
            data["tmdb_key"] = key.strip()
            with open(_key_file, 'w') as f:
                _json_mod.dump(data, f)
        except Exception:
            pass

def tmdb_request(path):
    key = get_tmdb_key()
    if not key:
        return None
    sep = "&" if "?" in path else "?"
    url = f"{TMDB_BASE}{path}{sep}api_key={key}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"TMDb error: {e}")
        return None

def tmdb_poster_for_film(title_en, title_ru, year):
    """Search TMDb by EN title then RU fallback, return full poster URL or None."""
    for query in filter(None, [title_en, title_ru]):
        params = urllib.parse.urlencode({"query": query, "year": year or ""})
        data = tmdb_request(f"/search/movie?{params}")
        if not data:
            continue
        results = data.get("results", [])
        if not results:
            continue
        path = results[0].get("poster_path")
        if path:
            return TMDB_IMG + path
    return None

def increment_counter():
    global _api_count
    with _counter_lock:
        _api_count = _load_counter()
        _api_count += 1
        _save_counter(_api_count)
        return _api_count

def get_counter():
    with _counter_lock:
        return _load_counter()

def get_reset_time():
    now = datetime.datetime.now()
    midnight = (now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    secs = int((midnight - now).total_seconds())
    h, m = divmod(secs // 60, 60)
    return f"{h}h {m}m"

API_BASE = "https://kinopoiskapiunofficial.tech/api"

def kp_request(path):
    key = get_api_key()
    if not key:
        return None
    increment_counter()
    url = f"{API_BASE}{path}"
    req = urllib.request.Request(url, headers={
        "X-API-KEY": key,
        "Content-Type": "application/json"
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"API error: {e}")
        return None

def get_random_film():
    for max_page in [50, 20, 10, 5, 1]:
        page = random.randint(1, max_page)
        order = random.choice(["RATING", "NUM_VOTE", "YEAR"])
        data = kp_request(f"/v2.2/films?order={order}&type=FILM&page={page}&ratingFrom=1")
        if data and "items" in data and data["items"]:
            return random.choice(data["items"])
    return None

SEQUEL_PATTERNS = [
    r'\b[2-9]\b', r'\bII\b', r'\bIII\b', r'\bIV\b', r'\bVI?I?I?\b',
    r'[Pp]art [2-9]', r'[Чч]асть [2-9]', r'[Сс]езон [2-9]',
    r'[Rr]eturns', r'[Rr]eloaded', r'[Rr]evolutions', r'[Rr]esurrection',
    r'[Aa]gain', r'[Ff]orever', r'[Ss]trikes [Bb]ack',
]
_sequel_re = _re.compile('|'.join(SEQUEL_PATTERNS))

def looks_like_sequel(title):
    return bool(_sequel_re.search(title or ''))

def random_year_window(year_min, year_max):
    span = year_max - year_min
    if span < 5:
        return year_min, year_max
    window = random.randint(3, min(8, span))
    y_start = random.randint(year_min, year_max - window)
    return y_start, y_start + window

def get_films_batch(count=3, rating_min=1, rating_max=10, year_min=1900, year_max=2026, exclude_countries=None, exclude_sequels=False, exclude_genres=None):
    if exclude_countries is None: exclude_countries = []
    if exclude_genres is None: exclude_genres = []
    excl_genres_lc = [g.lower() for g in exclude_genres]
    films = []
    seen_ids = set()
    lock = threading.Lock()

    def fetch_candidate():
        y_start, y_end = random_year_window(year_min, year_max)
        page = random.randint(1, 15)
        data = kp_request(f"/v2.2/films?order=YEAR&type=FILM&page={page}&ratingFrom={rating_min}&ratingTo={rating_max}&yearFrom={y_start}&yearTo={y_end}")
        
        if not data or "items" not in data or not data["items"]:
            return
            
        candidates = data["items"]
        random.shuffle(candidates)
        
        for f in candidates:
            fid = f.get("kinopoiskId")
            with lock:
                if not fid or fid in seen_ids or len(films) >= count:
                    continue
                    
            countries = [c.get("country", "") for c in f.get("countries", [])]
            if any(ex.lower() in c.lower() for ex in exclude_countries for c in countries):
                continue
                
            if exclude_sequels:
                name_ru = f.get("nameRu") or ""
                name_en = f.get("nameEn") or ""
                if looks_like_sequel(name_ru) or looks_like_sequel(name_en):
                    continue
                    
            if excl_genres_lc:
                film_genres = [g.get("genre", "").lower() for g in f.get("genres", [])]
                if any(eg in fg for eg in excl_genres_lc for fg in film_genres):
                    continue
                    
            with lock:
                if fid not in seen_ids and len(films) < count:
                    seen_ids.add(fid)
                    films.append(f)
                    break 

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_candidate) for _ in range(15)]
        for future in concurrent.futures.as_completed(futures):
            with lock:
                if len(films) >= count:
                    break

    return films[:count]

def get_roulette_films():
    good = []
    bad = []
    seen_ids = set()
    lock = threading.Lock()

    def fetch_good():
        y_start, y_end = random_year_window(1940, 2023)
        page = random.randint(1, 10)
        data = kp_request(f"/v2.2/films?order=YEAR&type=FILM&page={page}&ratingFrom=7&ratingTo=10&yearFrom={y_start}&yearTo={y_end}")
        if data and "items" in data and data["items"]:
            candidates = data["items"]
            random.shuffle(candidates)
            for f in candidates:
                fid = f.get("kinopoiskId")
                with lock:
                    if fid and fid not in seen_ids:
                        seen_ids.add(fid)
                        good.append(f)
                        break

    def fetch_bad():
        y_start, y_end = random_year_window(1950, 2020)
        page = random.randint(1, 10)
        data = kp_request(f"/v2.2/films?order=YEAR&type=FILM&page={page}&ratingFrom=1&ratingTo=5&yearFrom={y_start}&yearTo={y_end}")
        if data and "items" in data and data["items"]:
            candidates = data["items"]
            random.shuffle(candidates)
            for f in candidates:
                fid = f.get("kinopoiskId")
                with lock:
                    if fid and fid not in seen_ids:
                        seen_ids.add(fid)
                        bad.append(f)
                        break

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_good) for _ in range(12)]
        for future in concurrent.futures.as_completed(futures):
            with lock:
                if len(good) >= 5:
                    break

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_bad) for _ in range(5)]
        for future in concurrent.futures.as_completed(futures):
            with lock:
                if len(bad) >= 1:
                    break

    pool = good[:5]
    if bad:
        pool.insert(random.randint(0, len(pool)), bad[0])
    return pool

def format_film(f):
    name_ru = f.get("nameRu") or ""
    name_en = f.get("nameEn") or ""
    name_orig = f.get("nameOriginal") or ""
    title_en = name_en or name_orig or name_ru or "Unknown"
    title_ru = name_ru or name_orig or name_en or "Unknown"
    raw_poster = f.get("posterUrlPreview") or f.get("posterUrl")
    # Proxy through local server to avoid CDN/CORS/geo issues
    if raw_poster:
        poster = "/api/poster?url=" + urllib.parse.quote(raw_poster, safe="")
    else:
        poster = None
    return {
        "id": f.get("kinopoiskId"),
        "title_ru": title_ru,
        "title_en": title_en,
        "year": f.get("year"),
        "poster": poster,
        "poster_raw": raw_poster,  # kept for TMDb fallback lookup
        "rating": f.get("ratingKinopoisk") or f.get("ratingImdb"),
        "genres": [g["genre"] for g in f.get("genres", [])][:2],
    }

def search_film_by_title(title, year=None):
    keyword = urllib.parse.quote(title)
    data = kp_request(f"/v2.1/films/search-by-keyword?keyword={keyword}&page=1")
    if not data or "films" not in data or not data["films"]:
        return None
    films = data["films"]
    if year:
        def year_diff(f):
            try: return abs(int(f.get("year", 0)) - int(year))
            except: return 9999
        films = sorted(films, key=year_diff)
    best = films[0]
    return {
        "kinopoiskId": best.get("filmId") or best.get("kinopoiskId"),
        "nameRu":      best.get("nameRu"),
        "nameEn":      best.get("nameEn"),
        "nameOriginal":best.get("nameEn"),
        "year":        best.get("year"),
        "posterUrlPreview": best.get("posterUrlPreview"),
        "posterUrl":   best.get("posterUrl"),
        "ratingKinopoisk": best.get("rating"),
        "genres":      best.get("genres", []),
        "countries":   best.get("countries", []),
    }

def get_watchlist_draft_from_titles(titles, count=3, exclude_genres=None, exclude_sequels=False):
    if exclude_genres is None: exclude_genres = []
    excl_genres_lc = [g.lower() for g in exclude_genres]
    random.shuffle(titles)
    results = []
    seen_ids = set()
    for item in titles:
        if len(results) >= count:
            break
        film = search_film_by_title(item.get("title", ""), item.get("year"))
        if not film or not film.get("kinopoiskId"):
            continue
        fid = film["kinopoiskId"]
        if fid in seen_ids:
            continue
        if excl_genres_lc:
            film_genres = [g.get("genre", "").lower() for g in film.get("genres", [])]
            if any(eg in fg for eg in excl_genres_lc for fg in film_genres):
                continue
        if exclude_sequels:
            if looks_like_sequel(film.get("nameRu") or "") or looks_like_sequel(film.get("nameEn") or ""):
                continue
        seen_ids.add(fid)
        results.append(film)
    if not results:
        return None, "No matching films found after filters"
    return results, None

class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"[{self.address_string()}] {format % args}")

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(body)
            self.wfile.flush()
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass

    def send_error_json(self, msg, status=500):
        self.send_json({"error": msg}, status)

    def do_POST(self):
        path = self.path.split("?")[0]
        content_len = int(self.headers.get('Content-Length', 0))
        body_raw = self.rfile.read(content_len) if content_len else b''
        try:
            payload = json.loads(body_raw)
        except Exception:
            return self.send_error_json("Invalid JSON", 400)

        if path == "/api/key":
            key = payload.get('key', '').strip()
            if not key:
                return self.send_error_json("No key provided", 400)
            set_api_key(key)
            self.send_json({"ok": True})

        elif path == "/api/tmdb-key":
            key = payload.get('key', '').strip()
            if not key:
                return self.send_error_json("No key provided", 400)
            set_tmdb_key(key)
            self.send_json({"ok": True})

        elif path == "/api/tmdb-poster":
            # Frontend calls this when proxy fails, to get a TMDb poster URL
            title_en = payload.get('title_en', '')
            title_ru = payload.get('title_ru', '')
            year = payload.get('year')
            url = tmdb_poster_for_film(title_en, title_ru, year)
            if url:
                self.send_json({"url": url})
            else:
                self.send_error_json("Not found", 404)

        elif path == "/api/watchlist":
            if not get_api_key():
                return self.send_error_json("No API key configured", 403)
            titles = payload.get('titles', [])
            excl_genres = payload.get('excludeGenres', [])
            exclude_sequels = payload.get('excludeSequels', False)
            if not titles:
                return self.send_error_json("No titles", 400)
            films, err = get_watchlist_draft_from_titles(titles, 3, excl_genres, exclude_sequels)
            if err:
                return self.send_error_json(err)
            self.send_json([format_film(f) for f in films])

        else:
            self.send_error_json("Not found", 404)

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/api/mystery":
            film = get_random_film()
            if not film:
                return self.send_error_json("Could not fetch film")
            self.send_json(format_film(film))

        elif path == "/api/draft":
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            def qf(k, default, cast): return cast(qs[k][0]) if k in qs else default
            rating_min = qf('ratingMin', 1, float)
            rating_max = qf('ratingMax', 10, float)
            year_min = qf('yearMin', 1900, int)
            year_max = qf('yearMax', 2026, int)
            excl = [c.strip() for c in qs.get('excludeCountries', [''])[0].split(',') if c.strip()]
            excl_genres = [g.strip() for g in qs.get('excludeGenres', [''])[0].split(',') if g.strip()]
            exclude_sequels = qs.get('excludeSequels', ['0'])[0] == '1'
            films = get_films_batch(3, rating_min, rating_max, year_min, year_max, excl, exclude_sequels, excl_genres)
            if len(films) < 1:
                return self.send_error_json("Could not fetch films")
            self.send_json([format_film(f) for f in films[:3]])

        elif path == "/api/roulette":
            films = get_roulette_films()
            if len(films) < 6:
                return self.send_error_json("Could not fetch roulette films")
            self.send_json([format_film(f) for f in films])

        elif path == "/":
            try:
                html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'index.html')
                with open(html_path, 'rb') as f:
                    content = f.read()
                
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(content)
                self.wfile.flush()
            except FileNotFoundError:
                self.send_error(404, "index.html not found.")
            except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
                pass

        elif path == "/api/poster":
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(self.path).query)
            img_url = qs.get("url", [""])[0]
            if not img_url:
                return self.send_error_json("Missing url param", 400)
            try:
                req = urllib.request.Request(img_url, headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://www.kinopoisk.ru/"
                })
                with urllib.request.urlopen(req, timeout=8) as resp:
                    data = resp.read()
                    ct = resp.headers.get("Content-Type", "image/jpeg")
                self.send_response(200)
                self.send_header("Content-Type", ct)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "public, max-age=86400")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(data)
                self.wfile.flush()
            except Exception as e:
                # Return 404 so frontend onerror fires and tries TMDb
                self.send_error_json(f"Poster fetch failed: {e}", 404)



        elif path == "/api/stats":
            self.send_json({
                "used": get_counter(),
                "limit": 500,
                "resets_in": get_reset_time(),
                "has_key": bool(get_api_key()),
                "has_tmdb": bool(get_tmdb_key())
            })

        elif path == "/api/key":
            self.send_json({"has_key": bool(get_api_key()), "has_tmdb": bool(get_tmdb_key())})

        else:
            self.send_error_json("Not found", 404)

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def handle_error(self, request, client_address):
        import sys
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionAbortedError, ConnectionResetError, BrokenPipeError)):
            pass
        else:
            super().handle_error(request, client_address)

if __name__ == "__main__":
    port = 7777
    print(f"CinemaRoulette running at http://localhost:{port}")
    print("Press Ctrl+C to stop.")
    ThreadedHTTPServer(("localhost", port), Handler).serve_forever()
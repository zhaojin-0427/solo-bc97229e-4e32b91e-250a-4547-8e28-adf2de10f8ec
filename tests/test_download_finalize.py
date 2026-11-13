"""回归测试：下载收尾（释放引用 + 回收旧文件）在目录库写锁下的行为。

覆盖：
- 断开下载后，目录库被另一连接持写锁（可超过 busy_timeout）时，释放任务
  转入受限后台线程池退避重试，不占满服务请求线程；
- 持锁期间 /healthz、/api/status、状态页仍可响应；
- 解锁后无需重启或再次下载，自动补完释放并回收无读者的旧文件；
  收尾再次遇锁也不丢失；
- 多个下载共享文件时各自只释放自身引用，仍在传输的下载读完整快照，
  文件不被提前删除；
- 后台执行线程数量有上限；
- 重启回收保持不变。
"""

import csv
import http.client
import importlib
import io
import json
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest
from werkzeug.serving import make_server as ws_make_server

APP_DIR = str(Path(__file__).resolve().parent.parent / "app")
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)


# ---------------------------------------------------------------- 基础设施


def wait_for(cond, desc="", timeout=20.0, interval=0.1):
    deadline = time.monotonic() + timeout
    while True:
        if cond():
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"超时等待：{desc}")
        time.sleep(interval)


class Server:
    def __init__(self, mod, httpd, data_dir):
        self.mod = mod
        self.httpd = httpd
        self.data_dir = data_dir
        self.db_path = data_dir / "catalog.db"
        self.parquet_dir = data_dir / "parquet"
        self.port = httpd.server_address[1]

    def stop(self):
        try:
            self.httpd.shutdown()
            self.httpd.server_close()
        except Exception:
            pass


@pytest.fixture()
def make_server(tmp_path, monkeypatch):
    """每个服务实例使用独立数据目录，按需重载 app 模块以应用环境变量。"""
    servers = []
    counter = iter(range(1000))

    def _make(**env):
        data_dir = Path(env.pop("DATA_DIR", tmp_path / f"data{next(counter)}"))
        monkeypatch.setenv("DATA_DIR", str(data_dir))
        monkeypatch.setenv("BATCH_ROWS", env.pop("BATCH_ROWS", "50"))
        monkeypatch.setenv("DB_BUSY_TIMEOUT_MS", env.pop("DB_BUSY_TIMEOUT_MS", "300"))
        monkeypatch.setenv("RELEASE_WORKERS", env.pop("RELEASE_WORKERS", "2"))
        for key in ("DOWNLOAD_BATCH_DELAY_MS", "MERGE_BATCH_DELAY_MS"):
            if key in env:
                monkeypatch.setenv(key, env.pop(key))
            else:
                monkeypatch.delenv(key, raising=False)
        assert not env, f"未知环境变量: {env}"
        sys.modules.pop("app", None)
        mod = importlib.import_module("app")
        httpd = ws_make_server("127.0.0.1", 0, mod.app, threaded=True)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        srv = Server(mod, httpd, data_dir)
        servers.append(srv)
        return srv

    yield _make
    for srv in servers:
        srv.stop()


class HeldWriteLock:
    """测试专用：另一 SQLite 连接长时间持有目录库写锁（BEGIN IMMEDIATE）。"""

    def __init__(self, db_path):
        self.con = sqlite3.connect(str(db_path), isolation_level=None)
        self.con.execute("PRAGMA busy_timeout=100")
        self.con.execute("BEGIN IMMEDIATE")

    def release(self):
        try:
            self.con.execute("ROLLBACK")
        finally:
            self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.release()


# ---------------------------------------------------------------- HTTP / DB 助手


def http_req(port, method, path, body=None, headers=None, timeout=10):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    conn.request(method, path, body=body, headers=headers or {})
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def status_json(port, timeout=10):
    status, data = http_req(port, "GET", "/api/status", timeout=timeout)
    assert status == 200
    return json.loads(data)


def release_backlog(port):
    rel = status_json(port)["release"]
    return rel["pending"] + rel["active"]


def make_rows(n, site, base=0):
    return [
        (f"{site}-{i % 3}", str(1_760_000_000 + base + i), f"{i}.5")
        for i in range(n)
    ]


def upload_csv(port, name, rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(["site", "ts", "reading"])
    w.writerows(rows)
    boundary = "----pytest-boundary-7d1a"
    body = b"\r\n".join(
        [
            f"--{boundary}".encode(),
            f'Content-Disposition: form-data; name="file"; filename="{name}"'.encode(),
            b"Content-Type: text/csv",
            b"",
            buf.getvalue().encode(),
            f"--{boundary}--".encode(),
            b"",
        ]
    )
    status, _ = http_req(
        port, "POST", "/upload", body,
        {"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    assert status == 302


def start_merge(port, ids):
    body = "&".join(f"file_ids={i}" for i in ids).encode()
    status, _ = http_req(
        port, "POST", "/merge", body,
        {"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert status == 302


def wait_merge_done(port, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        m = status_json(port).get("merge")
        if m:
            assert m["state"] != "failed", f"合并失败: {m['error']}"
            if m["state"] == "done":
                return m
        time.sleep(0.1)
    raise AssertionError("超时等待：合并完成")


def open_download(port):
    """开始一个下载并确认流式响应已开始（此时引用已登记）。"""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    conn.request("GET", "/download")
    resp = conn.getresponse()
    assert resp.status == 200
    first = resp.read1(64)
    assert first.startswith(b"site,ts,reading")
    return conn, resp


def close_download(conn, resp):
    """客户端断开：werkzeug 以 Connection: close 流式响应，套接字归响应
    对象所有，必须关闭响应才能真正断开（服务端随后在下一次写入时感知）。"""
    try:
        resp.close()
    finally:
        conn.close()


def db_query(db_path, sql, args=()):
    con = sqlite3.connect(str(db_path))
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()


def refs_total(db_path):
    return db_query(db_path, "SELECT COALESCE(SUM(refs),0) FROM file_refs")[0][0]


def retired_paths(db_path):
    return [r[0] for r in db_query(db_path, "SELECT path FROM files WHERE state='retired'")]


def active_files(db_path):
    return db_query(
        db_path, "SELECT id, path, rows FROM files WHERE state='active' ORDER BY id"
    )


def all_gone(parquet_dir, paths):
    return all(not (parquet_dir / p).exists() for p in paths)


# ---------------------------------------------------------------- 回归测试


def test_disconnect_release_retries_until_unlock(make_server):
    """断连后遇长写锁：后台重试不占请求线程，解锁后自动补完释放与回收。"""
    srv = make_server(DOWNLOAD_BATCH_DELAY_MS="100")
    upload_csv(srv.port, "a.csv", make_rows(800, "a"))
    upload_csv(srv.port, "b.csv", make_rows(800, "b", base=1000))

    conn, resp = open_download(srv.port)
    wait_for(lambda: refs_total(srv.db_path) == 2, "下载引用已登记")

    ids = [f[0] for f in active_files(srv.db_path)]
    start_merge(srv.port, ids)
    wait_merge_done(srv.port)
    retired = retired_paths(srv.db_path)
    assert len(retired) == 2
    for p in retired:  # 下载仍在读，退役文件必须保留
        assert (srv.parquet_dir / p).exists()

    lock = HeldWriteLock(srv.db_path)  # 另一连接持写锁（远超 busy_timeout=300ms）
    try:
        close_download(conn, resp)  # 客户端断开
        wait_for(lambda: release_backlog(srv.port) >= 1, "释放任务进入后台队列")
        time.sleep(1.0)  # 覆盖多次遇锁重试
        # 写锁未开：引用不丢失、文件不提前删
        assert refs_total(srv.db_path) == 2
        for p in retired:
            assert (srv.parquet_dir / p).exists()
        # 持锁期间健康检查、状态接口、状态页仍可响应
        t0 = time.monotonic()
        status, body = http_req(srv.port, "GET", "/healthz", timeout=5)
        assert status == 200 and body == b"ok"
        rel = status_json(srv.port, timeout=5)["release"]
        assert rel["workers"] == 2
        assert rel["active"] <= rel["workers"]  # 后台线程有上限
        status, _ = http_req(srv.port, "GET", "/", timeout=5)
        assert status == 200
        assert time.monotonic() - t0 < 5
    finally:
        lock.release()

    # 解锁后无需重启/再次下载：自动补完释放并回收无读者的旧文件
    wait_for(lambda: refs_total(srv.db_path) == 0, "解锁后引用释放")
    wait_for(lambda: all_gone(srv.parquet_dir, retired), "退役文件被回收")
    wait_for(lambda: release_backlog(srv.port) == 0, "后台释放队列排空")


def test_concurrent_disconnects_release_only_own_refs(make_server):
    """并发断连：每次结束只释放自身引用；全部结束后才回收共享文件。"""
    srv = make_server(DOWNLOAD_BATCH_DELAY_MS="100")
    upload_csv(srv.port, "a.csv", make_rows(1000, "a"))
    upload_csv(srv.port, "b.csv", make_rows(1000, "b", base=1000))

    conns = [open_download(srv.port) for _ in range(3)]
    wait_for(lambda: refs_total(srv.db_path) == 6, "三个下载的引用已登记")

    ids = [f[0] for f in active_files(srv.db_path)]
    start_merge(srv.port, ids)
    wait_merge_done(srv.port)
    retired = retired_paths(srv.db_path)
    assert len(retired) == 2

    # 第一个断开（无锁）：只释放自己 -> 6 减为 4，文件仍在
    close_download(*conns[0])
    wait_for(lambda: refs_total(srv.db_path) == 4, "第一个断连只释放自身引用")
    for p in retired:
        assert (srv.parquet_dir / p).exists()

    # 持锁后再断开两个：收尾排队重试，引用保持，线程数不超上限
    lock = HeldWriteLock(srv.db_path)
    try:
        close_download(*conns[1])
        close_download(*conns[2])
        wait_for(lambda: release_backlog(srv.port) >= 2, "两个释放任务进入后台")
        time.sleep(1.0)
        assert refs_total(srv.db_path) == 4
        rel = status_json(srv.port)["release"]
        assert rel["active"] <= 2
        for p in retired:
            assert (srv.parquet_dir / p).exists()
    finally:
        lock.release()

    # 解锁后全部补完：引用清零，共享旧文件才被回收
    wait_for(lambda: refs_total(srv.db_path) == 0, "解锁后全部释放")
    wait_for(lambda: all_gone(srv.parquet_dir, retired), "无读者旧文件被回收")


def test_slow_download_reads_fixed_snapshot(make_server):
    """存活慢下载：合并/新上传不影响其固定快照，文件不提前删，内容完整。"""
    srv = make_server(DOWNLOAD_BATCH_DELAY_MS="50")
    data_a = make_rows(300, "a")
    data_b = make_rows(300, "b", base=1000)
    upload_csv(srv.port, "a.csv", data_a)
    upload_csv(srv.port, "b.csv", data_b)
    ids = [f[0] for f in active_files(srv.db_path)]

    conn = http.client.HTTPConnection("127.0.0.1", srv.port, timeout=30)
    conn.request("GET", "/download")
    resp = conn.getresponse()
    assert resp.status == 200

    # 下载进行中合并输入文件：退役文件被该下载引用，不能提前删除
    start_merge(srv.port, ids)
    wait_merge_done(srv.port)
    retired = retired_paths(srv.db_path)
    assert len(retired) == 2

    # 下载期间再上传新文件：不属于本次快照
    upload_csv(srv.port, "c.csv", make_rows(50, "c", base=2000))

    first = resp.read1(128)  # 流仍在进行中
    assert first
    for p in retired:
        assert (srv.parquet_dir / p).exists()

    body = first
    while True:
        chunk = resp.read1(65536)
        if not chunk:
            break
        body += chunk
    conn.close()

    got = list(csv.reader(io.StringIO(body.decode())))
    assert got[0] == ["site", "ts", "reading"]
    # 快照只含受理时的 a+b（完整、不丢行、不含新上传的 c）
    assert sorted(got[1:]) == sorted([list(r) for r in data_a + data_b])

    # 正常结束后引用释放、旧文件回收；合并产物与新文件保持活动
    wait_for(lambda: refs_total(srv.db_path) == 0, "引用释放")
    wait_for(lambda: all_gone(srv.parquet_dir, retired), "旧文件回收")
    assert len(active_files(srv.db_path)) == 2


def test_health_and_status_responsive_while_catalog_locked(make_server):
    """目录库写锁被长时间持有时，健康检查与状态页仍可响应。"""
    srv = make_server()
    upload_csv(srv.port, "a.csv", make_rows(50, "a"))
    with HeldWriteLock(srv.db_path):
        for path in ("/healthz", "/api/status", "/"):
            t0 = time.monotonic()
            status, _ = http_req(srv.port, "GET", path, timeout=5)
            assert status == 200
            assert time.monotonic() - t0 < 5, f"{path} 在持锁期间未响应"


def test_restart_recovery_still_reaps(make_server):
    """重启回收保持：引用清零、退役文件回收、合并结果不丢。"""
    srv = make_server(DOWNLOAD_BATCH_DELAY_MS="100")
    upload_csv(srv.port, "a.csv", make_rows(1000, "a"))
    upload_csv(srv.port, "b.csv", make_rows(1000, "b", base=1000))
    # 下载在“进程崩溃”时仍在进行（保持连接与响应对象存活，引用未释放）
    conn, resp = open_download(srv.port)
    wait_for(lambda: refs_total(srv.db_path) == 2, "引用已登记")
    ids = [f[0] for f in active_files(srv.db_path)]
    start_merge(srv.port, ids)
    wait_merge_done(srv.port)
    retired = retired_paths(srv.db_path)
    assert len(retired) == 2

    srv.stop()  # 模拟进程停止（下载未结束，引用残留）

    srv2 = make_server(DATA_DIR=str(srv.data_dir))  # 同目录“重启”
    wait_for(lambda: refs_total(srv2.db_path) == 0, "重启后引用清零")
    wait_for(lambda: all_gone(srv2.parquet_dir, retired), "重启后退役文件回收")
    active = active_files(srv2.db_path)
    assert len(active) == 1 and active[0][2] == 2000  # 合并结果保留
    status, body = http_req(srv2.port, "GET", "/download")
    assert status == 200
    lines = [ln for ln in body.decode().split("\r\n") if ln]
    assert len(lines) == 1 + 2000
    close_download(conn, resp)


def test_upload_merge_download_roundtrip(make_server):
    """基本流程保持：上传 -> 合并 -> 下载内容一致，无读者旧文件被回收。"""
    srv = make_server()
    data_a = make_rows(120, "a")
    data_b = make_rows(130, "b", base=1000)
    upload_csv(srv.port, "a.csv", data_a)
    upload_csv(srv.port, "b.csv", data_b)
    ids = [f[0] for f in active_files(srv.db_path)]
    start_merge(srv.port, ids)
    wait_merge_done(srv.port)
    wait_for(
        lambda: len(list(srv.parquet_dir.glob("*.parquet"))) == 1,
        "无读者旧文件已回收",
    )
    status, body = http_req(srv.port, "GET", "/download")
    assert status == 200
    got = list(csv.reader(io.StringIO(body.decode())))
    assert got[0] == ["site", "ts", "reading"]
    assert sorted(got[1:]) == sorted([list(r) for r in data_a + data_b])
    wait_for(lambda: refs_total(srv.db_path) == 0, "下载结束后引用已释放")

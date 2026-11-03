"""JamsilClient 사용 예.  JAMSIL_HOSTS(쉼표 구분, 모든 노드), JAMSIL_USER, JAMSIL_PASSWORD 환경변수 사용.
    pip install "psycopg[binary]"
    JAMSIL_HOSTS=10.0.0.1,10.0.0.2,10.0.0.3 python quickstart.py
"""
import os
import sys

from jamsil_client import JamsilClient, new_request_key

hosts = (sys.argv[1] if len(sys.argv) > 1 else os.environ["JAMSIL_HOSTS"]).split(",")
with JamsilClient(hosts, os.environ["JAMSIL_USER"], os.environ["JAMSIL_PASSWORD"], application_name="quickstart-py") as db:
    # 스키마 (예제 전용 테이블)
    def ddl(c):
        c.execute("CREATE TABLE IF NOT EXISTS sample_orders (id BIGINT PRIMARY KEY, customer TEXT, amount BIGINT)")
        c.execute("CREATE TABLE IF NOT EXISTS sample_stock (sku TEXT PRIMARY KEY, qty BIGINT)")
    db.write(new_request_key("ddl"), ddl)

    # 1) 단일 문장 쓰기 — 키는 논리 작업당 1개
    n = db.execute(new_request_key("order-2001"),
                   "INSERT INTO sample_orders VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING", (2001, "kim", 5000))
    print("insert affected =", n)

    # 2) RETURNING 결과가 필요한 쓰기 — autocommit 단일 문장은 결과까지 재생된다
    amount = db.write(new_request_key("order-2001-bump"),
                      lambda c: c.execute("UPDATE sample_orders SET amount = amount + %s WHERE id = %s RETURNING amount",
                                          (100, 2001)).fetchone()[0])
    print("amount after bump =", amount)

    # 3) 여러 문장 트랜잭션 — 재시도 시 본문이 처음부터 다시 실행되므로 DB 밖의 부수 효과를 두지 않는다
    def place(c):
        c.execute("INSERT INTO sample_orders VALUES (%s, %s, %s)", (2002, "lee", 7000))
        c.execute("INSERT INTO sample_stock VALUES (%s, %s) ON CONFLICT (sku) DO UPDATE SET qty = sample_stock.qty - 1", ("SKU-1", 99))
    db.write(new_request_key("order-2002"), place, transaction=True)

    # 4) 읽기 — 끊기면 다른 노드로 자동 재연결
    for row in db.read(lambda c: c.execute("SELECT id, customer, amount FROM sample_orders ORDER BY id").fetchall()):
        print("order", *row)
    print("connected to", db.host)

    # 5) 정리
    def cleanup(c):
        c.execute("DROP TABLE IF EXISTS sample_orders")
        c.execute("DROP TABLE IF EXISTS sample_stock")
    db.write(new_request_key("cleanup"), cleanup)
    print("done (retries in last op:", db.retries, ")")

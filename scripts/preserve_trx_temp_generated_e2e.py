#!/usr/bin/env python3
"""Generated-expression and index assertions across the strict RESUME fixture."""

from preserve_trx_session_only_packet_e2e import SqlError


class TempStoredColumns:
    projection = "id,a,g,b,h,HEX(tag),HEX(norm)"

    def __init__(self, client, generated="STORED", indexed=True):
        self.indexed = indexed
        self.generated = generated
        self.tables = []
        for fmt in ("DYNAMIC", "REDUNDANT"):
            name = "tmp_" + generated.lower() + "_" + fmt.lower()
            keys = ",KEY k_g(g),KEY k_h(h),KEY k_norm(norm)" if indexed else ""
            extra = ""
            if generated == "VIRTUAL":
                extra = ",d DOUBLE GENERATED ALWAYS AS (a/8.0) VIRTUAL,c INT GENERATED ALWAYS AS (123) VIRTUAL"
                if indexed:
                    extra += (",wide VARCHAR(8000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin "
                              "GENERATED ALWAYS AS (CONCAT(REPEAT(tag,80),':',COALESCE(a,0))) VIRTUAL")
                    keys = ",UNIQUE KEY k_g(g),KEY k_h(h),KEY k_norm(norm),KEY k_wide(wide(20),b DESC),KEY k_d(d)"
            client.query(f"CREATE TEMPORARY TABLE {name}(id INT PRIMARY KEY,a INT,"
                         f"g INT GENERATED ALWAYS AS (a*3+1) {generated},b INT,"
                         f"h BIGINT GENERATED ALWAYS AS (g+b) {generated},"
                         "tag VARCHAR(40) CHARACTER SET utf8mb4,"
                         "norm VARCHAR(80) CHARACTER SET utf8mb4 GENERATED ALWAYS AS "
                         f"(CONCAT(LOWER(tag),':',COALESCE(a,0))) {generated}"
                         + extra + keys + ") ENGINE=InnoDB ROW_FORMAT=" + fmt)
            client.query(f"INSERT INTO {name}(id,a,b,tag) WITH RECURSIVE s(n) AS "
                         "(SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<64) "
                         "SELECT n,IF(n%7=0,NULL,n),n*10,CONCAT('AbC',n) FROM s")
            self.tables.append((name, self.rows(client, name)))
        self.at_savepoint = {}
        self.current = {}
        self.resumed = {}

    def rows(self, client, name):
        rows = client.query(f"SELECT {self.projection} FROM {name} ORDER BY id")
        for column, position in ((("g", 2), ("h", 4), ("norm", 6)) if self.indexed else ()):
            expected = [row for row in rows if row[position] is not None]
            assert client.query(f"SELECT {self.projection} FROM {name} FORCE INDEX(k_{column}) WHERE {column} IS NOT NULL ORDER BY id") == expected
            expected_null = [row for row in rows if row[position] is None]
            assert client.query(f"SELECT {self.projection} FROM {name} FORCE INDEX(k_{column}) WHERE {column} IS NULL ORDER BY id") == expected_null
        if self.indexed:
            # Covering reads expose stale virtual keys even when a clustered
            # lookup would recompute the correct projection from base columns.
            assert client.query(f"SELECT id,g FROM {name} FORCE INDEX(k_g) ORDER BY id") == [[r[0], r[2]] for r in rows]
            for key in (4, 28, 304, 334, 358, 3304, 3334, 3358):
                assert client.query(f"SELECT id,g FROM {name} FORCE INDEX(k_g) WHERE g={key} ORDER BY id") == [[r[0], r[2]] for r in rows if r[2] == str(key)]
        if self.generated == "VIRTUAL":
            assert client.query(f"SELECT COUNT(*) FROM {name} WHERE NOT(d <=> a/8.0) OR c<>123") == [["0"]]
            if self.indexed:
                actual = client.query(f"SELECT id,d FROM {name} FORCE INDEX(k_d) ORDER BY id")
                assert [[r[0], None if r[1] is None else float(r[1])] for r in actual] == [[r[0], None if r[1] is None else int(r[1])/8.0] for r in rows]
                for prefix in ("AbC1", "ChAnGeD1", "TaRgEt"):
                    expected = [[r[0]] for r in rows if r[5] is not None and bytes.fromhex(r[5]).decode().startswith(prefix)]
                    assert client.query(f"SELECT id FROM {name} FORCE INDEX(k_wide) WHERE wide LIKE '{prefix}%' ORDER BY id") == expected
        assert client.query(f"SELECT COUNT(*) FROM {name} WHERE "
                            "NOT(g <=> a*3+1) OR NOT(h <=> g+b) OR "
                            "NOT(norm <=> CONCAT(LOWER(tag),':',COALESCE(a,0)))") == [["0"]]
        return rows

    def before_drain(self, client):
        for name, _ in self.tables:
            client.query(f"UPDATE {name} SET a=a+100,b=b+1,tag=CONCAT('ChAnGeD',id) WHERE id<=32")
            client.query(f"DELETE FROM {name} WHERE id=64")
            client.query(f"INSERT INTO {name}(id,a,b,tag) VALUES(70,7,70,'Inserted')")
            self.at_savepoint[name] = self.rows(client, name)
        client.query("SAVEPOINT stored_before_tail")
        for name, _ in self.tables:
            # Keep the expected-success batch outside every existing UNIQUE
            # key range; deliberate duplicate errors are checked after resume.
            delta = 1000 if self.generated == "VIRTUAL" else 10
            client.query(f"UPDATE {name} SET a=a+{delta},b=b+20 WHERE id<=16")
            self.current[name] = self.rows(client, name)

    def after_resume(self, client):
        for name, _ in self.tables:
            assert self.rows(client, name) == self.current[name]
        client.query("ROLLBACK TO SAVEPOINT stored_before_tail")
        for name, _ in self.tables:
            assert self.rows(client, name) == self.at_savepoint[name]
            client.query(f"INSERT INTO {name}(id,a,b,tag) VALUES(71,8,80,'Target')")
            client.query(f"UPDATE {name} SET a=9,b=90,tag='TaRgEt' WHERE id=71")
            assert client.query(f"SELECT g,h,norm FROM {name} WHERE id=71") == [["28", "118", "target:9"]]
            try:
                client.query(f"UPDATE {name} SET g=999 WHERE id=71")
                raise AssertionError("explicit generated-column assignment succeeded")
            except SqlError as error:
                assert error.args[0] == 3105, error
            if self.generated == "VIRTUAL" and self.indexed:
                try:
                    client.query(f"INSERT INTO {name}(id,a,b,tag) VALUES(72,9,900,'duplicate')")
                    raise AssertionError("duplicate virtual key accepted")
                except SqlError as error:
                    assert error.args[0] == 1062, error
                assert client.query(f"SELECT id FROM {name} WHERE id=72") == []
            client.query(f"UPDATE {name} SET a=NULL,b=b+10,tag=NULL WHERE id=1")
            self.resumed[name] = self.rows(client, name)
        client.query("RELEASE SAVEPOINT stored_before_tail")

    def after_commit(self, client):
        for name, _ in self.tables:
            assert self.rows(client, name) == self.resumed[name]
            client.query(f"DROP TEMPORARY TABLE {name}")

    def after_rollback(self, client):
        for name, baseline in self.tables:
            assert self.rows(client, name) == baseline
            value = 999 if self.generated == "VIRTUAL" and self.indexed else 9
            client.query(f"INSERT INTO {name}(id,a,b,tag) VALUES(72,{value},90,'After rollback')")
            assert client.query(f"SELECT g,h,norm FROM {name} WHERE id=72") == [[str(value*3+1), str(value*3+91), f"after rollback:{value}"]]
            client.query(f"DROP TEMPORARY TABLE {name}")

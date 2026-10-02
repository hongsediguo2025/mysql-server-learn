#!/usr/bin/env python3
"""Column-shape data assertions for the real DRAIN/receiver/RESUME fixture."""


class TempColumnShapes:
    def __init__(self, profile, client):
        self.rowid_limit = profile == "rowid-limit"
        if self.rowid_limit:
            profile = "rowid"
        self.profile = profile
        if profile == "lob-old":
            client.query("SET SESSION debug='+d,lob_insert_noindex'")
        self.tables = []
        self.baseline_binary = {}
        self.current_binary = {}
        self.scan_index = "promoted" if profile == "unique" else None if profile == "rowid" else "PRIMARY"
        if profile == "strings":
            columns = [
                ("k CHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL", "LPAD(n,8,'0')"),
                ("a CHAR(24) CHARACTER SET latin1 COLLATE latin1_swedish_ci", "CONCAT('Ab',n,'  ')"),
                ("b CHAR(24) CHARACTER SET latin1 COLLATE latin1_bin", "CONCAT('bC',n)"),
                ("c CHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci", "REPEAT(CONVERT(0xF09F9982 USING utf8mb4),255)"),
                ("d BINARY(8)", "UNHEX('0001FF00')"),
            ]
            self.update = "a=CONCAT('changed',id),b='',c=REPEAT(CONVERT(0xE4B8AD USING utf8mb4),255),d=UNHEX('FF00')"
            self.projection = "id,HEX(k),HEX(a),HEX(b),HEX(c),HEX(d)"
            self.key_update = "k=CONCAT('changed',id)"
            self.index_predicate = "a IS NOT NULL"
            indexes = "KEY value_idx(a(8),b),KEY extra_idx(c(32))"
        elif profile == "numeric":
            columns = [
                ("k DECIMAL(30,10) NOT NULL", "n+0.0000000001"),
                ("a DECIMAL(65,30)", "IF(n%2=0,-1,1)*CAST('99999999999999999999999999999999999.123456789012345678901234567890' AS DECIMAL(65,30))"),
                ("b DECIMAL(65,30) UNSIGNED", "CAST('0.000000000000000000000000000001' AS DECIMAL(65,30))"),
                ("c FLOAT", "n*0.125"),
                ("d DOUBLE", "IF(n%2=0,-1e100,1e-100)"),
            ]
            self.update = "a=-a,b=b+0.000000000000000000000000000001,c=-c,d=d*2"
            self.projection = "id,k,a,b,c,d"
            self.key_update = "k=k+1000"
            self.index_predicate = "c IS NOT NULL"
            indexes = "KEY value_idx(c,d)"
        elif profile == "rowid":
            columns = [
                ("k INT NULL", "n%8"),
                ("a VARCHAR(32) CHARACTER SET utf8mb4 NOT NULL", "LPAD(n,8,'0')"),
                ("b INT NULL", "n*100000"),
            ]
            self.update = "k=k+1,b=b+100"
            self.projection = "id,k,a,b"
            self.key_update = "k=k+1000"
            self.index_predicate = "a IS NOT NULL"
            indexes = "KEY value_idx(a),UNIQUE KEY nullable(b)"
        elif profile == "json":
            columns = [
                ("k INT NOT NULL", "n"),
                ("a JSON", "JSON_OBJECT('a',REPEAT('a',35000),'z',REPEAT('z',60))"),
                ("b JSON", "IF(n%3=0,CAST('null' AS JSON),JSON_OBJECT('n',n))"),
                ("c JSON", "JSON_ARRAY(REPEAT('p',15644),REPEAT('a',40),REPEAT('t',20000))"),
            ]
            self.update = "a=JSON_SET(a,'$.z',IF(JSON_UNQUOTE(JSON_EXTRACT(a,'$.z')) LIKE 'z%',REPEAT('q',60),REPEAT('z',60))),b=JSON_SET(b,'$.n',100)"
            self.projection = "id,k,SHA2(CAST(a AS CHAR),256),JSON_STORAGE_SIZE(a),JSON_STORAGE_FREE(a),CAST(b AS CHAR),SHA2(CAST(c AS CHAR),256),JSON_STORAGE_SIZE(c),JSON_STORAGE_FREE(c)"
            self.key_update = "k=k+1000"
            self.index_predicate = "k>0"
            indexes = "KEY value_idx(k)"
        elif profile in ("lob", "lob-index", "lob-old", "varlob", "varlob-mixed"):
            binary_type = "VARBINARY(20000)" if profile.startswith("varlob") else "LONGBLOB"
            text_type = "VARCHAR(6000)" if profile.startswith("varlob") else "LONGTEXT"
            binary_repeats = 6000 if profile.startswith("varlob") else 22000
            text_repeats = 2500 if profile.startswith("varlob") else 6000
            columns = [
                ("k INT NOT NULL", "n"),
                ("a " + binary_type, f"CONCAT(UNHEX('00FF'),REPEAT(UNHEX('00FF81'),{binary_repeats}),LPAD(n,4,'0'))"),
                ("b " + text_type + " CHARACTER SET utf8mb4", f"REPEAT(CONVERT(0xE4B8ADF09F9982 USING utf8mb4),{text_repeats})"),
            ]
            if profile == "lob-index":
                # One real INDEX chain per layout without inflating every row.
                columns[1] = ("a LONGBLOB", "IF(n=1,REPEAT(UNHEX('00FF81'),180000)," + columns[1][1] + ")")
            self.update = "a=CONCAT(IF(LEFT(a,1)=UNHEX('00'),UNHEX('FF'),UNHEX('00')),SUBSTRING(a,2)),b=CONCAT(b,'x')"
            self.projection = "id,k,OCTET_LENGTH(a),SHA2(a,256),OCTET_LENGTH(b),SHA2(b,256)"
            self.key_update = "k=k+1000"
            self.index_predicate = "a IS NOT NULL"
            indexes = "KEY value_idx(a(16)),KEY extra_idx(b(16))"
        elif profile == "unique":
            columns = [
                ("k CHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL", "LPAD(n,8,'0')"),
                ("u INT NOT NULL", "n"),
                ("a INT NULL", "n*100000"),
                ("p VARCHAR(32) CHARACTER SET latin1 NOT NULL", "CONCAT(LPAD(n,8,'0'),'suffix')"),
            ]
            self.update = "a=a+100"
            self.projection = "id,HEX(k),u,a,p"
            self.key_update = "k=CONCAT('changed',id)"
            self.index_predicate = "a IS NOT NULL"
            indexes = "UNIQUE KEY promoted(k DESC,u),UNIQUE KEY another(u,k),UNIQUE KEY nullable(a),UNIQUE KEY prefix_only(p(8)),KEY value_idx(a)"
        elif profile == "bits":
            columns = [("k BIT(64) NOT NULL", "CAST(9223372036854775808 AS UNSIGNED)+n")]
            updates = []
            projected = ["id", "HEX(k)"]
            for width in (1, 7, 8, 9, 31, 32, 63, 64):
                name = "b" + str(width)
                maximum = (1 << width) - 1
                columns.append((f"{name} BIT({width})", f"IF(n%2=0,0,{maximum})"))
                updates.append(f"{name}=IF({name}+0=0,{maximum},0)")
                projected.extend((f"HEX({name})", name + "+0"))
            self.update = ",".join(updates)
            self.projection = ",".join(projected)
            self.key_update = "k=k+1000"
            self.index_predicate = "b9 IS NOT NULL"
            indexes = "KEY value_idx(b9,b64),KEY extra_idx(b1,b7,b8,b31,b32,b63)"
        else:
            assert profile == "temporal-enum"
            columns = [
                ("k DATE NOT NULL", "DATE_ADD('2000-02-01',INTERVAL n DAY)"),
                ("y YEAR", "2000+n"),
                ("dt DATE", "CASE n%3 WHEN 0 THEN '1000-01-01' WHEN 1 THEN '2000-02-29' ELSE '9999-12-31' END"),
                ("changed TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)", "'2024-01-01 00:00:00'"),
            ]
            updates = ["dt='2024-02-29'", "y=2099"]
            projected = ["id", "k", "y", "dt", "changed"]
            for precision in (0, 1, 3, 6):
                for prefix, typename, value in (
                    ("t", "TIME", "'-838:12:34.123456'"),
                    ("d", "DATETIME", "'1000-01-01 12:34:56.123456'"),
                    ("s", "TIMESTAMP", "'2024-02-29 12:34:56.123456'"),
                ):
                    name = prefix + str(precision)
                    columns.append((f"{name} {typename}({precision}) NULL", value))
                    projected.append(name)
                    updates.append(f"{name}=NULL")
            for width in (255, 256):
                name = "e" + str(width)
                elements = ",".join("'e" + str(n) + "'" for n in range(1, width+1))
                columns.append((f"{name} ENUM({elements})", f"'e{width}'"))
                projected.extend((name, name + "+0"))
                updates.append(name + "='e1'")
            for width in (8, 9, 32, 33, 64):
                name = "v" + str(width)
                elements = ",".join("'v" + str(n) + "'" for n in range(1, width+1))
                columns.append((f"{name} SET({elements})", f"'v1,v{width}'"))
                projected.extend((name, name + "+0"))
                updates.append(name + "=''")
            self.update = ",".join(updates)
            self.projection = ",".join(projected)
            self.key_update = "k=DATE_ADD(k,INTERVAL 100 YEAR)"
            self.index_predicate = "dt IS NOT NULL"
            indexes = "KEY value_idx(dt,t6),KEY extra_idx(e256,v64)"
            # The target connection starts in another zone. Session restoration
            # must precede comparing TIMESTAMP values.
            client.query("SET time_zone='+08:00'")
        definitions = ",".join(definition for definition, _ in columns)
        values = ",".join(value if "NOT NULL" in definition else f"IF(n%7=0,NULL,{value})"
                          for definition, value in columns)
        layouts = [(fmt, False) for fmt in ("DYNAMIC", "REDUNDANT")]
        if profile == "rowid":
            layouts += [(fmt, True) for fmt in ("DYNAMIC", "REDUNDANT")]
        for row_format, noindex in layouts:
            name = "tmp_shapes_" + row_format.lower() + ("_noindex" if noindex else "")
            keys = [] if profile in ("unique", "rowid") else ["PRIMARY KEY(k)"]
            if not noindex:
                keys.append(indexes)
            definition = ",".join(["id INT NOT NULL", definitions] + keys)
            if profile == "varlob-mixed" and row_format == "REDUNDANT":
                definition = definition.replace("a VARBINARY(20000)", "a LONGBLOB").replace("b VARCHAR(6000)", "b LONGTEXT")
            client.query(f"CREATE TEMPORARY TABLE {name}({definition}) ENGINE=InnoDB ROW_FORMAT={row_format}")
            row_values = values
            if profile == "json" and row_format == "REDUNDANT":
                # Account for the native 768-byte local prefix and the extra
                # string-length byte, placing $[1] across the same LOB boundary.
                row_values = row_values.replace("'p',15644", "'p',16411")
            client.query(f"INSERT INTO {name} WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<64) SELECT n,{row_values} FROM s")
            baseline = self.rows(client, name)
            assert len(baseline) == 64
            self.tables.append((name, baseline, self.index_rows(client, name)))
            if self.profile == "numeric":
                self.baseline_binary[name] = self.binary_values(client, name)
        self.current = {}
        self.current_index = {}
        self.resumed = {}

    def rows(self, client, name, index=None):
        index = index or self.scan_index
        force = f" FORCE INDEX({index})" if index else ""
        return self.ordered_query(client, f"SELECT {self.projection} FROM {name}{force}")

    def ordered_query(self, client, sql):
        # MySQL's VARCHAR addon sort records can contain the full wide values.
        # This fixture compares digests; avoid an unrelated sort_buffer_size
        # requirement on either backend by ordering the small digest rows here.
        if self.profile.startswith("varlob") or self.profile == "json":
            return sorted(client.query(sql), key=lambda row: int(row[0]))
        return client.query(sql + " ORDER BY id")

    def index_rows(self, client, name):
        if name.endswith("_noindex"):
            return self.ordered_query(client, f"SELECT {self.projection} FROM {name} WHERE {self.index_predicate}")
        return self.ordered_query(client, f"SELECT {self.projection} FROM {name} FORCE INDEX(value_idx) WHERE {self.index_predicate}")

    def binary_values(self, client, name):
        # Compare native float/double bytes without text protocol rounding.
        statement = client.prepare(f"SELECT id,c,d FROM {name} ORDER BY id")
        try:
            return client.execute(statement, [])[1]
        finally:
            client.close_statement(statement)

    def before_drain(self, client):
        for name, _, _ in self.tables:
            client.query(f"UPDATE {name} SET {self.update} WHERE id<=32")
            client.query(f"UPDATE {name} SET {self.update} WHERE id<=16")
            if self.profile == "json":
                # COW rollback leaves the FIRST version counter ahead of the
                # restored row reference. A later small diff must remain valid.
                client.query("SAVEPOINT json_version")
                client.query(f"UPDATE {name} SET a=JSON_SET(a,'$.a',REPEAT('r',35000)),c=JSON_SET(c,'$[2]',REPEAT('u',20000)) WHERE id=1")
                client.query("ROLLBACK TO SAVEPOINT json_version")
                client.query("RELEASE SAVEPOINT json_version")
                for _ in range(100):
                    client.query(f"UPDATE {name} SET {self.update} WHERE id=1")
                client.query(f"UPDATE {name} SET c=JSON_SET(c,'$[1]',REPEAT('b',40)) WHERE id=1")
                client.query(f"UPDATE {name} SET c=JSON_SET(c,'$[2]',REPEAT('v',20000)) WHERE id=1")
                client.query(f"UPDATE {name} SET c=JSON_REMOVE(c,'$[2]') WHERE id=2")
                client.query(f"UPDATE {name} SET a=JSON_SET(a,'$.z','short') WHERE id=3")
            client.query(f"UPDATE {name} SET {self.key_update} WHERE id=1")
            client.query(f"DELETE FROM {name} WHERE id=64")
            self.current[name] = self.rows(client, name)
            self.current_index[name] = self.index_rows(client, name)
            if self.profile == "numeric":
                self.current_binary[name] = self.binary_values(client, name)
            assert len(self.current[name]) == 63

    def after_resume(self, client):
        if self.profile == "temporal-enum":
            assert client.query("SELECT @@time_zone") == [["+08:00"]]
        for name, _, _ in self.tables:
            expected = self.current[name]
            assert self.rows(client, name) == expected, (self.profile, name, "current")
            assert self.index_rows(client, name) == self.current_index[name], (self.profile, name, "index")
            if self.profile == "numeric":
                assert self.binary_values(client, name) == self.current_binary[name]
            if self.profile == "temporal-enum":
                client.query("SET timestamp=1767225600.123456")
            client.query(f"UPDATE {name} SET {self.update} WHERE id>32")
            if self.profile == "temporal-enum":
                assert client.query(f"SELECT UNIX_TIMESTAMP(changed) FROM {name} WHERE id=34") == [["1767225600.123456"]]
                client.query("SET timestamp=DEFAULT")
            client.query(f"DELETE FROM {name} WHERE id=33")
            if self.profile == "unique":
                client.query(f"INSERT INTO {name} VALUES(70,'continued',70,7000,'continued-prefix')")
            if self.profile == "rowid":
                client.query(f"INSERT INTO {name} VALUES(70,1,'continued',7000),(71,1,'continued',NULL)")
            self.resumed[name] = (self.rows(client, name), self.index_rows(client, name),
                                  self.binary_values(client, name) if self.profile == "numeric" else None)

    def after_commit(self, client):
        for name, _, _ in self.tables:
            rows, index_rows, binary = self.resumed[name]
            assert self.rows(client, name) == rows, (self.profile, name, "commit")
            assert self.index_rows(client, name) == index_rows
            if binary is not None:
                assert self.binary_values(client, name) == binary
            if self.rowid_limit:
                self.check_rowid_boundary(client, name)
            client.query(f"DROP TEMPORARY TABLE {name}")

    def after_rollback(self, client):
        for name, baseline, baseline_index in self.tables:
            assert self.rows(client, name) == baseline, (self.profile, name, "rollback")
            assert self.index_rows(client, name) == baseline_index
            if self.profile == "numeric":
                assert self.binary_values(client, name) == self.baseline_binary[name]
            client.query("START TRANSACTION")
            client.query(f"UPDATE {name} SET {self.update} WHERE id=1")
            if self.profile == "rowid":
                client.query(f"INSERT INTO {name} VALUES(72,1,'after rollback',NULL)")
            committed = self.rows(client, name)
            client.query("COMMIT")
            assert self.rows(client, name) == committed
            if self.rowid_limit:
                self.check_rowid_boundary(client, name)
            client.query(f"DROP TEMPORARY TABLE {name}")

    def check_rowid_boundary(self, client, name):
        # Internal fault probe: position the real table-owned allocator at
        # its last 48-bit value. OFF and rollback must not fall back to global
        # allocation or rewind it. This is not physical-promotion acceptance.
        from preserve_trx_session_only_packet_e2e import SqlError
        before = self.rows(client, name)
        client.query("SET GLOBAL rds_preserve_trx_temp_table_enable=OFF")
        try:
            client.query("START TRANSACTION")
            client.query("SET SESSION debug='+d,preserve_temp_row_id_last'")
            try:
                client.query(f"INSERT INTO {name} VALUES(900,1,'last row id',NULL)")
            finally:
                client.query("SET SESSION debug='-d,preserve_temp_row_id_last'")
            assert client.query(f"SELECT COUNT(*) FROM {name} WHERE id=900") == [["1"]]
            client.query("ROLLBACK")
            try:
                client.query(f"INSERT INTO {name} VALUES(901,1,'must fail',NULL)")
            except SqlError as exc:
                assert exc.args[0] == 1114, exc.args
            else:
                raise AssertionError("exhausted hidden row ID wrapped or used global allocator")
            assert self.rows(client, name) == before
        finally:
            client.query("SET GLOBAL rds_preserve_trx_temp_table_enable=ON")

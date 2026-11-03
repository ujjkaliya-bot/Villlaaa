"""
mongo_compat.py - MongoDB backend that exposes the same pool/connection API
the bot already uses (pool.acquire() -> conn.fetch / fetchrow / fetchval /
execute / executemany), so bot.py, helpers.py and addstock_devices.py keep
working without PostgreSQL.

Only the SQL subset the bot actually uses is supported (SELECT / INSERT ...
ON CONFLICT / UPDATE / DELETE with simple WHERE, GROUP BY, ORDER BY, LIMIT).
Anything else raises SQLCompatError loudly instead of silently misbehaving.
"""

import datetime as _dt
import re

try:  # pymongo is only needed against a real MongoDB
    from pymongo.errors import DuplicateKeyError
except Exception:  # pragma: no cover
    class DuplicateKeyError(Exception):
        pass


class SQLCompatError(Exception):
    """Raised for SQL that this compatibility layer does not understand."""


class UniqueViolationError(Exception):
    """Raised when a plain INSERT hits an existing primary key."""


# ─── Schema (mirrors the old PostgreSQL tables) ──────────────────────────────

NOW = object()  # marker: default = current timestamp

TABLES = {
    "users": {
        "cols": [("user_id", "int", None), ("balance", "int", 0), ("referred_by", "int", None),
                 ("total_deposited", "int", 0), ("joined_date", "ts", NOW), ("banned", "int", 0),
                 ("discount", "int", 0), ("terms_accepted", "int", 0)],
        "pk": ["user_id"], "serial": False,
    },
    "settings": {
        "cols": [("key", "text", None), ("value", "text", None)],
        "pk": ["key"], "serial": False,
    },
    "stock": {
        "cols": [("phone", "text", None), ("session_file", "text", None), ("country_name", "text", None),
                 ("country_icon", "text", "🌍"), ("account_year", "int", None), ("category", "text", "Good"),
                 ("price", "int", None), ("available", "int", 1), ("twofa", "text", "None"),
                 ("added_date", "ts", NOW)],
        "pk": ["phone"], "serial": False,
    },
    "auto_prices": {
        "cols": [("country", "text", None), ("year", "text", None), ("price", "int", None)],
        "pk": ["country", "year"], "serial": False,
    },
    "deposits": {
        "cols": [("id", "int", None), ("user_id", "int", None), ("amount", "int", None),
                 ("method_name", "text", None), ("status", "text", None), ("utr", "text", None),
                 ("date", "ts", NOW)],
        "pk": ["id"], "serial": True,
    },
    "upi_orders": {
        "cols": [("order_id", "text", None), ("user_id", "int", None), ("amount", "int", None),
                 ("status", "text", None), ("date", "ts", NOW)],
        "pk": ["order_id"], "serial": False,
    },
    "bank_orders": {
        "cols": [("order_id", "text", None), ("user_id", "int", None), ("amount", "int", None),
                 ("utr", "text", None), ("status", "text", None), ("date", "ts", NOW)],
        "pk": ["order_id"], "serial": False,
    },
    "orders": {
        "cols": [("id", "int", None), ("user_id", "int", None), ("country", "text", None),
                 ("year", "int", None), ("price", "int", None), ("phone", "text", None),
                 ("otp", "text", None), ("date", "ts", NOW)],
        "pk": ["id"], "serial": True,
    },
    "custom_payments": {
        "cols": [("id", "int", None), ("name", "text", None), ("caption", "text", None),
                 ("qr_file_id", "text", None)],
        "pk": ["id"], "serial": True,
    },
    "admins": {
        "cols": [("user_id", "int", None), ("p_add_stock", "int", 0), ("p_manage_stock", "int", 0),
                 ("p_stats", "int", 0), ("p_bal", "int", 0), ("p_settings", "int", 0), ("p_demo", "int", 0)],
        "pk": ["user_id"], "serial": False,
    },
    "custom_countries": {
        "cols": [("code", "text", None), ("name", "text", None), ("flag", "text", None)],
        "pk": ["code"], "serial": False,
    },
}

# Extra (non-unique) indexes for the queries the bot runs most.
EXTRA_INDEXES = {
    "stock": [["country_name", "account_year", "available"], ["available"]],
    "orders": [["user_id"]],
    "deposits": [["status", "method_name"]],
    "users": [["referred_by"]],
    "bank_orders": [["status", "amount"], ["utr"]],
}


def _col_type(table: str, col: str):
    for name, kind, _ in TABLES[table]["cols"]:
        if name == col:
            return kind
    return None


def _table(name: str) -> dict:
    if name not in TABLES:
        raise SQLCompatError(f"Unknown table: {name}")
    return TABLES[name]


# ─── Record (asyncpg.Record look-alike) ──────────────────────────────────────

class Record(dict):
    """Supports row["name"], row[0], row.keys(), row.values(), dict(row)."""

    def __init__(self, names, vals):
        super().__init__()
        for n, v in zip(names, vals):
            dict.__setitem__(self, n, v)
        self._vals = list(vals)

    def __getitem__(self, k):
        if isinstance(k, int):
            return self._vals[k]
        return dict.__getitem__(self, k)

    def __iter__(self):  # asyncpg iterates values
        return iter(self._vals)


# ─── Small SQL helpers ───────────────────────────────────────────────────────

def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().rstrip(";").strip()


def _split_csv(s: str) -> list:
    """Split on top-level commas (respects quotes and parentheses)."""
    out, cur, depth, quote = [], [], 0, False
    for ch in s:
        if ch == "'":
            quote = not quote
        if not quote:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                out.append("".join(cur).strip())
                cur = []
                continue
        cur.append(ch)
    if cur:
        out.append("".join(cur).strip())
    return out


_PARAM = re.compile(r"\$(\d+)(?:::\w+(?:\[\])?)?")


def _operand(tok: str, params):
    tok = tok.strip()
    m = _PARAM.fullmatch(tok)
    if m:
        idx = int(m.group(1)) - 1
        if idx >= len(params):
            raise SQLCompatError(f"Missing parameter ${idx + 1}")
        return params[idx]
    if len(tok) >= 2 and tok[0] == "'" and tok[-1] == "'":
        return tok[1:-1].replace("''", "'")
    if tok.upper() == "NULL":
        return None
    if tok.upper() == "CURRENT_TIMESTAMP":
        return _dt.datetime.now()
    if re.fullmatch(r"-?\d+", tok):
        return int(tok)
    if re.fullmatch(r"-?\d+\.\d+", tok):
        return float(tok)
    raise SQLCompatError(f"Cannot evaluate operand: {tok}")


def _coerce(kind, v):
    if v is None or kind is None:
        return v
    if kind == "int":
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return v
        return int(v)
    if kind == "text":
        return v if isinstance(v, str) else str(v)
    if kind == "ts":
        if isinstance(v, _dt.datetime):
            return v
        s = str(v).strip()
        try:
            return _dt.datetime.fromisoformat(s)
        except ValueError:
            return _dt.datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
    return v


def _like_to_regex(pattern: str) -> str:
    out = ["^"]
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    out.append("$")
    return "".join(out)


def _norm_utr(v) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(v or "").upper())


# ─── WHERE parsing ───────────────────────────────────────────────────────────

def _parse_where(where, params, table):
    """Return (mongo_filter, post_filters). post_filters are python predicates."""
    if not where:
        return {}, []
    conds, post = [], []
    for c in re.split(r"\s+AND\s+", where.strip(), flags=re.I):
        c = c.strip()

        m = re.fullmatch(r"regexp_replace\(upper\((\w+)\),\s*'\[\^A-Z0-9\]',\s*''\s*,\s*'g'\)\s*=\s*(.+)", c, re.I)
        if m:
            col, want = m.group(1), _norm_utr(_operand(m.group(2), params))
            post.append(lambda d, col=col, want=want: _norm_utr(d.get(col)) == want and d.get(col) is not None)
            conds.append({col: {"$ne": None}})
            continue
        m = re.fullmatch(r"(\w+)\s+IS\s+NOT\s+NULL", c, re.I)
        if m:
            conds.append({m.group(1): {"$ne": None}})
            continue
        m = re.fullmatch(r"(\w+)\s+IS\s+NULL", c, re.I)
        if m:
            conds.append({m.group(1): None})
            continue
        m = re.fullmatch(r"(\w+)\s*=\s*ANY\s*\((.+)\)", c, re.I)
        if m:
            col = m.group(1)
            vals = _operand(m.group(2), params)
            k = _col_type(table, col)
            conds.append({col: {"$in": [_coerce(k, v) for v in vals]}})
            continue
        m = re.fullmatch(r"(\w+)\s+LIKE\s+(.+)", c, re.I)
        if m:
            col = m.group(1)
            pat = str(_operand(m.group(2), params))
            conds.append({col: {"$regex": _like_to_regex(pat), "$options": "s"}})
            continue
        m = re.fullmatch(r"(\w+)\s+IN\s*\((.+)\)", c, re.I)
        if m:
            col = m.group(1)
            k = _col_type(table, col)
            conds.append({col: {"$in": [_coerce(k, _operand(t, params)) for t in _split_csv(m.group(2))]}})
            continue
        m = re.fullmatch(r"(\w+)\s*(<>|!=|>=|<=|=|>|<)\s*(.+)", c)
        if m:
            col, op, rhs = m.group(1), m.group(2), m.group(3)
            val = _coerce(_col_type(table, col), _operand(rhs, params))
            mop = {"=": None, "<>": "$ne", "!=": "$ne", ">": "$gt", "<": "$lt", ">=": "$gte", "<=": "$lte"}[op]
            conds.append({col: val} if mop is None else {col: {mop: val}})
            continue
        raise SQLCompatError(f"Unsupported WHERE condition: {c}")

    if not conds:
        return {}, post
    return (conds[0] if len(conds) == 1 else {"$and": conds}), post


def _parse_order(order):
    out = []
    if not order:
        return out
    for part in _split_csv(order):
        m = re.fullmatch(r"(\w+)(?:\s+(ASC|DESC))?", part.strip(), re.I)
        if not m:
            raise SQLCompatError(f"Unsupported ORDER BY: {part}")
        out.append((m.group(1), (m.group(2) or "").upper() == "DESC"))
    return out


def _sort_rows(items, order, getter):
    for col, desc in reversed(order):
        def key(x, col=col):
            v = getter(x, col)
            return (v is None, v if v is not None else 0)
        items.sort(key=key, reverse=desc)


# ─── Connection ──────────────────────────────────────────────────────────────

_SEL = re.compile(
    r"SELECT\s+(?P<distinct>DISTINCT\s+)?(?P<cols>.+?)\s+FROM\s+(?P<table>\w+)"
    r"(?:\s+WHERE\s+(?P<where>.+?))?"
    r"(?:\s+GROUP\s+BY\s+(?P<group>.+?))?"
    r"(?:\s+ORDER\s+BY\s+(?P<order>.+?))?"
    r"(?:\s+LIMIT\s+(?P<limit>\S+))?"
    r"(?:\s+OFFSET\s+(?P<offset>\S+))?\s*$",
    re.I | re.S,
)
_INS = re.compile(
    r"INSERT\s+INTO\s+(?P<table>\w+)\s*\((?P<cols>.+?)\)\s*VALUES\s*\((?P<vals>.+?)\)"
    r"(?:\s+ON\s+CONFLICT\s*(?:\((?P<target>.+?)\))?\s*(?P<action>DO\s+NOTHING|DO\s+UPDATE\s+SET\s+(?P<set>.+?)))?"
    r"(?:\s+RETURNING\s+(?P<ret>.+?))?\s*$",
    re.I | re.S,
)
_UPD = re.compile(r"UPDATE\s+(?P<table>\w+)\s+SET\s+(?P<set>.+?)(?:\s+WHERE\s+(?P<where>.+))?$", re.I | re.S)
_DEL = re.compile(r"DELETE\s+FROM\s+(?P<table>\w+)(?:\s+WHERE\s+(?P<where>.+))?$", re.I | re.S)
_AGG = re.compile(r"(COUNT|SUM|MIN|MAX)\((\*|\w+)\)", re.I)


class Connection:
    def __init__(self, db):
        self._db = db

    # ---- public asyncpg-style API -------------------------------------------
    async def execute(self, sql, *params):
        _, status = await self._run(sql, params)
        return status

    async def executemany(self, sql, args):
        for p in args:
            await self._run(sql, tuple(p))

    async def fetch(self, sql, *params):
        rows, _ = await self._run(sql, params)
        return rows

    async def fetchrow(self, sql, *params):
        rows, _ = await self._run(sql, params)
        return rows[0] if rows else None

    async def fetchval(self, sql, *params):
        row = await self.fetchrow(sql, *params)
        return row[0] if row else None

    # ---- dispatcher ---------------------------------------------------------
    async def _run(self, sql, params):
        s = _norm(sql)
        head = s.split(" ", 1)[0].upper()
        if head == "SELECT":
            return await self._select(s, params), "SELECT"
        if head == "INSERT":
            return await self._insert(s, params)
        if head == "UPDATE":
            return [], await self._update(s, params)
        if head == "DELETE":
            return [], await self._delete(s, params)
        if head in ("CREATE", "ALTER"):
            return [], head  # schema-less: nothing to do
        raise SQLCompatError(f"Unsupported statement: {s[:80]}")

    # ---- helpers ------------------------------------------------------------
    async def _next_id(self, table):
        doc = await self._db["_counters"].find_one_and_update(
            {"_id": table}, {"$inc": {"seq": 1}}, upsert=True, return_document=True
        )
        return int(doc["seq"])

    async def _find_docs(self, table, flt, post, order=None, skip=0, limit=None, pushdown=False):
        cur = self._db[table].find(flt, {"_id": 0})
        if pushdown:
            if order:
                cur = cur.sort([(c, -1 if d else 1) for c, d in order])
            if skip:
                cur = cur.skip(skip)
            if limit is not None:
                cur = cur.limit(limit)
        docs = await cur.to_list(None)
        for p in post:
            docs = [d for d in docs if p(d)]
        return docs

    # ---- SELECT -------------------------------------------------------------
    async def _select(self, s, params):
        if re.fullmatch(r"SELECT\s+1", s, re.I):
            return [Record(["?column?"], [1])]

        m = re.fullmatch(r"SELECT\s+COUNT\(\*\)\s+FROM\s+\(\s*(SELECT\s+DISTINCT\s+.+?)\s*\)\s*\w+", s, re.I | re.S)
        if m:
            inner = await self._select(m.group(1), params)
            return [Record(["count"], [len(inner)])]

        m = _SEL.match(s)
        if not m:
            raise SQLCompatError(f"Unsupported SELECT: {s[:120]}")
        table = m.group("table")
        _table(table)
        flt, post = _parse_where(m.group("where"), params, table)
        order = _parse_order(m.group("order"))
        limit = _operand(m.group("limit"), params) if m.group("limit") else None
        offset = _operand(m.group("offset"), params) if m.group("offset") else 0
        distinct = bool(m.group("distinct"))
        gcols = [g.strip() for g in _split_csv(m.group("group"))] if m.group("group") else []

        # select list
        items = []  # (kind, name, arg): kind in star/col/agg/const
        for raw in _split_csv(m.group("cols")):
            raw = raw.strip()
            if raw == "*":
                items.append(("star", None, None))
                continue
            am = _AGG.fullmatch(raw)
            if am:
                items.append(("agg", am.group(1).lower(), am.group(2)))
            elif re.fullmatch(r"\w+", raw) and not raw.isdigit():
                items.append(("col", raw, None))
            elif raw.isdigit():
                items.append(("const", "?column?", int(raw)))
            else:
                raise SQLCompatError(f"Unsupported select item: {raw}")
        has_agg = any(k == "agg" for k, _, _ in items)

        simple = not (distinct or has_agg or gcols or post)
        docs = await self._find_docs(
            table, flt, post, order=order, skip=offset, limit=limit, pushdown=simple
        )

        # ---- aggregate / group by
        if has_agg or gcols:
            groups = {}
            for d in docs:
                groups.setdefault(tuple(d.get(g) for g in gcols), []).append(d)
            if not gcols:
                groups = {(): docs}
            rows = []
            for key, gdocs in groups.items():
                names, vals = [], []
                for kind, name, arg in items:
                    if kind == "agg":
                        names.append(name)
                        vals.append(self._aggregate(name, arg, gdocs))
                    elif kind == "col":
                        names.append(name)
                        vals.append(gdocs[0].get(name) if gdocs else None)
                    elif kind == "const":
                        names.append(name)
                        vals.append(arg)
                    else:
                        raise SQLCompatError("SELECT * with aggregates is unsupported")
                ctx = dict(zip(gcols, key))
                rows.append((Record(names, vals), ctx))
            _sort_rows(rows, order, lambda r, c: r[1].get(c, r[0].get(c)))
            recs = [r for r, _ in rows]
            recs = recs[offset:] if offset else recs
            return recs[:limit] if limit is not None else recs

        # ---- plain projection
        if not simple:
            _sort_rows(docs, order, lambda d, c: d.get(c))
        out, seen = [], set()
        for d in docs:
            names, vals = [], []
            for kind, name, arg in items:
                if kind == "star":
                    for cname, _, _ in TABLES[table]["cols"]:
                        names.append(cname)
                        vals.append(d.get(cname))
                elif kind == "col":
                    names.append(name)
                    vals.append(d.get(name))
                else:
                    names.append(name)
                    vals.append(arg)
            if distinct:
                key = tuple(vals)
                if key in seen:
                    continue
                seen.add(key)
            out.append(Record(names, vals))
        if not simple:
            out = out[offset:] if offset else out
            out = out[:limit] if limit is not None else out
        return out

    @staticmethod
    def _aggregate(func, arg, docs):
        if func == "count":
            return len(docs) if arg == "*" else sum(1 for d in docs if d.get(arg) is not None)
        vals = [d.get(arg) for d in docs if d.get(arg) is not None]
        if not vals:
            return None
        if func == "sum":
            return sum(vals)
        return min(vals) if func == "min" else max(vals)

    # ---- INSERT -------------------------------------------------------------
    async def _insert(self, s, params):
        m = _INS.match(s)
        if not m:
            raise SQLCompatError(f"Unsupported INSERT: {s[:120]}")
        table = m.group("table")
        meta = _table(table)
        cols = [c.strip() for c in _split_csv(m.group("cols"))]
        toks = _split_csv(m.group("vals"))
        if len(cols) != len(toks):
            raise SQLCompatError("INSERT column/value count mismatch")
        given = {c: _coerce(_col_type(table, c), _operand(t, params)) for c, t in zip(cols, toks)}

        doc = {}
        for name, kind, default in meta["cols"]:
            if name in given:
                doc[name] = given[name]
            elif default is NOW:
                doc[name] = _dt.datetime.now()
            else:
                doc[name] = default
        if meta["serial"] and doc.get("id") is None:
            doc["id"] = await self._next_id(table)

        coll = self._db[table]
        pk = meta["pk"]
        pk_filter = {k: doc[k] for k in pk}
        action = (m.group("action") or "").upper()

        returning = None
        if m.group("ret"):
            rnames = [r.strip() for r in _split_csv(m.group("ret"))]
            returning = Record(rnames, [doc.get(r) for r in rnames])

        if not action:
            try:
                await coll.insert_one(dict(doc))
            except DuplicateKeyError as exc:
                raise UniqueViolationError(str(exc)) from exc
            return ([returning] if returning else []), "INSERT 0 1"

        if action.startswith("DO NOTHING"):
            on_insert = {k: v for k, v in doc.items() if k not in pk}
            update = {"$setOnInsert": on_insert} if on_insert else {"$setOnInsert": {}}
            await self._upsert(coll, pk_filter, update)
            return ([returning] if returning else []), "INSERT 0 1"

        # DO UPDATE SET ...
        set_doc = {}
        for a in _split_csv(m.group("set")):
            am = re.fullmatch(r"(\w+)\s*=\s*(.+)", a.strip())
            if not am:
                raise SQLCompatError(f"Unsupported ON CONFLICT assignment: {a}")
            set_doc[am.group(1)] = _coerce(_col_type(table, am.group(1)), _operand(am.group(2), params))
        set_doc = {k: v for k, v in set_doc.items() if k not in pk}
        on_insert = {k: v for k, v in doc.items() if k not in pk and k not in set_doc}
        update = {}
        if set_doc:
            update["$set"] = set_doc
        if on_insert:
            update["$setOnInsert"] = on_insert
        if not update:
            update = {"$setOnInsert": {}}
        await self._upsert(coll, pk_filter, update)
        return ([returning] if returning else []), "INSERT 0 1"

    @staticmethod
    async def _upsert(coll, pk_filter, update):
        if update.get("$setOnInsert") == {}:
            update = {k: v for k, v in update.items() if k != "$setOnInsert"}
            if not update:
                update = {"$setOnInsert": {}}
        try:
            await coll.update_one(pk_filter, update, upsert=True)
        except DuplicateKeyError:  # concurrent upsert race: retry once
            await coll.update_one(pk_filter, update, upsert=True)

    # ---- UPDATE -------------------------------------------------------------
    async def _update(self, s, params):
        m = _UPD.match(s)
        if not m:
            raise SQLCompatError(f"Unsupported UPDATE: {s[:120]}")
        table = m.group("table")
        _table(table)
        flt, post = _parse_where(m.group("where"), params, table)
        if post:
            raise SQLCompatError("Function filters are not supported in UPDATE")

        set_doc, inc_doc, pipeline = {}, {}, {}
        for a in _split_csv(m.group("set")):
            a = a.strip()
            cm = re.fullmatch(
                r"(\w+)\s*=\s*CASE\s+WHEN\s+(\w+)\s*=\s*(\S+)\s+THEN\s+(\S+)\s+ELSE\s+(\S+)\s+END", a, re.I
            )
            if cm:
                col, cc, cv, tv, ev = cm.groups()
                pipeline[col] = {"$cond": [{"$eq": ["$" + cc, _operand(cv, params)]},
                                           _operand(tv, params), _operand(ev, params)]}
                continue
            im = re.fullmatch(r"(\w+)\s*=\s*(\w+)\s*([+-])\s*(.+)", a)
            if im and im.group(1) == im.group(2):
                amount = _coerce(_col_type(table, im.group(1)), _operand(im.group(4), params))
                inc_doc[im.group(1)] = amount if im.group(3) == "+" else -amount
                continue
            sm = re.fullmatch(r"(\w+)\s*=\s*(.+)", a)
            if not sm:
                raise SQLCompatError(f"Unsupported SET expression: {a}")
            set_doc[sm.group(1)] = _coerce(_col_type(table, sm.group(1)), _operand(sm.group(2), params))

        coll = self._db[table]
        if pipeline:
            stage = dict(set_doc)
            stage.update(pipeline)
            res = await coll.update_many(flt, [{"$set": stage}])
        else:
            update = {}
            if set_doc:
                update["$set"] = set_doc
            if inc_doc:
                update["$inc"] = inc_doc
            res = await coll.update_many(flt, update)
        return f"UPDATE {res.matched_count}"

    # ---- DELETE -------------------------------------------------------------
    async def _delete(self, s, params):
        m = _DEL.match(s)
        if not m:
            raise SQLCompatError(f"Unsupported DELETE: {s[:120]}")
        table = m.group("table")
        _table(table)
        flt, post = _parse_where(m.group("where"), params, table)
        if post:
            raise SQLCompatError("Function filters are not supported in DELETE")
        res = await self._db[table].delete_many(flt)
        return f"DELETE {res.deleted_count}"


# ─── Pool ────────────────────────────────────────────────────────────────────

class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class Pool:
    def __init__(self, db, client=None):
        self.db = db
        self.client = client
        self._conn = Connection(db)  # Motor is already pooled internally

    def acquire(self):
        return _Acquire(self._conn)

    async def execute(self, sql, *p):
        return await self._conn.execute(sql, *p)

    async def fetch(self, sql, *p):
        return await self._conn.fetch(sql, *p)

    async def fetchrow(self, sql, *p):
        return await self._conn.fetchrow(sql, *p)

    async def fetchval(self, sql, *p):
        return await self._conn.fetchval(sql, *p)

    async def close(self):
        if self.client is not None:
            self.client.close()


async def ensure_schema(db):
    """Create unique indexes (= old PRIMARY KEYs) and seed SERIAL counters."""
    for table, meta in TABLES.items():
        await db[table].create_index([(k, 1) for k in meta["pk"]], unique=True)
        for cols in EXTRA_INDEXES.get(table, []):
            await db[table].create_index([(c, 1) for c in cols])
        if meta["serial"]:
            existing = await db["_counters"].find_one({"_id": table})
            if not existing:
                top = await db[table].find({}, {"_id": 0, "id": 1}).sort([("id", -1)]).limit(1).to_list(1)
                start = int(top[0]["id"]) if top and top[0].get("id") is not None else 0
                try:
                    await db["_counters"].insert_one({"_id": table, "seq": start})
                except DuplicateKeyError:
                    pass

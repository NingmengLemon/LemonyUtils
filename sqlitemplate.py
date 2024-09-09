import sqlite3
from typing import TypeAlias, List, Any, Type
from collections.abc import Mapping, Iterable
from dataclasses import dataclass

from rwlock import ReadWriteLock

_TYPE_TO_SQLITE_TYPE_MAP = {
    str: "TEXT",
    int: "INTEGER",
    float: "REAL",
    bytes: "BLOB",
    None: "NULL",
}


def type_to_sqlitetype(t: Type) -> str | None:
    """将类型转换为 SQLite 中的类型

    :param t: 要被转换的类型
    :type t: Type
    :return: SQLite 类型，`None` 表示失败
    :rtype: str | None
    """
    return _TYPE_TO_SQLITE_TYPE_MAP.get(t)


_DataTypeDef: TypeAlias = dict[str, Type]


@dataclass
class _RowDef:
    name: str
    type: Type
    unique: bool = False
    nullable: bool = False

    @property
    def as_dict(self) -> _DataTypeDef:
        return {self.name: self.type}


class TableDef(list[_RowDef]):
    def __init__(self, cdef: Mapping[str, Type] | Iterable[_RowDef]):
        if isinstance(cdef, Mapping):
            for n, t in cdef.items():
                if n.lower() == "id":
                    raise NameError("name `id` is reserved for inner use")
                self.append(_RowDef(name=n, type=t))
        elif isinstance(cdef, Iterable):
            for rd in cdef:
                if isinstance(rd, _RowDef):
                    self.append(rd)
                else:
                    raise ValueError(f"expect RowDef, got {type(rd)}")

    @property
    def as_dict(self) -> _DataTypeDef:
        result: _DataTypeDef = {}
        for r in self:
            result.update(r.as_dict)
        return result


def dbcls_factory(
    tabledef: TableDef | _DataTypeDef | Iterable[_RowDef],
    cls_name: str,
    table_name: str | None = None,
):
    """从 `datadef` 动态生成一个继承自 `_DataBase` 的数据库类定义

    :param table: 数据库类定义中的表字段定义
    :type tabledef: TableDef
    :param cls_name: 类定义的名称
    :type cls_name: str
    :param table_name: 表的名称，省略时取 `cls_name` 的值
    :type table_name: str | None, optional
    :return: 新建的数据库类定义
    :rtype: Type[_DataBase]
    """
    if table_name is None:
        table_name = cls_name
    if not isinstance(tabledef, TableDef):
        tabledef = TableDef(tabledef)

    def init(self, dbpath: str):
        _DataBase.__init__(self, dbpath, tabledef, table_name)

    return type(cls_name, (_DataBase,), {"__init__": init})


class KeysValidationFailure(Exception):
    """当键验证不通过时抛出此错误"""


class _DataBase:
    def __init__(
        self, dbpath: str, tabledef: TableDef, tablename: str = "database"
    ) -> None:
        self._tabledef = tabledef
        self._datadef = tabledef.as_dict
        self._datadef_with_id = dict(self._datadef)
        self._datadef_with_id["id"] = int
        self._table_name = tablename

        self._dbpath = dbpath
        self._lock = ReadWriteLock()
        self._create_table()

    @staticmethod
    def validate_keys(
        d: Mapping[str, Any], dt: TableDef | Mapping[str, Any], fullmatch=False
    ):
        """以 `dt` 为标准验证 `d` 中的键

        :param d: 被验证的映射
        :type d: Mapping
        :param dt: 作为标准的表定义
        :type dt: DBContentDef
        :param fullmatch: 是否必须全部匹配, defaults to False
        :type fullmatch: bool, optional
        :raises KeysValidationFailure:
        """
        if isinstance(dt, TableDef):
            dt_keys = {d_.name for d_ in dt}
        else:
            dt_keys = set(dt.keys())
        d_keys = set(d.keys())

        if fullmatch:
            if d_keys != dt_keys:
                raise KeysValidationFailure("keys not equal")
            return

        unexpected_keys = d_keys - dt_keys
        if unexpected_keys:
            raise KeysValidationFailure(
                f"unexpected keys: {', '.join(unexpected_keys)}"
            )

    def _get_connection(self):
        return sqlite3.connect(self._dbpath)

    def _create_table(self):
        with self._lock.write_lock(), self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    {},
                    id INTEGER PRIMARY KEY AUTOINCREMENT
                )
                """.format(
                    self._table_name,
                    ", ".join(
                        [
                            (
                                f"{rd.name} {type_to_sqlitetype(rd.type)}"
                                + (" UNIQUE" if rd.unique else "")
                                + ("" if rd.nullable else " NOT NULL")
                            )
                            for rd in self._tabledef
                        ]
                    ),
                )
            )

    def add(self, item: dict[str, Any]) -> int | None:
        """增加条目

        :param item: 要增加的条目，键必须与定义完全相同（不计顺序）
        :type item: dict[str, Any]
        :return: 新增的条目在数据库中的id
        :rtype: int | None
        """
        self.validate_keys(item, self._datadef, fullmatch=True)
        with self._lock.write_lock(), self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO {} ({})
                VALUES ({})
                """.format(
                    self._table_name,
                    ", ".join(self._datadef.keys()),
                    ", ".join(["?"] * len(self._datadef)),
                ),
                [item[k] for k in self._datadef.keys()],
            )
            return cursor.lastrowid

    def delete(self, item_id: int):
        """删除条目

        :param item_id: 要删除的条目在数据库中的唯一id
        :type item_id: int
        """
        with self._lock.write_lock(), self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {self._table_name} WHERE id = ?", (item_id,))

    def modify(self, item_id: int, **info):
        """修改条目

        :param item_id: 要修改的条目在数据库中的唯一id
        :type item_id: int
        """
        if not info:
            return
        self.validate_keys(info, self._datadef, fullmatch=False)
        update_query = f"UPDATE {self._table_name} SET "
        update_values = []
        for key, value in info.items():
            update_query += f"{key} = ?, "
            update_values.append(value)
        update_query = update_query.rstrip(", ") + " WHERE id = ?"
        update_values.append(item_id)
        with self._lock.write_lock(), self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(update_query, tuple(update_values))

    def search(self, fuzzy_match=False, **info) -> List[dict]:
        """查找条目

        :return: 找到的条目们
        :rtype: List[dict]
        """
        self.validate_keys(info, self._datadef_with_id, fullmatch=False)
        query = f"SELECT * FROM {self._table_name}"
        params: List[Any] = []
        if info:
            query += " WHERE "
            conditions = []
            for key, value in info.items():
                if fuzzy_match and self._datadef_with_id.get(key) == str:
                    conditions.append(f"{key} LIKE ?")
                    params.append(f"%{value}%")
                else:
                    conditions.append(f"{key} = ?")
                    params.append(value)
            query += " AND ".join(conditions)
        with self._lock.read_lock(), self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
        books = []
        for row in rows:
            book = {k: row[i] for i, k in enumerate(self._datadef_with_id.keys())}
            books.append(book)
        return books

    def vacuum(self):
        with self._lock.write_lock(), self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("VACUUM")


if __name__ == "__main__":
    BOOKDB_CONTENT_DEF = {
        "title": str,
        "isbn": str,  # ISBN 13
        "author": str,
        "publisher": str,
        "desc": str,
        "cover": str,
        "price": float,
        "extra": str,
    }
    BookDB = dbcls_factory(BOOKDB_CONTENT_DEF, "BookDB", "books")
    db = BookDB("./books.db")

from __future__ import annotations

import logging
import re
import sqlite3
import atexit
from datetime import datetime
from pathlib import Path
from typing import Literal, TypedDict, Optional

import pandas as pd

BASE_PATH = Path(__file__).parent
DB_PATH = BASE_PATH / "financeiro.db"

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS transacoes (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    data      TEXT    NOT NULL,
    tipo      TEXT    NOT NULL CHECK (tipo IN ('faturamento','despesa')),
    valor     REAL    NOT NULL,
    descricao TEXT
);
"""

INDEX_SQL = "CREATE INDEX IF NOT EXISTS idx_tipo_data ON transacoes (tipo, data);"

class Registro(TypedDict):
    data: str
    tipo: Literal["faturamento", "despesa"]
    valor: float
    descricao: str | None

_conn_singleton: sqlite3.Connection | None = None

def _conn() -> sqlite3.Connection:
    global _conn_singleton
    if _conn_singleton is None:
        _conn_singleton = sqlite3.connect(
            DB_PATH,
            check_same_thread=False,
            detect_types=sqlite3.PARSE_DECLTYPES,
        )
        _conn_singleton.row_factory = sqlite3.Row
        _conn_singleton.execute("PRAGMA journal_mode=WAL;")
        _conn_singleton.execute(DDL)
        _conn_singleton.execute(INDEX_SQL)
    return _conn_singleton

atexit.register(lambda: _conn_singleton and _conn_singleton.close())

def inserir(r: Registro) -> None:
    c = _conn()
    c.execute(
        "INSERT INTO transacoes (data, tipo, valor, descricao) VALUES (?,?,?,?)",
        (r["data"], r["tipo"], r["valor"], r["descricao"]),
    )
    c.commit()

def deletar(reg_id: int) -> bool:
    c = _conn()
    cursor = c.execute("DELETE FROM transacoes WHERE id = ?", (reg_id,))
    c.commit()
    return cursor.rowcount > 0  # Retorna True se excluiu algum registro

def totais(
    data_inicio: Optional[str] = None, 
    data_fim: Optional[str] = None
) -> dict[str, float]:
    c = _conn()
    
    query = """
        SELECT tipo, SUM(valor) 
        FROM transacoes
    """
    
    conditions = []
    params = []
    
    if data_inicio:
        conditions.append("date(data) >= ?")
        params.append(data_inicio)
    if data_fim:
        conditions.append("date(data) <= ?")
        params.append(data_fim)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " GROUP BY tipo"
    
    return {
        t: v or 0.0
        for t, v in c.execute(query, params).fetchall()
    }

def listar_transacoes(
    data_inicio: Optional[str] = None, 
    data_fim: Optional[str] = None
) -> pd.DataFrame:
    c = _conn()
    
    query = """
        SELECT id, data, tipo, valor,
               COALESCE(descricao,'') AS descricao
        FROM transacoes
    """
    
    conditions = []
    params = []
    
    if data_inicio:
        conditions.append("date(data) >= ?")
        params.append(data_inicio)
    if data_fim:
        conditions.append("date(data) <= ?")
        params.append(data_fim)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ORDER BY date(data) DESC, id DESC"
    
    return pd.read_sql_query(
        query,
        c,
        parse_dates=["data"],
        params=params if params else None
    )

def faturamento_por_descricao(
    data_inicio: Optional[str] = None, 
    data_fim: Optional[str] = None
) -> pd.DataFrame:
    c = _conn()
    
    query = """
        SELECT descricao, SUM(valor) AS total
        FROM transacoes
        WHERE tipo='faturamento'
    """
    
    conditions = []
    params = []
    
    if data_inicio:
        conditions.append("date(data) >= ?")
        params.append(data_inicio)
    if data_fim:
        conditions.append("date(data) <= ?")
        params.append(data_fim)
    
    if conditions:
        query += " AND " + " AND ".join(conditions)
    
    query += " GROUP BY descricao ORDER BY total DESC"
    
    return pd.read_sql_query(query, c, params=params if params else None)

PADRAO = re.compile(
    r"registre(?:\s+no\s+banco\s+de\s+dados)?\s+(?:r\$\s*)?([\d\.,]+)\s+de\s+"
    r"(faturamento|despesa)s?\s+em\s+(\d{1,2}[\/-]\d{1,2}[\/-]\d{4})"
    r"(?:\s+com\s+descricao\s+(.+?))?[\s\.]*$",
    flags=re.I,
)

def tentar_extrair_comando(txt: str) -> Registro | None:
    m = PADRAO.search(txt.strip())
    if not m:
        return None
    valor = float(m.group(1).replace('.', '').replace(',', '.'))
    data_iso = datetime.strptime(m.group(3).replace('-', '/'), "%d/%m/%Y").date().isoformat()
    return {
        "valor": valor,
        "tipo": m.group(2).lower(),
        "data": data_iso,
        "descricao": m.group(4).strip() if m.group(4) else None,
    }
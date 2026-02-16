"""
Pydantic schemas — define response shapes and input validation.

Separating schemas from models follows a clean architecture pattern:
  - models.py  → how data is STORED (database columns)
  - schemas.py → how data is SENT/RECEIVED (API contract)

This separation allows the API response to differ from the DB structure
without modifying the database layer.
"""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel


# ── Operator ──────────────────────────────────────────────────────────

class OperatorResponse(BaseModel):
    """Schema for a single operator in list and detail views."""

    reg_ans: str
    cnpj: str | None
    razao_social: str | None
    uf: str | None
    modalidade: str | None

    model_config = {"from_attributes": True}


# ── Expense ───────────────────────────────────────────────────────────

class ExpenseResponse(BaseModel):
    """Schema for a single expense record."""

    id: int
    data_trimestre: date
    conta_contabil: str | None
    conta_contabil: str | None
    vl_saldo_final: Decimal | None

    model_config = {"from_attributes": True}


# ── Pagination wrapper ────────────────────────────────────────────────

class PaginationMeta(BaseModel):
    """Metadata about the current page of results."""

    total: int
    page: int
    limit: int
    total_pages: int


class PaginatedOperators(BaseModel):
    """Paginated response for GET /api/operadoras."""

    data: list[OperatorResponse]
    pagination: PaginationMeta


# ── Statistics ────────────────────────────────────────────────────────

class TopOperator(BaseModel):
    """An operator ranked by total expenses."""

    reg_ans: str
    razao_social: str | None
    total_expenses: Decimal


class ExpensesByUF(BaseModel):
    """Aggregated expenses for a single state."""

    uf: str
    total_expenses: Decimal


class StatisticsResponse(BaseModel):
    """Aggregated statistics for GET /api/estatisticas."""

    total_expenses: Decimal
    average_expenses: Decimal
    top_5_operators: list[TopOperator]
    expenses_by_uf: list[ExpensesByUF]

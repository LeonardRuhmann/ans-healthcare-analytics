"""
SQLAlchemy ORM models — mirrors the Star Schema from sql/ddl_schema.sql.

Tables:
  - dim_operadoras: Dimension table with operator info (reg_ans PK, cnpj, razao_social, uf, modalidade)
  - fact_despesas_eventos: Fact table with quarterly expenses (id PK, data_trimestre, reg_ans FK, conta_contabil, descricao, vl_saldo_final)
"""

from sqlalchemy import String, Numeric, Date, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


class Operadora(Base):
    __tablename__ = "dim_operadoras"

    reg_ans: Mapped[str] = mapped_column(String(20), primary_key=True)
    cnpj: Mapped[str | None] = mapped_column(String(14))
    razao_social: Mapped[str | None] = mapped_column(String(255))
    uf: Mapped[str | None] = mapped_column(String(2))
    modalidade: Mapped[str | None] = mapped_column(String(100))

    despesas: Mapped[list["Despesa"]] = relationship(back_populates="operadora")


class Despesa(Base):
    __tablename__ = "fact_despesas_eventos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    data_trimestre: Mapped[str] = mapped_column(Date, nullable=False)
    reg_ans: Mapped[str] = mapped_column(
        String(20), ForeignKey("dim_operadoras.reg_ans"), nullable=False
    )
    conta_contabil: Mapped[str | None] = mapped_column(String(50))
    vl_saldo_final: Mapped[float | None] = mapped_column(Numeric(18, 2))

    operadora: Mapped["Operadora"] = relationship(back_populates="despesas")

-- ============================================================
-- Analytical Queries
-- ============================================================
-- IMPORTANT: ANS financial data is Year-to-Date (YTD).
--   Q1_YTD = Jan-Mar only
--   Q2_YTD = Jan-Mar + Apr-Jun (cumulative)
--   Q3_YTD = Jan-Mar + Apr-Jun + Jul-Sep (cumulative)
--
-- All queries filter by CHAR_LENGTH(conta_contabil) = 9
-- to use only leaf-level accounts and avoid hierarchy double counting.
-- ============================================================


-- ============================================================
-- Query 1: Top 5 Operators — Highest Quarterly Expense Growth
-- ============================================================
-- Compares real Q3 spending vs real Q1 spending.
-- Since data is YTD, real quarterly value is:
--   Q1_real = Q1_YTD  (no prior quarter to subtract)
--   Q3_real = Q3_YTD - Q2_YTD
WITH DateBounds AS (
    SELECT
        MIN(data_trimestre) AS first_dt,
        MAX(data_trimestre) AS last_dt
    FROM fact_despesas_eventos
    WHERE CHAR_LENGTH(conta_contabil) = 9
),
OperatorQuarterlyYTD AS (
    -- One row per (operator, quarter) with YTD total
    SELECT
        reg_ans,
        data_trimestre,
        SUM(vl_saldo_final) AS ytd_despesa
    FROM fact_despesas_eventos
    WHERE CHAR_LENGTH(conta_contabil) = 9
    GROUP BY reg_ans, data_trimestre
),
OperatorPivot AS (
    -- Pivots 3 rows per operator into 1 row with q1/q2/q3 columns.
    -- Uses MAX(CASE WHEN) pattern since MySQL has no native PIVOT.
    SELECT
        q.reg_ans,
        o.razao_social,
        MAX(CASE WHEN q.data_trimestre = db.first_dt THEN q.ytd_despesa END) AS q1_ytd,
        MAX(CASE WHEN q.data_trimestre = db.last_dt  THEN q.ytd_despesa END) AS q3_ytd,
        MAX(CASE WHEN q.data_trimestre NOT IN (db.first_dt, db.last_dt)
                 THEN q.ytd_despesa END) AS q2_ytd
    FROM OperatorQuarterlyYTD q
    JOIN dim_operadoras o  ON q.reg_ans = o.reg_ans
    CROSS JOIN DateBounds db
    GROUP BY q.reg_ans, o.razao_social
)
SELECT
    reg_ans,
    razao_social,
    q1_ytd as first_q_val,
    (q3_ytd - q2_ytd) as last_q_val,
    ((q3_ytd - q2_ytd) - q1_ytd) / q1_ytd * 100 AS growth_pct
FROM OperatorPivot
WHERE q1_ytd  > 0         -- Avoid division by zero
  AND q2_ytd IS NOT NULL  -- Ensure operator has data in all 3 quarters
  AND q3_ytd IS NOT NULL
ORDER BY growth_pct DESC
LIMIT 5;


-- ============================================================
-- Query 2: Top 5 States by Total Expenses
-- ============================================================
-- Uses only the latest YTD quarter to represent the full-year total.
-- Summing all quarters would triple-count because of YTD accumulation.
SELECT
    o.uf,
    SUM(d.vl_saldo_final) AS total_despesas
FROM fact_despesas_eventos d
JOIN dim_operadoras o ON d.reg_ans = o.reg_ans
WHERE CHAR_LENGTH(d.conta_contabil) = 9
  AND d.data_trimestre = (SELECT MAX(data_trimestre) FROM fact_despesas_eventos)
GROUP BY o.uf
ORDER BY total_despesas DESC
LIMIT 5;


-- ============================================================
-- Query 3: Average Expense per Operator, by State
-- ============================================================
-- Same YTD filter as Query 2: only the latest quarter.
SELECT
    o.uf,
    SUM(d.vl_saldo_final) / COUNT(DISTINCT d.reg_ans) AS avg_despesa_por_operadora
FROM fact_despesas_eventos d
JOIN dim_operadoras o ON d.reg_ans = o.reg_ans
WHERE CHAR_LENGTH(d.conta_contabil) = 9
  AND d.data_trimestre = (SELECT MAX(data_trimestre) FROM fact_despesas_eventos)
GROUP BY o.uf
ORDER BY avg_despesa_por_operadora DESC;


-- ============================================================
-- Query 4: Operators Above Market Average in 2+ Quarters
-- ============================================================
-- YTD comparison is valid here because each operator's YTD is compared
-- against the market average YTD for the SAME quarter (apples-to-apples).
WITH MarketAverage AS (
    -- Average YTD expense across all operators, per quarter
    SELECT
        data_trimestre,
        AVG(operator_ytd) AS avg_market_expense
    FROM (
        SELECT
            reg_ans,
            data_trimestre,
            SUM(vl_saldo_final) AS operator_ytd
        FROM fact_despesas_eventos
        WHERE CHAR_LENGTH(conta_contabil) = 9
        GROUP BY reg_ans, data_trimestre
    ) per_operator
    GROUP BY data_trimestre
),
OperatorYTD AS (
    SELECT
        reg_ans,
        data_trimestre,
        SUM(vl_saldo_final) AS operator_ytd
    FROM fact_despesas_eventos
    WHERE CHAR_LENGTH(conta_contabil) = 9
    GROUP BY reg_ans, data_trimestre
),
AboveAverage AS (
    -- Flag each (operator, quarter) as above or below market average
    SELECT
        oe.reg_ans,
        CASE WHEN oe.operator_ytd > ma.avg_market_expense THEN 1 ELSE 0 END AS is_above
    FROM OperatorYTD oe
    JOIN MarketAverage ma ON oe.data_trimestre = ma.data_trimestre
)
SELECT
    aa.reg_ans,
    o.razao_social,
    SUM(aa.is_above) AS quarters_above_avg
FROM AboveAverage aa
JOIN dim_operadoras o ON aa.reg_ans = o.reg_ans
GROUP BY aa.reg_ans, o.razao_social
HAVING SUM(aa.is_above) >= 2;

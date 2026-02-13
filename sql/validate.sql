-- Validation Script: Run after import to verify data integrity
-- Usage: docker exec -i mysql-ans mysql -uroot -proot ans_test < sql/validate.sql

-- ============================================================
-- 1. Row Counts
-- ============================================================
SELECT '=== ROW COUNTS ===' as '';
SELECT 'dim_operadoras' as tabela, COUNT(*) as total FROM dim_operadoras
UNION ALL
SELECT 'fact_despesas_eventos', COUNT(*) FROM fact_despesas_eventos;

-- ============================================================
-- 2. Referential Integrity (Orphan Check)
-- Expected: 0 orphans
-- ============================================================
SELECT '=== ORPHAN CHECK ===' as '';
SELECT COUNT(*) as registros_orfaos
FROM fact_despesas_eventos f
LEFT JOIN dim_operadoras o ON f.reg_ans = o.reg_ans
WHERE o.reg_ans IS NULL;

-- ============================================================
-- 3. Data Quality Checks
-- ============================================================
SELECT '=== DATA QUALITY ===' as '';

-- NULL dates
SELECT 'Dates NULL' as check_name, COUNT(*) as total
FROM fact_despesas_eventos WHERE data_trimestre IS NULL;

-- NULL values
SELECT 'Values NULL' as check_name, COUNT(*) as total
FROM fact_despesas_eventos WHERE vl_saldo_final IS NULL;

-- Distinct quarters
SELECT '=== QUARTERS FOUND ===' as '';
SELECT DISTINCT data_trimestre FROM fact_despesas_eventos ORDER BY data_trimestre;

-- ============================================================
-- 4. Summary Statistics (ALL levels — for comparison)
-- ============================================================
SELECT '=== SUMMARY (ALL LEVELS) ===' as '';
SELECT
    COUNT(DISTINCT reg_ans) as operadoras_com_despesas,
    COUNT(DISTINCT data_trimestre) as trimestres,
    SUM(vl_saldo_final) as total_despesas,
    MIN(vl_saldo_final) as min_valor,
    MAX(vl_saldo_final) as max_valor
FROM fact_despesas_eventos;

-- ============================================================
-- 5. Hierarchy Distribution (Account Code Levels)
-- ============================================================
SELECT '=== ACCOUNT HIERARCHY ===' as '';
SELECT
    CHAR_LENGTH(conta_contabil) as account_level,
    COUNT(*) as row_count,
    SUM(vl_saldo_final) as total_value
FROM fact_despesas_eventos
GROUP BY CHAR_LENGTH(conta_contabil)
ORDER BY account_level;

-- ============================================================
-- 6. Summary Statistics (LEAF ONLY — no double counting)
-- ============================================================
SELECT '=== SUMMARY (LEAF ONLY, length=9) ===' as '';
SELECT
    COUNT(DISTINCT reg_ans) as operadoras_com_despesas,
    COUNT(DISTINCT data_trimestre) as trimestres,
    SUM(vl_saldo_final) as total_despesas,
    MIN(vl_saldo_final) as min_valor,
    MAX(vl_saldo_final) as max_valor
FROM fact_despesas_eventos
WHERE CHAR_LENGTH(conta_contabil) = 9;

-- ============================================================
-- 7. Sample Data (Top 5 LEAF by value)
-- ============================================================
SELECT '=== TOP 5 DESPESAS (LEAF) ===' as '';
SELECT f.reg_ans, o.razao_social, f.data_trimestre, f.vl_saldo_final
FROM fact_despesas_eventos f
JOIN dim_operadoras o ON f.reg_ans = o.reg_ans
WHERE CHAR_LENGTH(f.conta_contabil) = 9
ORDER BY f.vl_saldo_final DESC
LIMIT 5;

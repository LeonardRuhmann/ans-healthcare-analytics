-- Docker Import Script (Adapted for real CSV structure)
-- This script is executed automatically by MySQL on container startup.
-- It uses LOAD DATA INFILE (server-side) since files are mounted via --secure-file-priv.

-- ============================================================
-- 1. Import Operators (From Relatorio_Cadop.csv)
-- ============================================================
CREATE TEMPORARY TABLE temp_operadoras (
    registro_ans VARCHAR(20),
    cnpj VARCHAR(20),
    razao_social VARCHAR(255),
    nome_fantasia VARCHAR(255),
    modalidade VARCHAR(100),
    logradouro VARCHAR(255),
    numero VARCHAR(50),
    complemento VARCHAR(255),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf VARCHAR(2),
    cep VARCHAR(20),
    ddd VARCHAR(5),
    telefone VARCHAR(20),
    fax VARCHAR(20),
    endereco_eletronico VARCHAR(255),
    representante VARCHAR(255),
    cargo_representante VARCHAR(100),
    regiao_comercializacao VARCHAR(50),
    data_registro_ans VARCHAR(20)
);

LOAD DATA INFILE '/var/lib/mysql-files/Relatorio_Cadop.csv'
INTO TABLE temp_operadoras
CHARACTER SET 'utf8'
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(registro_ans, cnpj, razao_social, nome_fantasia, modalidade, logradouro, numero, complemento, bairro, cidade, uf, cep, ddd, telefone, fax, endereco_eletronico, representante, cargo_representante, regiao_comercializacao, data_registro_ans);

INSERT IGNORE INTO dim_operadoras (reg_ans, cnpj, razao_social, modalidade, uf)
SELECT DISTINCT registro_ans, cnpj, razao_social, modalidade, uf
FROM temp_operadoras;

DROP TEMPORARY TABLE temp_operadoras;

-- ============================================================
-- 2. Import Expenses (From consolidado_despesas.csv)
-- CSV structure: DATA;REG_ANS;CD_CONTA_CONTABIL;CNPJ;RazaoSocial;Trimestre;Ano;ValorDespesas
-- ============================================================
CREATE TEMPORARY TABLE temp_despesas (
    data_str VARCHAR(10),
    reg_ans VARCHAR(20),
    cd_conta_contabil VARCHAR(50),
    cnpj_empty VARCHAR(20),
    razao_empty VARCHAR(255),
    trimestre_str VARCHAR(10),
    ano_str VARCHAR(10),
    vl_saldo_final_str VARCHAR(20)
);

LOAD DATA INFILE '/var/lib/mysql-files/consolidado_despesas.csv'
INTO TABLE temp_despesas
CHARACTER SET 'utf8'
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(data_str, reg_ans, cd_conta_contabil, cnpj_empty, razao_empty, trimestre_str, ano_str, vl_saldo_final_str);

INSERT INTO fact_despesas_eventos (data_trimestre, reg_ans, conta_contabil, vl_saldo_final)
SELECT
    STR_TO_DATE(data_str, '%Y-%m-%d'),
    reg_ans,
    cd_conta_contabil,
    CAST(REPLACE(vl_saldo_final_str, ',', '.') AS DECIMAL(18,2))
FROM temp_despesas
WHERE reg_ans IN (SELECT reg_ans FROM dim_operadoras);

DROP TEMPORARY TABLE temp_despesas;

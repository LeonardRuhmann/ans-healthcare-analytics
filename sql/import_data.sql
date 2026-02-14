-- Import Data Script (MySQL 8.0 Example)
-- Adjust 'LOCAL' depending on server configuration (client vs server side load)

-- 1. Import Operators (From Cadastral CSV)
-- Using a temporary table to handle duplicates and transformations
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
    regiao_comercializacao VARCHAR(50), -- ou DATA
    data_registro_ans VARCHAR(20)
);

LOAD DATA LOCAL INFILE '/path/to/Relatorio_cadop.csv' 
INTO TABLE temp_operadoras
CHARACTER SET 'utf8'
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(registro_ans, cnpj, razao_social, nome_fantasia, modalidade, logradouro, numero, complemento, bairro, cidade, uf, cep, ddd, telefone, fax, endereco_eletronico, representante, cargo_representante, regiao_comercializacao, data_registro_ans);

-- Insert Unique Operators
INSERT IGNORE INTO dim_operadoras (reg_ans, cnpj, razao_social, modalidade, uf)
SELECT DISTINCT registro_ans, cnpj, razao_social, modalidade, uf
FROM temp_operadoras;

-- 2. Import Expenses (Consolidated)
-- Using temporary table to handle date conversion and decimal replacement
CREATE TEMPORARY TABLE temp_despesas (
    data_str VARCHAR(10),
    reg_ans VARCHAR(20),
    conta_contabil VARCHAR(50),
    cnpj_dummy VARCHAR(20),
    razao_dummy VARCHAR(255),
    trim_dummy VARCHAR(10),
    ano_dummy VARCHAR(10),
    vl_saldo_final_str VARCHAR(20)
);

LOAD DATA LOCAL INFILE '/path/to/consolidado_despesas.csv' 
INTO TABLE temp_despesas
CHARACTER SET 'utf8'
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(data_str, reg_ans, conta_contabil, cnpj_dummy, razao_dummy, trim_dummy, ano_dummy, vl_saldo_final_str);

-- Transform and Load
INSERT INTO fact_despesas_eventos (data_trimestre, reg_ans, conta_contabil, descricao, vl_saldo_final)
SELECT 
    STR_TO_DATE(data_str, '%Y-%m-%d'), -- Convert YYYY-MM-DD
    reg_ans,
    conta_contabil,
    NULL,
    CAST(REPLACE(vl_saldo_final_str, ',', '.') AS DECIMAL(18,2)) -- Convert "1234,56" to 1234.56
FROM temp_despesas
WHERE reg_ans IN (SELECT reg_ans FROM dim_operadoras); -- Ensure Referential Integrity

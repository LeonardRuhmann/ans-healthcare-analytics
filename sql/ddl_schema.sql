-- Table: operadoras (Dimension)
-- Holds unique information about each Health Operator.
CREATE TABLE IF NOT EXISTS dim_operadoras (
    reg_ans VARCHAR(20) PRIMARY KEY, -- Unique ID from ANS
    cnpj VARCHAR(14),                -- Standardized 14-digit CNPJ
    razao_social VARCHAR(255),       -- Full legal name
    uf VARCHAR(2),                   -- State abbreviation (e.g., 'RO', 'SP')
    modalidade VARCHAR(100),         -- Company type

    INDEX idx_operadoras_cnpj (cnpj)
);

-- Table: despesas_eventos (Fact)
CREATE TABLE IF NOT EXISTS fact_despesas_eventos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data_trimestre DATE NOT NULL,
    reg_ans VARCHAR(20) NOT NULL,
    conta_contabil VARCHAR(50),
    descricao VARCHAR(255),
    vl_saldo_final NUMERIC(18,2),

    INDEX idx_despesas_data (data_trimestre),
    INDEX idx_despesas_reg (reg_ans),

    CONSTRAINT fk_operadora
        FOREIGN KEY (reg_ans) 
        REFERENCES dim_operadoras(reg_ans)
        ON DELETE CASCADE
);

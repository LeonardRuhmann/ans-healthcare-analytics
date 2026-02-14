# IntuitiveCare-Teste - Documentação Técnica

Este repositório contém a solução para o teste técnico de Engenharia de Dados. O projeto consiste em um pipeline ETL (Extract, Transform, Load) para coletar, limpar, enriquecer e agregar dados financeiros e cadastrais de operadoras de planos de saúde a partir do portal de dados abertos da ANS.

## 🔄 Fluxo do Projeto (Pipeline)

O pipeline executa as seguintes etapas sequencialmente:

1.  **Scraping e Identificação:** O sistema acessa o site da ANS e identifica os arquivos de demonstrações contábeis (ZIP) mais recentes (padrão Trimestral).
2.  **Extração e Transformação (Stream):**
    * Baixa os arquivos ZIP utilizando *streaming* para economizar memória.
    * Lê o conteúdo em memória e aplica filtros para isolar despesas assistenciais.
    * Normaliza dados numéricos e datas.
3.  **Consolidação:** Unifica os dados dos trimestres processados em um único arquivo intermediário.
4.  **Enriquecimento (Join):**
    * Baixa a base cadastral de operadoras ativas.
    * Realiza o cruzamento (Join) entre dados financeiros e cadastrais via `REG_ANS`.
5.  **Validação (Quality Gate):** Separa registros inválidos ou inconsistentes em um arquivo de "Quarentena", mantendo a integridade contábil dos dados válidos.
6.  **Agregação e Entrega:**
    * Calcula totais, médias trimestrais e desvio padrão.
    * Gera o arquivo final compactado `Teste_Leonardo_Ruhmann.zip`.

---

## 🚀 Como Executar o Projeto

Siga os passos abaixo para rodar o pipeline ou as análises.

### Pré-requisitos
* **Python 3.8+** (para execução local do ETL).
* **Git** instalado.
* **Docker & Docker Compose** (para validação e analytics).

### Passo a Passo

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/LeonardRuhmann/IntuitiveCare-Teste.git
    cd IntuitiveCare-Teste
    ```

2.  **Execute o pipeline completo (recomendado):**
    ```bash
    ./run.sh
    ```
    Este script automatiza todo o fluxo: cria o ambiente virtual, instala dependências, executa o ETL Python, sobe o Docker com MySQL 8.0, importa os dados, roda validações e executa as queries analíticas.

    > **Nota:** O MySQL 8.0 roda dentro do container Docker — não é necessário instalá-lo na máquina.

    O script também aceita subcomandos para execução parcial:
    | Comando | Descrição |
    | :--- | :--- |
    | `./run.sh` | Pipeline completo (ETL + Docker + Analytics) |
    | `./run.sh etl` | Apenas o pipeline Python (gera os dados em `output/`) |
    | `./run.sh docker` | Apenas Docker + Analytics (requer dados já gerados) |
    | `./run.sh down` | Para e remove o container Docker |

3.  **Execução manual (alternativa):**

    Caso prefira executar cada etapa individualmente:
    
    a. **ETL Python:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # Linux/Mac
    # .\venv\Scripts\activate # Windows
    pip install -r requirements.txt
    python -m src.main
    ```

    b. **Docker & SQL:**
    ```bash
    docker-compose up -d
    # Aguarde ~30s para o MySQL inicializar e importar os dados
    docker exec -i mysql-ans mysql -uroot -proot ans_test < sql/validate.sql
    docker exec -i mysql-ans mysql -uroot -proot ans_test < sql/queries_analytics.sql
    docker-compose down -v
    ```

4.  **Verifique os Resultados (Pós-Etapa 1):**
    Os arquivos gerados estarão na pasta `output/`:
    * `Teste_Leonardo_Ruhmann.zip` — Arquivo final com despesas agregadas
    * `data_clean.csv` — Dados validados (entrada para agregação)
    * `data_quarantine.csv` — Registros inválidos/inconsistentes (auditoria)
    * `consolidado_despesas.zip` — Dados consolidados (intermediário)
    * `enriched_data.zip` — Dados enriquecidos com cadastro (intermediário)

---

## 📚 Documentação Técnica e Decisões Arquiteturais

Abaixo detalho as decisões de design, trade-offs escolhidos e estratégias de resolução de problemas adotadas durante o desenvolvimento.

##  Pipeline de Extração e Transformação (ETL)

### Acesso ao Portal e Arquitetura

#### **Decisão 1: Separação de Responsabilidades (`Services`)**
Criei uma pasta `services` para isolar a lógica de negócio:
* `AnsDataClient`: Acesso ao portal e download.
* `IngestionService`: Orquestração do processamento de arquivos.
* `ZipProcessor`, `DataConsolidator`, etc.: Implementações específicas de cada etapa.

* **Justificativa:** Evita misturar a lógica de orquestração com detalhes de implementação.
* **✅ Prós:** Código desacoplado, fácil manutenção e testabilidade.
* **⚠️ Contras:** Aumenta a quantidade de arquivos.

#### **Decisão 2: Ferramenta de Scraping (`BeautifulSoup4` vs `Selenium`)**
Optei pela biblioteca `BeautifulSoup4` (BS4) em conjunto com `Requests` ao invés de Selenium ou Regex puro.
* **Justificativa (KISS):** O portal da ANS disponibiliza listagens HTML estáticas. Usar Selenium seria *overkill* (pesado e lento). Regex puro seria frágil para tags HTML.
* **✅ Prós:** Leve, rápido e robusto contra pequenas mudanças de layout HTML.
* **⚠️ Contras:** Não funcionaria se o site dependesse de JavaScript para renderizar os links (o que não é o caso).

#### **Decisão 3: Detecção de Trimestres via Regex (`re`)**
Desenvolvi uma Expressão Regular para identificar os arquivos (método `_detect_quarter`).
* **Justificativa:** Os nomes variam muito (ex: `1T2025` vs `dados_2023_1_trim`).
* **✅ Prós:** Evita *hardcoding* de nomes, tornando a solução flexível para diferentes anos.
* **⚠️ Contras:** Exige uma regex complexa e bem testada para cobrir todos os casos de borda.

#### **Decisão 4: Download em Stream (`shutil.copyfileobj`)**
Utilizei `stream=True` nas requisições HTTP.
* **Justificativa:** Processa o download em partes (*chunks*).
* **✅ Prós:** Previne estouro de memória (RAM) ao baixar arquivos ZIP grandes em servidores modestos.
* **⚠️ Contras:** Nenhum significativo para este contexto.

#### **Decisão 4b: Observabilidade (Logging)**
Substituição de `print()` por `logging` estruturado.
* **Justificativa:** Permite monitoramento adequado em produção e categorização de níveis de erro (`INFO`, `WARNING`, `ERROR`).
* **✅ Prós:** Logs mais limpos e possibilidade de persistência em arquivo.

---

###  Processamento e Transformação

#### **Decisão 5: Processamento Incremental (Iterativo)**
O script itera sobre os arquivos ZIP baixados um por um, descartando os dados brutos da memória após a extração.
* **Justificativa:** Escalabilidade.
* **✅ Prós:** Se a ANS liberar arquivos de 10GB no futuro, essa solução continuará funcionando sem estourar a memória.
* **⚠️ Contras:** Pode ser ligeiramente mais lento que carregar tudo na memória para volumes muito pequenos (overhead de I/O), mas a segurança compensa.

#### **Decisão 6: Filtragem em Memória (`ZipFile`)**
Uso da classe `ZipProcessor` para inspecionar o conteúdo do ZIP sem extraí-lo para o disco.
* **Justificativa:** Evitar I/O desnecessário de disco.
* **✅ Prós:** Mais rápido e limpo (não deixa "lixo" temporário no disco).
* **⚠️ Contras:** Exige manipulação de bytes em memória.

#### **Decisão 7: Normalização de Encoding e Numéricos**
Leitura forçada em `UTF-8` e conversão de `1.000,00` para `1000.0`.
* **Justificativa:** Garantir integridade de acentos e cálculos matemáticos corretos.
* **✅ Prós:** Previne erros silenciosos em análises futuras.

---

###  Consolidação e Limpeza

#### **Decisão 8: Tratamento de Valores Zerados vs. Negativos**
Remoção física de registros com valor `0.0`. O arquivo consolidado é persistido em `output/consolidado_despesas.zip` e reutilizado nas etapas seguintes.
 **Justificativa:** Registros zerados indicam inatividade e geram "ruído".
* **✅ Prós:** Redução drástica do volume de dados sem perda de informação.
* **⚠️ Contras:** Perde-se o histórico de que a conta existiu naquele trimestre (embora inativa).

> **Nota sobre Negativos:** Valores negativos foram **mantidos**, pois representam estornos ou ajustes contábeis legítimos.

#### **Decisão 9: Parser de Datas (`Pandas` vs `Regex`)**
Uso de `pd.to_datetime(errors='coerce')` em vez de Regex manual.
* **Justificativa:** *Fail-safe*. O método nativo gerencia variações (`/` ou `-`) automaticamente.
* **✅ Prós:** Robustez. Dados inválidos viram `NaT` sem quebrar o pipeline.
* **⚠️ Contras:** Menos controle granular sobre formatos exóticos (mas suficiente para o padrão ANS).

---

##  Enriquecimento e Qualidade de Dados

###  Validação (Quarantine Pattern)

#### **Decisão 10: Segregação (Quarentena) vs. Exclusão**
Ao encontrar registros inválidos (ex: CNPJ incorreto ou Razão Social vazia), a decisão foi **não excluir**, mas separar em `data_quarantine.csv`.
* **Justificativa:** Em sistemas financeiros, o valor monetário é real e precisa ser contabilizado, mesmo que o dado cadastral esteja errado. Excluir geraria "furos" no balanço.
* **✅ Prós:**
    * Preservação da integridade contábil (Soma total bate com a origem).
    * Rastreabilidade para correção manual posterior (Backoffice).
* **⚠️ Contras:**
    * Aumenta a complexidade do pipeline (gera 2 saídas em vez de 1).
    * Requer armazenamento para dados "sujos".

---

###  Enriquecimento de Dados (Cadastral)

#### **Decisão 11: Estratégia de Join (In-Memory)**
Para cruzar dados financeiros e cadastrais, optou-se pelo processamento em memória com Pandas, diferindo da extração incremental.
* **Justificativa:** O dataset final consolidado (~44k linhas) é pequeno o suficiente para memória RAM.
* **✅ Prós (KISS):** Simplicidade e rapidez de implementação. Frameworks distribuídos (Spark) seriam *over-engineering*.
* **⚠️ Contras:** Se o dataset final crescesse para a casa dos Gigabytes, essa etapa precisaria ser refatorada para chunks.

#### **Decisão 12: Chave de Ligação (`REG_ANS`)**
Uso de `REG_ANS` como chave primária de join, ao invés do CNPJ solicitado.
* **Justificativa (Realidade vs Requisito):** Os arquivos financeiros brutos **não continham CNPJ**, apenas `REG_ANS`.
* **✅ Prós:** Viabilizou o enriquecimento. O CNPJ foi trazido da base cadastral para a financeira.
* **⚠️ Contras:** Dependência da qualidade da base cadastral da ANS.

---

###  Resolução de Anomalias

Durante a validação, foram tomadas decisões específicas para "Sanitizar" os dados:

1.  **Bug da "SUL AMERICA" (Zeros à Esquerda)**
    * **Problema:** O CSV da ANS trazia CNPJs como numéricos (`1685...` - 13 dígitos), causando falha na validação.
    * **Ação:** Implementação de `zfill(14)` no `DataEnricher`.
    * **Resultado:** Taxa de aprovação subiu de 3% para 99%.

2.  **Registro sem Match (Ghost)**
    * **Ação:** `Left Join`.
    * **Justificativa:** Mantém a despesa financeira mesmo se a operadora não for encontrada no cadastro ativo. Prioridade é o dado financeiro.

3.  **Duplicidade no Cadastro**
    * **Ação:** Deduplicação prévia por `REGISTRO_OPERADORA`.
    * **Justificativa:** Evita a explosão de linhas (Produto Cartesiano) no join 1:N.

###  Estratégia de Agregação e Ordenação

Para a geração do relatório final, optou-se pelo uso de **Processamento em Memória (In-Memory)** utilizando a biblioteca Pandas.

* **Estratégia:** O cálculo de métricas (Soma, Desvio Padrão e Média Trimestral) e a ordenação final foram realizados carregando o dataset consolidado na memória RAM.
* **Algoritmo de Ordenação:** Utilizou-se o método `sort_values` do Pandas, que implementa uma variação otimizada do *Quicksort* (Complexidade média $O(N \log N)$).
* **Justificativa do Trade-off:**
    * **Volume de Dados vs. Complexidade:** O volume total de dados processados resulta em um dataframe de baixo consumo de memória (< 100MB). Implementar algoritmos de ordenação externa (*External Merge Sort*) ou utilizar processamento distribuído (Spark) adicionaria complexidade de infraestrutura desnecessária (*Over-engineering*) para o escopo atual.
    * **Performance:** A operação em memória elimina o *overhead* de I/O de disco, resultando em um tempo de execução de milissegundos para a etapa de agregação.

---

## �️ Banco de Dados e Queries Analíticas

Esta seção documenta as decisões de modelagem do banco de dados (`sql/ddl_schema.sql`), a estratégia de importação (`sql/import_data.sql`) e as queries analíticas (`sql/queries_analytics.sql`).

### Modelagem do Esquema (DDL)

#### **Decisão 13: Normalização — Star Schema (Opção B)**

> **Trade-off técnico:** Tabela desnormalizada (Opção A) vs. Tabelas normalizadas separadas (Opção B).

Optou-se pela **Opção B: Tabelas normalizadas** seguindo o modelo *Star Schema*, com prefixos `dim_` (dimensão) e `fact_` (fato):

| Tabela | Tipo | Função |
| :--- | :--- | :--- |
| `dim_operadoras` | Dimensão | Dados cadastrais únicos por operadora |
| `fact_despesas_eventos` | Fato | Registros financeiros trimestrais |

* **Justificativa:**
    * **Volume de dados esperado:** A tabela fato cresce a cada trimestre, enquanto a dimensão é estável. Duplicar dados cadastrais em cada linha da fato seria desperdício.
    * **Frequência de atualizações:** Dados cadastrais (razão social, UF) mudam raramente. Com normalização, uma atualização no cadastro propaga automaticamente para todas as queries sem alterar a fato.
    * **Complexidade das queries:** O `JOIN` entre `dim` e `fact` via `reg_ans` é simples e indexado, sem impacto perceptível em performance.
* **✅ Prós:** Eliminação de redundância, integridade referencial via Foreign Keys, e atualização única de dados cadastrais.
* **⚠️ Contras:** Requer `JOIN` em todas as queries analíticas (custo negligível para este volume).

#### **Decisão 14: Tipos de Dados — Precisão vs. Performance**

> **Trade-off técnico:** Para valores monetários: `DECIMAL` vs `FLOAT` vs `INTEGER` (centavos). Para datas: `DATE` vs `VARCHAR` vs `TIMESTAMP`.

**Valores Monetários → `NUMERIC(18,2)`**
* **Justificativa:** Dados financeiros exigem precisão exata. `FLOAT`/`DOUBLE` usam representação binária de ponto flutuante (IEEE 754) e introduzem erros de arredondamento silenciosos (ex: `0.1 + 0.2 = 0.30000000000000004`). `INTEGER` em centavos é válido, mas exige conversão constante na camada de aplicação e dificulta a leitura direta no banco.
* **✅ Prós:** Zero perda de precisão. Operações `SUM()` e `AVG()` retornam valores exatos.
* **⚠️ Contras:** Ligeiramente mais lento que `FLOAT` em operações massivas, mas irrelevante para o nosso volume.

**Datas → `DATE`**
* **Justificativa:** Permite operações temporais nativas como `MIN()`, `MAX()` e comparações por igualdade (`WHERE data_trimestre = ...`), essenciais para a lógica de *Snapshot Final* e pivot trimestral. `VARCHAR` exigiria conversão em cada query e impediria ordenação/comparação correta. `TIMESTAMP` adicionaria hora/minuto/segundo sem utilidade para dados trimestrais.
* **✅ Prós:** Indexação eficiente, comparações nativas e conversão via `STR_TO_DATE()` na importação.
* **⚠️ Contras:** Nenhum significativo para este contexto.

#### **Decisão 15: Estratégia de Indexação**

Foram criados índices específicos para otimizar as queries analíticas mais frequentes:

| Índice | Coluna | Justificativa |
| :--- | :--- | :--- |
| `idx_operadoras_cnpj` | `dim_operadoras.cnpj` | Preparado para extensibilidade (lookups futuros por CNPJ). Não utilizado nas queries atuais. |
| `idx_despesas_data` | `fact_despesas_eventos.data_trimestre` | Acelera `MIN()`, `MAX()` e comparações por igualdade (`WHERE data_trimestre = ...`) |
| `idx_despesas_reg` | `fact_despesas_eventos.reg_ans` | `JOIN` com a dimensão e agrupamentos (`GROUP BY`) |

* **Justificativa:** Sem índices, toda query analítica faria *Full Table Scan*. Com o crescimento da tabela fato, isso se tornaria inviável.
* **⚠️ Trade-off:** Índices aceleram leituras (`SELECT`) mas desaceleram escritas (`INSERT`). Como a importação ocorre em *batch* (uma vez por trimestre), o custo de escrita é aceitável.

---

### Importação de Dados (ETL SQL)

#### **Decisão 16: Staging Tables (Tabelas Temporárias)**

Utilizou-se a abordagem de **tabelas temporárias** (`CREATE TEMPORARY TABLE`) como área de *staging* para receber os dados brutos do CSV antes de transformá-los e inseri-los nas tabelas finais.

* **Justificativa:** Os CSVs da ANS contêm dados em formatos incompatíveis com o schema final (datas como `VARCHAR`, valores monetários com vírgula, colunas extras desnecessárias).
* **✅ Prós:** Permite transformação (`STR_TO_DATE`, `REPLACE`, `CAST`) em SQL puro sem dependência de ferramentas externas.
* **⚠️ Contras:** Consome memória temporária do servidor durante a importação.

#### **Decisão 17: Mapeamento Completo de Colunas (vs. `@dummy`)**

A tabela `temp_operadoras` reflete **todas as 20 colunas** do CSV original (`Relatorio_cadop.csv`), mesmo que apenas 5 sejam utilizadas.

* **Justificativa (Clean Code):** Usar `@dummy` para 15 colunas torna o código ilegível e impossível de auditar. Com o mapeamento completo, o script funciona como **documentação viva** da estrutura do arquivo fonte.
* **✅ Prós:** Autodocumentado, extensível (se precisar de uma nova coluna, basta adicioná-la ao `INSERT`).
* **⚠️ Contras:** A tabela temporária ocupa mais memória, mas é descartada imediatamente após o uso.

#### **Análise Crítica: Tratamento de Inconsistências na Importação**

| Inconsistência | Estratégia | Justificativa |
| :--- | :---: | :--- |
| **Valores NULL em campos obrigatórios** | Rejeição via `WHERE` | Registros sem `reg_ans` são filtrados pelo `WHERE reg_ans IN (...)`, pois `NULL` nunca satisfaz a condição `IN`. Garante integridade referencial. |
| **Strings em campos numéricos** | Conversão com `REPLACE` + `CAST` | A vírgula decimal (`1234,56`) é convertida para ponto (`1234.56`) via `REPLACE`. Se a conversão falhar, o `CAST` retorna `NULL`, isolando o erro sem quebrar o batch. |
| **Datas em formatos inconsistentes** | `STR_TO_DATE` com formato explícito | Força o padrão `%Y-%m-%d`. Datas fora deste formato retornam `NULL` e são tratadas pelo `NOT NULL` constraint na tabela final, rejeitando o registro. |
| **Operadoras duplicadas no CSV** | `INSERT IGNORE` + `SELECT DISTINCT` | O `DISTINCT` elimina duplicatas na leitura; o `INSERT IGNORE` garante idempotência caso a mesma operadora já exista na tabela. |
| **Encoding incorreto** | `CHARACTER SET 'utf8'` | Forçamos UTF-8 na leitura para preservar acentos e caracteres especiais. |

---

### Queries Analíticas

#### **Decisão 18: Exclusão de Operadoras com Valores Zero**

> **Desafio:** Considerar operadoras que podem não ter dados em todos os trimestres.

Na Query 1 (Crescimento %), optou-se por **excluir operadoras cujo valor no primeiro trimestre é zero** (`WHERE q1_ytd > 0`).

* **Justificativa:** Valor zero no trimestre inicial indica inatividade ou ausência de registro. Calcular crescimento percentual a partir de zero resultaria em divisão por zero ou crescimento infinito, distorcendo o ranking.
* **✅ Prós:** Resultados matematicamente válidos e representativos do mercado ativo.
* **⚠️ Contras:** Operadoras que **iniciaram** atividade durante o período analisado não aparecem no ranking de crescimento, mesmo que tenham valores expressivos no último trimestre.

> **Limitação conhecida — Continuidade de dados:** A query não valida se há dados intermediários entre o primeiro e o último trimestre. Uma operadora com dados apenas em Q1 e Q3 (sem Q2) teria seu crescimento calculado normalmente, mesmo que o gap indique suspensão de operações, fusão/cisão ou problemas de qualidade de dados. Para o escopo atual (3 trimestres, dados regulados da ANS), o risco é baixo. Em um sistema de produção com histórico de 5+ anos, seria recomendável implementar um filtro de completude mínima (ex: dados em ≥ 80% dos trimestres esperados).

#### **Decisão 19: Otimização de Subqueries — CTE `DateBounds`**

Na Query 1, as subqueries `SELECT MIN(data_trimestre)` e `SELECT MAX(data_trimestre)` eram executadas **inline** dentro de expressões `CASE WHEN`, potencialmente recalculadas para cada linha do `GROUP BY`.

Refatorou-se para uma **CTE preliminar** (`DateBounds`) que calcula os limites uma única vez e é referenciada via `CROSS JOIN`.

* **Justificativa:** Boa prática de performance independente do volume. Elimina scans redundantes na tabela fato.
* **✅ Prós:** O otimizador calcula MIN/MAX uma vez; código mais limpo e explícito.
* **⚠️ Contras:** Nenhum. O `CROSS JOIN` com uma CTE de 1 linha não adiciona custo.

#### **Decisão 20: CTEs com Flags (vs. Window Functions vs. Subqueries)**

> **Trade-off técnico:** A Query 3 (Operadoras acima da média em ≥ 2 trimestres) pode ser resolvida com diferentes abordagens.

Optou-se pela **Abordagem A: CTEs (Common Table Expressions) com Flags**, ao invés de Window Functions ou Subqueries correlacionadas.

| Critério | CTEs (Escolhida) | Window Functions | Subqueries |
| :--- | :---: | :---: | :---: |
| **Legibilidade** | ⭐⭐⭐ | ⭐⭐ | ⭐ |
| **Manutenibilidade** | ⭐⭐⭐ | ⭐⭐ | ⭐ |
| **Performance** | ⭐⭐ | ⭐⭐⭐ | ⭐ |



* **Justificativa:**
    * **Legibilidade:** Cada CTE (`MarketAverage`, `OperatorYTD`, `AboveAverage`) tem uma responsabilidade única e nomeada, funcionando como "etapas" de um pipeline lógico.
    * **Manutenibilidade:** Alterar o limiar de 2 para 3 trimestres exige mudar apenas `HAVING SUM(...) >= 3`.
    * **Performance:** Window Functions leem a tabela fato apenas 1 vez, mas a clareza do código foi priorizada.

---

### 🐛 A Anomalia dos "71 Bilhões" (e a Solução SQL)

**O Problema:**
Ao realizar a primeira agregação dos arquivos trimestrais (`1T2025.csv`, `2T2025.csv`, `3T2025.csv`), o pipeline reportou um total de despesas para a operadora *Bradesco Saúde* superior a **R$ 71 Bilhões**. Este valor representava uma anomalia estatística quando comparado ao histórico de mercado da empresa.

**Investigação (RCA - Root Cause Analysis):**
A análise exploratória revelou que os arquivos de Demonstrações Contábeis da ANS seguem o regime de **competência acumulada (Year-to-Date / YTD)**.
* *Evidência:* A conta `411111061` (Despesas com Eventos) apresentava saldo crescente sem "zerar" a cada trimestre.
* *Erro Original:* A estratégia inicial de somar (`SUM`) os valores de todos os arquivos resultava na duplicação (e triplicação) dos saldos dos primeiros meses do ano.

**Solução Implementada (Estratégia Híbrida):**
Para garantir a integridade dos números tanto na agregação Python quanto na análise SQL, adotamos abordagens complementares:

1.  **No Pipeline Python (Data Aggregator):**
    Utilizamos a **Snapshot Strategy** para métricas de volume. O código filtra o dataset pelo `MAX(DATA)` (último trimestre) antes de realizar a soma, corrigindo o total exportado para o valor real de **~R$ 36.5 Bilhões**.

2.  **Nas Queries SQL (Analytics):**
    Implementamos a lógica de **Desacumulação Incremental** diretamente no banco de dados para análises temporais:
    *   **Crescimento Real (Query 1):** Ao invés de comparar acumulados (que distorceriam o percentual), extraímos o valor real do trimestre:
        *   `Q3_Real = Q3_YTD - Q2_YTD`
        *   `Crescimento = (Q3_Real - Q1_Real) / Q1_Real`
    *   **Volume Total (Query 2):** Utilizamos o *Snapshot Final* (`MAX(data_trimestre)`), alinhado com a lógica do Python.

3.  **Validação Automatizada:**
    Criamos o script `sql/validate.sql` que verificou a hierarquia de contas, confirmando que os dados consolidados são puramente **Level 9 (Analíticos/Leaf)**, eliminando a hipótese de dupla contagem por hierarquia.

> **Status:** ✅ Resolvido em Python (`fix/aggregation-ytd-logic`) e SQL (`feat/ytd-analytics`).

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
    * Gera o arquivo final compactado `Teste_{Nome}.zip`.

---

## 🚀 Como Executar o Projeto

Siga os passos abaixo para rodar o pipeline em seu ambiente local.

### Pré-requisitos
* **Python 3.8+** instalado.
* **Git** instalado.

### Passo a Passo

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/LeonardRuhmann/IntuitiveCare-Teste.git](https://github.com/LeonardRuhmann/IntuitiveCare-Teste.git)
    cd IntuitiveCare-Teste
    ```

2.  **Crie e ative um ambiente virtual (Recomendado):**
    * *Linux/Mac:*
        ```bash
        python3 -m venv venv
        source venv/bin/activate
        ```
    * *Windows:*
        ```bash
        python -m venv venv
        .\venv\Scripts\activate
        ```

3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Execute o Pipeline:**
    ```bash
    python -m src.main
    ```

5.  **Verifique os Resultados:**
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

* **Estratégia:** O cálculo de métricas (Soma, Desvio Padrão e Média Trimestral) e a ordenação final foram realizados carregando o dataset consolidado (~50k registros) na memória RAM.
* **Algoritmo de Ordenação:** Utilizou-se o método `sort_values` do Pandas, que implementa uma variação otimizada do *Quicksort* (Complexidade média $O(N \log N)$).
* **Justificativa do Trade-off:**
    * **Volume de Dados vs. Complexidade:** O volume total de dados processados resulta em um dataframe de baixo consumo de memória (< 100MB). Implementar algoritmos de ordenação externa (*External Merge Sort*) ou utilizar processamento distribuído (Spark) adicionaria complexidade de infraestrutura desnecessária (*Over-engineering*) para o escopo atual.
    * **Performance:** A operação em memória elimina o *overhead* de I/O de disco, resultando em um tempo de execução de milissegundos para a etapa de agregação.
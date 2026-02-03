# IntuitiveCare-Teste - Documentação Técnica

## 1. Pipeline de Extração e Transformação (ETL)

### 1.1. Acesso à API e Arquitetura

#### **Decisão 1: Separação de Responsabilidades (`Services`)**
Criei uma pasta `services` e a classe `AnsDataClient` para isolar a lógica de conexão.
* **Justificativa:** Evita misturar a lógica de conexão com o fluxo principal.
* **✅ Prós:** Código desacoplado, fácil manutenção e escalabilidade.
* **⚠️ Contras:** Aumenta ligeiramente a complexidade da estrutura de pastas para um projeto pequeno.

#### **Decisão 2: Ferramenta de Scraping (`BeautifulSoup4` vs `Selenium`)**
Optei pela biblioteca `BeautifulSoup4` (BS4) em conjunto com `Requests` ao invés de Selenium ou Regex puro.
* **Justificativa (KISS):** O site da ANS é estático. Usar Selenium seria *overkill* (pesado e lento). Regex puro seria frágil para tags HTML.
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

---

### 1.2. Processamento e Transformação

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

### 1.3. Consolidação e Limpeza

#### **Decisão 8: Tratamento de Valores Zerados vs. Negativos**
Remoção física de registros com valor `0.0`.
* **Justificativa:** Registros zerados indicam inatividade e geram "ruído".
* **✅ Prós:** Redução drástica do volume de dados sem perda de informação.
* **⚠️ Contras:** Perde-se o histórico de que a conta existiu naquele trimestre (embora inativa).

> **Nota sobre Negativos:** Valores negativos foram **mantidos**, pois representam estornos ou ajustes contábeis legítimos.

#### **Decisão 9: Parser de Datas (`Pandas` vs `Regex`)**
Uso de `pd.to_datetime(errors='coerce')` em vez de Regex manual.
* **Justificativa:** *Fail-safe*. O método nativo gerencia variações (`/` ou `-`) automaticamente.
* **✅ Prós:** Robustez. Dados inválidos viram `NaT` sem quebrar o pipeline.
* **⚠️ Contras:** Menos controle granular sobre formatos exóticos (mas suficiente para o padrão ANS).

---

## 2. Enriquecimento e Qualidade de Dados

### 2.1. Validação (Quarantine Pattern)

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

### 2.2. Enriquecimento de Dados (Cadastral)

#### **Decisão 11: Estratégia de Join (In-Memory)**
Para cruzar dados financeiros e cadastrais, optou-se pelo processamento em memória com Pandas, diferindo da extração incremental.
* **Justificativa:** O dataset final consolidado (~18k linhas) é pequeno o suficiente para memória RAM.
* **✅ Prós (KISS):** Simplicidade e rapidez de implementação. Frameworks distribuídos (Spark) seriam *over-engineering*.
* **⚠️ Contras:** Se o dataset final crescesse para a casa dos Gigabytes, essa etapa precisaria ser refatorada para chunks.

#### **Decisão 12: Chave de Ligação (`REG_ANS`)**
Uso de `REG_ANS` como chave primária de join, ao invés do CNPJ solicitado.
* **Justificativa (Realidade vs Requisito):** Os arquivos financeiros brutos **não continham CNPJ**, apenas `REG_ANS`.
* **✅ Prós:** Viabilizou o enriquecimento. O CNPJ foi trazido da base cadastral para a financeira.
* **⚠️ Contras:** Dependência da qualidade da base cadastral da ANS.

---

### 2.3. Resolução de Anomalias

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
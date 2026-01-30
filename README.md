# IntuitiveCare-Teste

1.1. Acesso à API de Dados Abertos da ANS
Arquitetura e Organização: Criei uma pasta services e a classe AnsDataClient para garantir a Separação de Responsabilidades. Isso evita misturar a lógica de conexão com o fluxo principal, tornando o código mais fácil de manter e escalar.

Escolha de Ferramentas (KISS): Escolhi a biblioteca BeautifulSoup4 (BS4) ao invés de alternativas como Selenium ou Regex puro.

Por que não Selenium? O Selenium é muito pesado para uma página estática e violaria o princípio KISS (Keep It Simple).

Por que não Regex puro? Regex é frágil para fazer parsing de tags HTML e poderia quebrar facilmente com mudanças no site. O BS4 é a solução mais equilibrada.

Pensamento Crítico (Regex): Como os nomes dos arquivos variam bastante entre os anos (ex: 1T2025 vs dados_2023_1_trim), precisei desenvolver uma Expressão Regular (Regex) robusta que atendesse a todos os casos sem "hardcode". Essa foi a parte mais desafiadora desta etapa (ver método _detect_quarter em src/services/ans_cliente.py).

Performance: No download, priorizei a eficiência de recursos utilizando stream=True e shutil.copyfileobj. Essa abordagem processa o download em partes (chunks), evitando carregar arquivos ZIP grandes inteiros na memória RAM. Isso previne que a aplicação crashe em servidores com memória limitada.

1.2. Processamento e Transformação de Dados
Estratégia de Processamento: Optei pela abordagem Incremental. O script main.py itera sobre os arquivos baixados, processando um arquivo ZIP por vez e descartando os dados brutos da memória após a extração das linhas de interesse.

Justificativa: Embora carregar tudo em memória fosse mais rápido para volumes pequenos, a abordagem incremental é mais escalável. Se a ANS liberar arquivos de 10GB no futuro, essa solução continuará funcionando sem estourar a memória, enquanto a abordagem de processar todos de uma vez falharia.

Identificação e Filtros: Implementei uma classe ZipProcessor que inspeciona o conteúdo do ZIP sem extraí-lo para o disco (usando zipfile em memória).

Normalização: O código realiza a leitura forçada em UTF-8 para garantir a integridade de acentos e caracteres especiais (evitando problemas de encoding comuns em dados governamentais). Além disso, normaliza colunas numéricas convertendo o formato brasileiro (1.000,00) para ponto flutuante padrão (1000.0).

Filtragem de Negócio: Após análise dos dados reais, apliquei um filtro preciso na coluna DESCRICAO buscando a string "Despesas com Eventos/Sinistros", garantindo que apenas os dados solicitados pelo teste sejam persistidos.
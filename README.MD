# Economic Indices Data Pipeline

Este projeto implementa um pipeline de dados que coleta, processa e armazena índices econômicos (ICC - Índice de Confiança do Consumidor e ICF - Índice de Intenção de Consumo das Famílias) da Fecomércio em um data warehouse no Google BigQuery.

## Arquitetura

O pipeline é composto por 3 camadas:

- **Raw**: Dados brutos extraídos diretamente das planilhas
- **Trusted**: Dados limpos e tipados corretamente
- **Refined**: Dados agregados e transformados para análise

## Pré-requisitos

- Docker e Docker Compose instalados
- Uma conta de serviço do Google Cloud Platform com acesso ao BigQuery
- Arquivo de credenciais da conta de serviço (service-account.json)

## Configuração

1. Clone este repositório
2. Coloque o arquivo `service-account.json` na raiz do projeto
3. Execute o pipeline com:

```bash
docker compose up --build
```

## Estrutura do Projeto

```bash
├── app/
│ ├── main.py # Script principal
│ └── requirements.txt # Dependências Python
├── Dockerfile
├── docker-compose.yml
└── service-account.json # Credenciais GCP (não versionado)
```

## Funcionamento

O pipeline:
1. Acessa os sites da Fecomércio
2. Baixa as planilhas de ICC e ICF
3. Processa os dados em 3 camadas (raw, trusted, refined)
4. Armazena os resultados no BigQuery

## Segurança

- O arquivo `service-account.json` contém credenciais sensíveis e não deve ser versionado
- As credenciais são montadas como volume somente leitura no container

## Regras aplicadas na tabela Trusted

- Tipagem e conversão de dados:
- As colunas foram convertidas para os tipos de dados apropriados, como FLOAT64 para valores numéricos, DATE para datas e TIMESTAMP para carimbos de tempo.
Valores nulos e inválidos (como NaN) foram tratados para evitar inconsistências. Valores numéricos inválidos foram substituídos por 0 ou NULL, dependendo do contexto.

- Sanitização de dados:

- Os nomes das colunas foram sanitizados para manter um padrão compatível com o BigQuery (uso de letras minúsculas, substituição de caracteres especiais por "_", etc.).
- Apenas os dados relevantes foram mantidos, descartando registros e colunas desnecessárias.

## Regras aplicadas na tabela Refined

- Agregação temporal:

- Os dados foram agregados por ano_mes, combinando diferentes períodos em uma única chave para facilitar análises mensais.
Cálculo de variação percentual:

- Foram calculadas variações percentuais nos índices ICC e ICF com base no mês anterior, considerando valores válidos.
Em caso de ausência de dados no mês anterior, o resultado da variação foi definido como NULL.
Enriquecimento de dados:

- Dados de ICC e ICF foram combinados em uma única tabela para fornecer uma visão unificada dos índices.
Adicionada uma coluna de carimbo de tempo (load_timestamp) para rastreamento da última atualização dos dados.

## Decisões técnicas tomadas

- Arquitetura em três camadas (Raw, Trusted e Refined):
Optou-se por uma arquitetura modular para separar dados brutos, limpos e transformados, garantindo maior flexibilidade e clareza nos processos de ETL.

- Tratamento de dados nulos e inválidos:
Para garantir a consistência das análises, dados nulos e inválidos foram substituídos por valores padrão (0 ou NULL), dependendo do contexto.

- Automatização de download com Selenium:
O uso do Selenium permitiu a automação do download de planilhas diretamente do site da Fecomércio, reduzindo o esforço manual.

- Sanitização de nomes de colunas:
Para evitar problemas de compatibilidade com o BigQuery, os nomes das colunas foram sanitizados, garantindo conformidade com os padrões exigidos.

- Adoção de UUID para datasets:
Foi implementado um identificador único para os datasets, permitindo múltiplas execuções do pipeline sem sobrescrever dados previamente armazenados.

- Cálculo de métricas derivadas na camada Refined:
A lógica de cálculos foi centralizada na camada Refined para facilitar futuras atualizações e garantir a consistência das análises.

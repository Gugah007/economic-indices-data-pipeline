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

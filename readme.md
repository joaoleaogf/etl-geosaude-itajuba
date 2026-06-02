# ETL GeoSaúde Itajubá

> Pipeline de ETL em TypeScript que limpa, geocodifica e prepara dados hospitalares para análise espacial em **PostGIS** e **QGIS** — base do projeto *Padrões Espaciais da Saúde Itajubá-MG*.

![TypeScript](https://img.shields.io/badge/TypeScript-5.9-3178C6?logo=typescript&logoColor=white)
![Node.js](https://img.shields.io/badge/Node.js-≥18-339933?logo=node.js&logoColor=white)
![PostGIS](https://img.shields.io/badge/PostGIS-ready-336791?logo=postgresql&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-E0A458)

---

## 🔒 Privacidade dos dados (LGPD)

Este repositório contém **apenas o código** do pipeline. **Nenhum dado real de pacientes** (nomes, endereços, CIDs, prontuários) é versionado aqui — esses dados são pessoais sensíveis (LGPD, Art. 5º, II) e ficam fora do controle de versão (veja o [`.gitignore`](.gitignore)).

Para rodar o ETL, forneça seu próprio arquivo de entrada no formato esperado (veja [Formato de entrada](#formato-de-entrada)).

---

## Visão geral

O pipeline integra e padroniza dados hospitalares, demográficos e socioeconômicos para análises de distribuição espacial de atendimentos em Itajubá-MG. Ele realiza:

- **Limpeza e padronização** de dados brutos (endereços, CIDs, convênios, faixas etárias);
- **Geocodificação resiliente** dos endereços, com *fallback* entre provedores (LocationIQ, Geoapify, OpenCage, Nominatim) e tratamento seguro de respostas 404;
- **Deduplicação de endereços** e consolidação de latitude/longitude;
- **Conversão** dos resultados para formatos compatíveis com **PostGIS**;
- **Checkpoints e logs** detalhados para retomada e auditoria.

A saída serve de base para identificar *hotspots*/*coldspots* de saúde pública, correlações demográficas (dados IBGE) e desigualdades de acesso.

## Arquitetura do pipeline

```
entrada (JSON export)
        │
        ▼
┌─────────────────────────────┐
│ Stage 1 — Normalização      │  → patients.ndjson
│ limpeza, schema, checkpoint │    attendances.ndjson
└─────────────────────────────┘    postgis_ready.csv
        │
        ▼
┌─────────────────────────────┐
│ Stage 2 — Endereços + Geo   │  → endereços deduplicados
│ dedup, geocodificação       │    com lat/long
└─────────────────────────────┘
        │
        ▼
   import.sql → PostGIS (attendances_geocoded) → QGIS
```

| Arquivo | Responsabilidade |
|---|---|
| `script.ts` | Orquestra os stages, CLI e schema de saída |
| `geocoding.ts` | Geocodificação com fallback entre provedores |
| `tokenManager.ts` | Rotação/uso de tokens de API por provedor |
| `jsonParser.ts` | Parsing resiliente do export JSON de entrada |
| `utils.ts` | Normalização de endereços, datas e campos |

## Stack

`TypeScript` · `Node.js` · `ts-node` · `axios` · `PostGIS` · `QGIS` · APIs de geocodificação

## Como rodar

```bash
# 1. Instale as dependências
npm install

# 2. Execute as duas etapas em sequência
npx ts-node script.ts ./seu_export.json ./output --geocode
```

Ou rode os estágios separadamente:

```bash
# Stage 1 — normalização e arquivos para PostGIS
npx ts-node script.ts stage1 ./seu_export.json ./output

# Stage 2 — consolidação de endereços e coordenadas
npx ts-node script.ts stage2 ./output/patients.ndjson ./output --geocode
```

O Stage 1 mantém `stage1.checkpoint.json` e **retoma automaticamente** após interrupções, reaproveitando os arquivos já gravados. Use `--patients=/caminho/custom.ndjson` para informar uma origem específica no Stage 2.

### Variáveis de ambiente

| Variável | Descrição |
|---|---|
| `GEOCODER_USER_AGENT` | User-Agent enviado aos provedores de geocodificação |
| `GEOCODER_ENABLE_NOMINATIM` | `true` para habilitar o Nominatim como provedor |

As chaves de API dos provedores são lidas de um arquivo local de tokens, **não versionado**.

### Formato de entrada

O pipeline espera um export JSON de atendimentos com, entre outros, os campos:
`nr_atendimento`, `dt_entrada`, `ds_convenio`, `ds_setor_atendimento`, `nm_paciente`,
`dt_nascimento`, `ie_sexo`, `ds_endereco`, `cd_cep`, `ds_bairro`, `cd_cid_principal`, `ds_motivo_alta`.

> ⚠️ Use sempre dados anonimizados/autorizados. Nunca faça commit de dados reais de pacientes.

### Importação no PostGIS

```psql
\set csv_file '/caminho/absoluto/para/postgis_ready.csv'
\i output/import.sql
```

O script cria a tabela `attendances_geocoded`, remove estruturas antigas e prepara índices espaciais. A instrução `\copy` vem comentada para permitir ajustes de caminho.

## Perguntas de pesquisa

O ETL alimenta análises espaciais que respondem, entre outras:

- Padrões de *hotspots*/*coldspots* da frequência de atendimentos;
- Correlação entre densidade demográfica (IBGE) e taxa de atendimentos;
- Relação entre distribuição espacial e variáveis socioeconômicas (grau de instrução, estado civil);
- Padrões por faixa etária, tipo de convênio (SUS × particular), nível de urgência e diagnósticos (CID);
- Diferenças espaciais por motivo de alta (alta, transferência, óbito).

A lista completa está em [`# Perguntas de Pesquisa Exploratórias.md`](./%23%20Perguntas%20de%20Pesquisa%20Explorat%C3%B3rias.md).

## Equipe

Projeto acadêmico colaborativo:

- **João Leão** — ETL, limpeza e geocodificação dos dados (este repositório)
- **Juliana** — carga no PostGIS (atendimentos + IBGE)
- **Hiara** — projeto QGIS (camadas de Itajubá, setores censitários, ruas)
- **Elisa** — escolha de técnicas e indicadores por questão de pesquisa

## Licença

[MIT](LICENSE) © João Leão

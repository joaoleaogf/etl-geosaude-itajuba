# Padrões Espaciais da Saúde Itajubá


Pipeline de **ETL (Extração, Transformação e Carga)** desenvolvido para o projeto  **Padrões Espaciais da Saúde Itajubá** , com foco em integrar e preparar dados hospitalares, demográficos e socioeconômicos para análises espaciais no **PostGIS** e  **QGIS** .

O sistema realiza:

* Limpeza e padronização de dados brutos (endereços, CIDs, convênios, etc.);
* **Geocodificação** dos endereços dos pacientes e cruzamento com setores censitários do IBGE;
* Conversão dos resultados para formatos geoespaciais compatíveis com o  **PostGIS** ;
* Geração de logs detalhados de execução e registro de falhas para auditoria.

O ETL serve como base para as análises de distribuição espacial de atendimentos, correlações demográficas e identificação de hotspots e coldspots de saúde pública no município de  **Itajubá-MG** .

## Execução do ETL em duas etapas

O pipeline pode ser executado em duas fases distintas:

1. **Stage 1 – Normalização e arquivos para PostGIS**

   ```bash
   npx ts-node script.ts stage1 ./TABLE_EXPORT_DATA.json ./output
   ```

   Gera `patients.ndjson`, `attendances.ndjson` e `postgis_ready.csv`, consolidados para importação no PostGIS.
   A etapa mantém um arquivo `stage1.checkpoint.json` com o número de registros concluídos e retoma automaticamente em caso de
   interrupções, reaproveitando os arquivos já gravados.

2. **Stage 2 – Consolidação de endereços e coordenadas**

   ```bash
   npx ts-node script.ts stage2 ./output/patients.ndjson ./output
   ```

   Deduplica endereços, agrega número de ocorrências e consolida latitude/longitude (quando disponível), podendo acionar geocodificação com `--geocode`.

Para executar as duas etapas em sequência basta omitir o subcomando:

```bash
npx ts-node script.ts ./TABLE_EXPORT_DATA.json ./output --geocode
```

Opcionalmente utilize `--patients=/caminho/personalizado.ndjson` para informar uma origem específica dos dados de pacientes na segunda etapa.

## Perguntas Focadas na Distribuição e Demografia Espacial

1. **Qual é o padrão de distribuição espacial (hotspots e coldspots) da frequência total de atendimentos hospitalares em Itajubá?**

   * A análise se baseia na geolocalização dos pacientes (endereço, CEP, bairro) e visa avaliar como essa distribuição se manifesta nos setores censitários ou bairros.
2. **Existe uma correlação espacial entre a densidade demográfica (dados IBGE) e a taxa de atendimentos hospitalares em diferentes setores censitários?**

   * Esta questão utiliza os dados do IBGE (densidade demográfica e indicadores por setor censitário) em conjunto com a frequência dos atendimentos.
3. **Como a distribuição espacial dos atendimentos se relaciona com características socioeconômicas dos pacientes, como o grau de instrução ou estado civil?**

   * Esta questão mapearia espacialmente variáveis demográficas como o grau de instrução (`ie_grau_instrucao`) e o estado civil para identificar se há concentrações geográficas de grupos específicos de pacientes.
4. **As faixas etárias mais vulneráveis (crianças, idosos – `nr_anos`) apresentam um padrão espacial de atendimento diferente em comparação com a população adulta?**

   * A idade do paciente (`nr_anos` ou `dt_nascimento`) pode ser mapeada espacialmente para verificar se setores censitários específicos têm uma proporção maior de atendimentos em grupos etários específicos.

---

## Perguntas Focadas no Tipo de Atendimento e Necessidade Clínica

5. **A distribuição espacial dos atendimentos difere significativamente entre os tipos de convênio (`ds_convenio`) utilizados (ex.: SUS versus Particular/Unimed)?**

   * É possível mapear a residência dos pacientes em função do seu convênio de saúde para entender se o uso do SUS ou planos particulares possui concentrações geográficas distintas.
6. **Quais são as áreas geográficas que concentram o maior número de atendimentos de urgência e emergência (`ds_nivel_urgencia`)?**

   * Esta análise exploraria se há disparidades espaciais na necessidade de cuidados críticos, mapeando o nível de urgência (como *Emergência* ou *Muito Urgente*) por setores censitários.
   * (Distribuição de ambulâncias)
7. **Existe uma concentração espacial de pacientes que buscaram atendimento em setores especializados, como Ortopedia ou Tomografia Computadorizada (`ds_setor_atendimento`)?**

   * Esta questão cruza a localização dos pacientes com o setor de atendimento para identificar se o acesso a determinadas especialidades ou exames diagnósticos é geograficamente desigual.
8. **Há padrões espaciais identificáveis para os principais diagnósticos/motivos de internação (`cd_cid_principal`) ou procedimentos (`ds_proc_principal`) realizados?**

   * Ao mapear as causas principais de atendimento (como *R51, J189, S934, M94* — exemplos de códigos CID) e os procedimentos, pode-se identificar *clusters* de doenças ou traumas específicos em certas regiões de Itajubá.
9. **Como a distribuição espacial de atendimentos por doenças específicas pode ser comparada com os dados de vacinação (Vacinação) no mesmo espaço geográfico?**

   * Esta é uma questão importante, pois o projeto prevê a inclusão de dados de vacinação, permitindo a correlação espacial entre a imunização de uma área e a incidência de doenças tratadas no hospital.

---

## Perguntas Focadas na Saída e Resultado

10. **A distribuição espacial dos pacientes que receberam “Alta do Pronto Socorro” difere daquelas que resultaram em “Transferido para outro estabelecimento” ou “Óbito” (`ds_motivo_alta`)?**

    * Mapear o motivo da alta pode ajudar a entender se determinadas áreas geográficas apresentam piores resultados de saúde ou se são fontes de pacientes que necessitam de transferência para centros mais especializados.

---

## Etapas do Projeto

1. **Fazer o código para limpar os dados. (João)**
   a. Fazer a geocodificação dos endereços.
   b. Criar um *Schema* do banco.
   c. Saída: Arquivos no formato aceito pelo **PostGIS**.
2. **Carregar os dados no PostGIS. (Juliana)**
   a. Carregar dados de atendimento.
   b. Carregar dados do IBGE.
3. **Criar Projeto no QGIS contendo. (Hiara)**
   a. Criar camadas de dados de Itajubá (ShapeFile Cidade, Setores Censitários e ruas).
   b. Criar camada de dados do IBGE.
   c. Criar camada de dados de atendimentos.
   d. Procurar ShapeFile de ruas.
4. **Avaliar quais técnicas são adequadas para responder a cada questão levantada. (Elisa)**
   a. Criar indicadores.

---

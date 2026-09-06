# Trabalho Final — Ciência de Dados II: Do Dado Bruto à Descoberta de Conhecimento

**Nome completo:** Luciane Siqueira Fernandes
**Matrícula:** 72500418

## Descrição do projeto

Pipeline de KDD (seleção, pré-processamento, transformação, mineração e interpretação) sobre dados
do CAR/SICAR (Cadastro Ambiental Rural), usando Apache Spark (PySpark) para processamento
distribuído. Combina um modelo preditivo (classificação da condição de cadastro do imóvel) e um
modelo descritivo (clusterização de perfis de propriedade rural).

## Dataset

- **Fonte:** Portal oficial do SICAR — https://consultapublica.car.gov.br/publico/estados/downloads
  (camada "Área do Imóvel", estado da Bahia). Baixado manualmente como shapefile (o download exige
  captcha) e convertida só a tabela de atributos (`.dbf`) para `.csv` com `src/leitor_dbf.py` — a
  geometria (`.shp`/`.shx`) não foi usada.
- **Volume:** 1.313.653 linhas × 12 colunas (CSV de ~184 MB) — bem acima do mínimo de 100 mil
  linhas exigido pelo enunciado.
- **Justificativa do recorte:** a Bahia sozinha já ultrapassa em mais de 13x o volume mínimo
  exigido; não foi necessário combinar mais estados.

## Estrutura do repositório

```
notebooks/   -> notebook(s) com o pipeline completo (Etapas 2 a 6)
data/raw/    -> dados originais baixados (ou instruções de download, se o arquivo for grande demais para o repo)
data/processed/ -> dados após pré-processamento (se for salvo em disco)
src/         -> funções auxiliares reaproveitadas entre notebooks, se houver
RELATORIO.md -> relatório estruturado pelas etapas do KDD
```

## Instruções de execução

1. [preencher: como instalar dependências — `pip install -r requirements.txt`]
2. [preencher: como obter/posicionar os dados brutos em `data/raw/`]
3. [preencher: como rodar o notebook do início ao fim]

## Ambiente

- Python + PySpark (Spark DataFrames, Spark SQL, MLlib)
- Bibliotecas de apoio: pandas, matplotlib, seaborn, scikit-learn

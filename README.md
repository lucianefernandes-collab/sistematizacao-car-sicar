# Sistematização — Ciência de Dados II: Do Dado Bruto à Descoberta de Conhecimento

**Nome completo:** Luciane Siqueira Fernandes
**Matrícula:** 72500418
**Professor:** Romes Heriberto Pires de Araujo

## Descrição do projeto

Pipeline de KDD (seleção, pré-processamento, transformação, mineração e interpretação) sobre dados
do CAR/SICAR (Cadastro Ambiental Rural), usando Apache Spark (PySpark) para processamento
distribuído. Combina um modelo preditivo (classificação da condição de cadastro do imóvel) e um
modelo descritivo (clusterização de perfis de propriedade rural).

## Dataset

- **Fonte:** Portal oficial do SICAR — https://consultapublica.car.gov.br/publico/estados/downloads
  (camada "Área do Imóvel", estado da Bahia). Baixado manualmente como shapefile (o download exige
  captcha).
- **Conversão:** a tabela de atributos (`.dbf`) foi extraída com um leitor próprio, escrito sem
  depender de geopandas/fiona/pyshp (`src/leitor_dbf.py`), executado via `src/etapa1_carregar_dbf.py`,
  que já converte para `.csv` e testa a carga no Spark. A geometria (`.shp`/`.shx`) não foi usada.
- **Volume:** 1.313.653 linhas × 12 colunas (CSV de ~184 MB) — bem acima do mínimo de 100 mil
  linhas exigido pelo enunciado.
- **Justificativa do recorte:** a Bahia sozinha já ultrapassa em mais de 13x o volume mínimo
  exigido; não foi necessário combinar mais estados.

## Estrutura do repositório

```
notebooks/ -> notebook(s) com o pipeline completo (Etapas 2 a 6)
data/raw/ -> dados originais baixados (ou instruções de download, se o arquivo for grande demais para o repo)
data/processed/ -> dados após pré-processamento (se for salvo em disco)
src/ -> leitor_dbf.py (parser de .dbf) e etapa1_carregar_dbf.py (conversão + teste de carga no Spark)
RELATORIO.md -> relatório estruturado pelas etapas do KDD
```


## Instruções de execução

1. **Instalar dependências:**
   - Java 17 (necessário para o PySpark rodar) — verifique com `java -version`.
   - Ambiente Python via Anaconda (testado com Python 3.11).
   - Instalar as bibliotecas: `pip install pyspark==3.5.1 pandas matplotlib seaborn scikit-learn`.
   - **Windows:** defina a variável de ambiente `PYSPARK_PYTHON` apontando para o mesmo
     interpretador Python usado pelo Jupyter, para evitar um bug conhecido nos workers do
     Spark no Windows. Exemplo, no Anaconda Prompt antes de abrir o Jupyter:
     `set PYSPARK_PYTHON=python`

2. **Obter e converter os dados brutos:**
   - Acesse https://consultapublica.car.gov.br/publico/estados/downloads, selecione o estado
     da Bahia e a camada "Área do Imóvel", resolva o captcha e baixe o shapefile.
   - Rode `python src/etapa1_carregar_dbf.py "caminho\para\AREA_IMOVEL.dbf"` — o script lê o
     `.dbf` com `src/leitor_dbf.py`, salva o `.csv` ao lado do arquivo original e testa a
     carga no Spark, imprimindo schema e total de linhas.
   - Posicione o `.csv` gerado em `data/raw/` (esta pasta não está versionada no repositório
     por causa do tamanho do arquivo — veja `.gitignore`).

3. **Rodar o notebook:**
   - Abra o Anaconda Navigator, inicie o Jupyter Notebook e abra `notebooks/pipeline_kdd.ipynb`.
   - Rode todas as células em ordem (Kernel → Restart & Run All), do início ao fim.

## Ambiente

- Python + PySpark (Spark DataFrames, Spark SQL, MLlib)
- Bibliotecas de apoio: pandas, matplotlib, seaborn, scikit-learn
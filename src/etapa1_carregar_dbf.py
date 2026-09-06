"""
Etapa 1 (seleção do dataset) — converte o .dbf do CAR/SICAR pra .csv e já
carrega no Spark pra conferir que leu certo.

Uso:
    python etapa1_carregar_dbf.py "caminho\\para\\AREA_IMOVEL.dbf"

(o leitor_dbf.py precisa estar na mesma pasta que este arquivo)
"""
import sys
import os

# --- mesma correção do teste de ambiente: garante que o Spark abre o worker
# usando este mesmo Python, evitando o erro "Connection reset" no Windows ---
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from leitor_dbf import ler_dbf  # noqa: E402


def main():
    if len(sys.argv) < 2:
        print('Uso: python etapa1_carregar_dbf.py "caminho\\para\\arquivo.dbf"')
        sys.exit(1)

    caminho_dbf = sys.argv[1]
    if not os.path.exists(caminho_dbf):
        print(f"Arquivo não encontrado: {caminho_dbf}")
        sys.exit(1)

    # --- 1) ler o .dbf com o leitor próprio (sem geopandas/fiona) ---
    print(f"Lendo {caminho_dbf} ...")
    df_pandas = ler_dbf(caminho_dbf)
    print()
    print("Shape (linhas, colunas):", df_pandas.shape)
    print()
    print("Colunas encontradas:", list(df_pandas.columns))
    print()
    print("Primeiras linhas:")
    print(df_pandas.head())
    print()
    print("Tipos de cada coluna:")
    print(df_pandas.dtypes)

    # --- 2) salvar como .csv, do lado do .dbf original ---
    caminho_csv = os.path.splitext(caminho_dbf)[0] + ".csv"
    df_pandas.to_csv(caminho_csv, index=False, encoding="utf-8")
    tamanho_mb = os.path.getsize(caminho_csv) / (1024 * 1024)
    print()
    print(f"Salvo em: {caminho_csv}  ({tamanho_mb:.1f} MB)")

    # --- 3) carregar esse .csv no Spark, só pra confirmar que está tudo ok ---
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("car-sicar-etapa1")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    df_spark = spark.read.csv(caminho_csv, header=True, inferSchema=True)
    print()
    print("--- Carregado no Spark ---")
    df_spark.printSchema()
    print("Total de linhas (Spark):", df_spark.count())
    df_spark.show(5)

    spark.stop()
    print("Etapa 1 concluída.")


if __name__ == "__main__":
    main()

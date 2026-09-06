"""
Leitor de arquivos .dbf (formato xBase/dBase III) sem depender de bibliotecas
externas (geopandas/fiona/pyshp não puderam ser instalados neste ambiente).

Serve para extrair só a TABELA DE ATRIBUTOS de um shapefile do SICAR/CAR,
descartando a geometria (.shp/.shx) — que é exatamente o que o plano de
trabalho pede antes de carregar no Spark (não precisamos dos polígonos,
só das colunas como cod_imovel, mod_fiscal, num_area, ind_status etc.).

Formato .dbf (xBase), domínio público / especificação aberta:
  Cabeçalho (32 bytes):
    byte 0        : versão
    bytes 1-3     : data da última atualização (AA, MM, DD)
    bytes 4-7     : número de registros (uint32 little-endian)
    bytes 8-9     : tamanho do cabeçalho em bytes (uint16 LE)
    bytes 10-11   : tamanho de cada registro em bytes (uint16 LE)
    bytes 12-31   : reservado
  Descritores de campo (32 bytes cada, terminados por 0x0D):
    bytes 0-10    : nome do campo (até 11 bytes, terminado em \\x00)
    byte 11       : tipo do campo ('C' texto, 'N' numérico, 'F' float,
                                    'D' data AAAAMMDD, 'L' lógico)
    bytes 12-15   : endereço do campo (não usado na leitura)
    byte 16       : tamanho do campo
    byte 17       : casas decimais
  Registros: 1 byte de flag de exclusão (' ' ativo, '*' excluído) seguido
  dos campos em largura fixa, na ordem dos descritores.

Uso:
    from leitor_dbf import ler_dbf
    df = ler_dbf("AREA_IMOVEL.dbf")
"""
import struct
from datetime import date


def _detectar_encoding(caminho_dbf):
    """Se existir um .cpg do lado do .dbf (comum em shapefiles), usa ele.
    Senão, tenta utf-8 e cai para latin-1 (padrão mais comum em dados
    públicos brasileiros mais antigos)."""
    caminho_cpg = caminho_dbf.rsplit(".", 1)[0] + ".cpg"
    try:
        with open(caminho_cpg, "r", encoding="ascii", errors="ignore") as f:
            nome = f.read().strip().upper()
        mapa = {
            "UTF-8": "utf-8", "UTF8": "utf-8",
            "ISO-8859-1": "latin-1", "8859-1": "latin-1", "ANSI 1252": "cp1252",
            "1252": "cp1252",
        }
        for chave, enc in mapa.items():
            if chave in nome:
                return enc
    except FileNotFoundError:
        pass
    return None


def ler_dbf_bytes(conteudo_bytes, encoding=None):
    """Lê os bytes de um .dbf e devolve (colunas, linhas) — linhas como lista
    de dicts já convertidos pro tipo Python correspondente (str/int/float/date/bool)."""
    if len(conteudo_bytes) < 32:
        raise ValueError("arquivo .dbf inválido: menor que o cabeçalho mínimo")

    n_registros = struct.unpack_from("<I", conteudo_bytes, 4)[0]
    tam_cabecalho = struct.unpack_from("<H", conteudo_bytes, 8)[0]
    tam_registro = struct.unpack_from("<H", conteudo_bytes, 10)[0]

    campos = []
    pos = 32
    while conteudo_bytes[pos:pos + 1] != b"\x0d" and pos < tam_cabecalho:
        nome = conteudo_bytes[pos:pos + 11].split(b"\x00")[0].decode("ascii", errors="replace")
        tipo = chr(conteudo_bytes[pos + 11])
        tamanho = conteudo_bytes[pos + 16]
        decimais = conteudo_bytes[pos + 17]
        campos.append({"nome": nome, "tipo": tipo, "tamanho": tamanho, "decimais": decimais})
        pos += 32

    encodings_tentar = [encoding] if encoding else ["utf-8", "latin-1"]

    linhas = []
    offset_registros = tam_cabecalho
    for i in range(n_registros):
        base = offset_registros + i * tam_registro
        if base >= len(conteudo_bytes):
            break
        flag = conteudo_bytes[base:base + 1]
        if flag == b"*":
            continue  # registro marcado como excluído
        registro = {}
        campo_pos = base + 1
        for campo in campos:
            bruto = conteudo_bytes[campo_pos:campo_pos + campo["tamanho"]]
            campo_pos += campo["tamanho"]
            registro[campo["nome"]] = _converter_campo(bruto, campo, encodings_tentar)
        linhas.append(registro)

    colunas = [c["nome"] for c in campos]
    return colunas, linhas


def _decodificar(bruto, encodings_tentar):
    for enc in encodings_tentar:
        try:
            return bruto.decode(enc)
        except UnicodeDecodeError:
            continue
    return bruto.decode("latin-1", errors="replace")


def _converter_campo(bruto, campo, encodings_tentar):
    tipo = campo["tipo"]
    if tipo == "C":
        return _decodificar(bruto, encodings_tentar).rstrip()
    if tipo in ("N", "F"):
        texto = bruto.decode("ascii", errors="replace").strip()
        if not texto or texto in ("-", "."):
            return None
        try:
            if campo["decimais"] > 0:
                return float(texto)
            return int(texto)
        except ValueError:
            try:
                return float(texto)
            except ValueError:
                return None
    if tipo == "D":
        texto = bruto.decode("ascii", errors="replace").strip()
        if len(texto) == 8 and texto.isdigit():
            try:
                return date(int(texto[0:4]), int(texto[4:6]), int(texto[6:8]))
            except ValueError:
                return None
        return None
    if tipo == "L":
        ch = bruto.decode("ascii", errors="replace").strip().upper()
        if ch in ("T", "Y"):
            return True
        if ch in ("F", "N"):
            return False
        return None
    # tipo não mapeado (ex.: 'M' memo) — devolve texto bruto decodificado
    return _decodificar(bruto, encodings_tentar).strip()


def ler_dbf(caminho):
    """Lê um arquivo .dbf do disco e devolve um pandas.DataFrame."""
    import pandas as pd
    encoding = _detectar_encoding(caminho)
    with open(caminho, "rb") as f:
        conteudo = f.read()
    colunas, linhas = ler_dbf_bytes(conteudo, encoding=encoding)
    return pd.DataFrame(linhas, columns=colunas)


if __name__ == "__main__":
    import sys
    df = ler_dbf(sys.argv[1])
    print(df.shape)
    print(df.head())
    print(df.dtypes)

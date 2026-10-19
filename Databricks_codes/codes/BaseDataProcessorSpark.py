'''
    Codigo desenvolvido por André Luiz Souza Santos
    contato: andrezluz19@gmail.com

    Versão revisada (out/2026):
    - Buffer de governança configurável, gravado em modo append (não sobrescreve exports anteriores)
    - Log de governança com uma linha por coluna (sem produto arquivo x coluna), inclusive para
      DataFrames vindos de SQL/set_df()
    - export_to_db grava TBLPROPERTIES de governança e preserva comentários de tabela/colunas
      entre execuções em modo overwrite
    - dbutils/display obtidos de forma compatível com import de módulo .py e com compute serverless
    - cache/checkpoint/parallel_read_csv/pivot_or_unpivot/fill_nan_based_on_dtype corrigidos
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id, regexp_replace
import os
import pandas as pd
from functools import reduce
import zipfile
import tarfile
import gzip
import shutil
import unicodedata
import re
from datetime import datetime
import getpass

# Caminho padrão do buffer de governança. Só é usado quando nem `buffer_log_path`
# nem `output_directory` são informados no construtor.
# Em Databricks com Unity Catalog, prefira um Volume: /Volumes/<catalogo>/<schema>/<volume>/export_log_buffer_.txt
BUFFER_PATH = "/insert_your_path_here/export_log_buffer_.txt"
BUFFER_FILENAME = "export_log_buffer_.txt"

# Colunas do buffer de governança (devem bater com o DataDictionaryBuilder)
GOVERNANCE_COLUMNS = [
    "catalog", "schema", "table", "column", "collumn_dtype", "creation_date", "creation_time",
    "table_description", "user", "file_fetched", "source_notebook_url"
]


# ============================================================================
# Utilitários de ambiente Databricks
# Quando a classe é importada como módulo (.py), as variáveis globais do notebook
# (dbutils, display, spark) NÃO ficam visíveis aqui. Estas funções as recuperam.
# ============================================================================

def _obter_dbutils():
    """Retorna o dbutils do Databricks, ou None fora do Databricks."""
    try:
        from databricks.sdk.runtime import dbutils  # disponível nos runtimes do Databricks
        return dbutils
    except Exception:
        pass
    try:
        import IPython
        return IPython.get_ipython().user_ns.get("dbutils")
    except Exception:
        return None


def _exibir(df, linhas=20):
    """Usa display() do Databricks quando disponível; senão, df.show()."""
    display_fn = None
    try:
        from databricks.sdk.runtime import display as display_fn
    except Exception:
        try:
            import IPython
            display_fn = IPython.get_ipython().user_ns.get("display")
        except Exception:
            display_fn = None
    try:
        if display_fn is not None:
            display_fn(df)
            return
    except Exception:
        pass
    df.show(linhas, truncate=False)


def _obter_contexto_notebook():
    """
    Retorna {'path': caminho do notebook, 'url': URL do notebook} da execução atual.
    Tenta primeiro a API compatível com serverless e depois o entry_point (clusters clássicos).
    """
    info = {"path": None, "url": None}
    host = None

    # 1) dbruntime (funciona em clusters clássicos e serverless)
    try:
        from dbruntime.databricks_repl_context import get_context
        ctx = get_context()
        info["path"] = getattr(ctx, "notebookPath", None)
        host = getattr(ctx, "browserHostName", None) or getattr(ctx, "workspaceUrl", None)
    except Exception:
        pass

    # 2) entry_point do dbutils (clusters clássicos)
    if not info["path"] or not host:
        try:
            dbu = _obter_dbutils()
            ctx = dbu.notebook.entry_point.getDbutils().notebook().getContext()
            if not info["path"]:
                info["path"] = ctx.notebookPath().get()
            if not host:
                try:
                    host = ctx.browserHostName().get()
                except Exception:
                    host = ctx.tags().get("browserHostName").get()
        except Exception:
            pass

    if info["path"] and host:
        host = str(host).replace("https://", "").rstrip("/")
        info["url"] = f"https://{host}/#workspace{info['path']}"
    return info


def _sql_str(valor):
    """Escapa um valor para uso dentro de uma string SQL com aspas simples."""
    return str(valor).replace("\\", "\\\\").replace("'", "\\'")


def _q(*partes):
    """Monta um identificador SQL com crases: `catalogo`.`schema`.`tabela`."""
    return ".".join(f"`{str(p).replace('`', '``')}`" for p in partes)


class BaseDataProcessorSpark:
    def __init__(self, input_directory=None, output_directory=None, catalog=None,
                 buffer_log_path=None, checkpoint_dir=None):
        """
        Inicializa uma instância da classe BaseDataProcessorSpark.

        Parâmetros:
        - input_directory: O diretório de entrada contendo os dados brutos.
            - Exemplo: '/Volumes/meu_catalogo/raw/arquivos/'
        - output_directory: O diretório de saída para os dados tratados.
            - Exemplo: '/Volumes/meu_catalogo/raw/saida/'
        - catalog: Catálogo de dados usado na exportação (obrigatório para export_to_db).
            - Exemplo: 'meu_catalogo'
        - buffer_log_path: Caminho do arquivo de buffer de governança (opcional).
            Se omitido, usa '<output_directory>/export_log_buffer_.txt'.
            Use o MESMO caminho no DataDictionaryBuilder (parâmetro buffer_path).
        - checkpoint_dir: Diretório de checkpoint (opcional). Padrão: '<output_directory>/_checkpoints'.
        """
        if input_directory is not None:
            self.input_directory = input_directory
        else:
            raise ValueError("O diretório de entrada deve ser fornecido.")

        if output_directory is not None:
            self.output_directory = output_directory
        else:
            raise ValueError("O diretório de saída deve ser fornecido.")

        # Inicializa a sessão Spark (no Databricks, reaproveita a sessão já existente)
        self.spark = SparkSession.builder.appName("BaseDataProcessor_Spark").getOrCreate()

        # Objeto de DataFrame para uso da classe genérica
        self.df = None
        self.catalog = catalog
        self.data_dict = {}
        # Database exporting methods in databricks
        self.import_database = None
        self.import_table = None
        self.export_database = None
        self.export_table = None
        # Lista de arquivos lidos (linhagem usada no log de governança)
        self.file_list = []

        # Caminho do buffer de governança
        if buffer_log_path:
            self.buffer_log_path = buffer_log_path
        elif self.output_directory:
            self.buffer_log_path = os.path.join(self.output_directory, BUFFER_FILENAME)
        else:
            self.buffer_log_path = BUFFER_PATH

        if self.buffer_log_path.startswith("/insert_your_path_here"):
            print("[ALERTA] buffer_log_path não configurado: o log de governança não será gravado. "
                  "Informe buffer_log_path ou output_directory no construtor.")

        self.checkpoint_dir = checkpoint_dir or (
            os.path.join(self.output_directory, "_checkpoints") if self.output_directory else "/tmp/checkpoints"
        )

    def read_data(self, filename, separator=None, skip_rows=0, header=True, sheet_name=None, ignore_last_rows=0):
        """
        Lê os dados de um arquivo Parquet, CSV ou Excel (compatível com Spark Connect/serverless).

        Parâmetros:
        - filename: Nome do arquivo (relativo ao input_directory) ou caminho absoluto.
        - separator: Separador CSV (opcional).
        - skip_rows: Linhas a ignorar no topo (opcional).
        - header: Se o CSV/Excel possui cabeçalho.
        - sheet_name: Nome ou índice da aba do Excel (opcional; padrão: primeira aba).
        - ignore_last_rows: Número de linhas a ignorar no final (opcional).

        Retorna:
        Um DataFrame do Spark contendo os dados lidos.

        EX:
        read_data("my_file.csv", separator=",", skip_rows=1, header=True)
        """
        # Garante caminho absoluto
        file_path = filename if filename.startswith("/") or filename.startswith("dbfs:") \
            else os.path.abspath(os.path.join(self.input_directory, filename))
        nome = filename.lower()

        print(f"Tentando ler o arquivo: {file_path}")

        # Pré-processamento: sanitiza colunas antes da leitura; em caso de erro, continua a execução
        try:
            self.preprocess_data_file(
                file_path=file_path,
                separator=separator,
                sheet_name=sheet_name
            )
        except Exception as e:
            print(f"Erro ao pré-processar o arquivo: {e}, continuando a leitura de dados")

        if nome.endswith(".parquet"):
            df = self.spark.read.parquet(file_path)

        elif nome.endswith(".csv"):
            read_options = {
                "header": header,
                "inferSchema": True
            }
            if separator:
                read_options["sep"] = separator

            df = self.spark.read.csv(file_path, **read_options)

        elif nome.endswith(".xlsx") or nome.endswith(".xls"):
            pdf = pd.read_excel(
                file_path,
                engine="openpyxl" if nome.endswith(".xlsx") else None,
                skiprows=skip_rows,
                header=0 if header else None,
                sheet_name=sheet_name if sheet_name is not None else 0
            )
            if ignore_last_rows > 0:
                pdf = pdf.iloc[:-ignore_last_rows]

            pdf.columns = [str(c).strip().replace(" ", "_") for c in pdf.columns]
            # Colunas 'object' com tipos misturados quebram o createDataFrame: converte para texto
            for c in pdf.columns:
                if pdf[c].dtype == "object":
                    pdf[c] = pdf[c].map(lambda v: None if pd.isna(v) else str(v))
            self.df = self.spark.createDataFrame(pdf)
            self.file_list.append(file_path)
            return self.df

        else:
            raise ValueError("Formato de arquivo não suportado. Use Parquet, CSV ou Excel.")

        # Aplicar skip_rows e ignore_last_rows para CSV/Parquet
        if skip_rows > 0 or ignore_last_rows > 0:
            df = df.withColumn("__row_id", row_number().over(Window.orderBy(monotonically_increasing_id())))

            total_rows = df.selectExpr("count(*) as total").collect()[0]["total"]
            start = skip_rows + 1  # row_number começa em 1
            end = total_rows - ignore_last_rows if ignore_last_rows > 0 else total_rows

            df = df.filter((df["__row_id"] >= start) & (df["__row_id"] <= end)).drop("__row_id")

        self.df = df

        # Registra o arquivo lido para a linhagem do log de governança
        self.file_list.append(file_path)

        return self.df

    def fetch_and_append_data(self, search_string, separator=None):
        """
        Busca arquivos com base em uma string presente no nome do arquivo e
        concatena os arquivos em um único DataFrame (por nome de coluna).

        Parâmetros:
        - search_string: String a ser buscada nos nomes dos arquivos.
        - separator: Separador a ser usado para arquivos CSV (opcional).

        Retorna:
        Um DataFrame do Spark contendo os dados concatenados (também armazenado via set_df).

        EX:
        fetch_and_append_data("my_string", ",")
        """
        files = self.list_files()

        matching_files = sorted(f for f in files if search_string in f)
        print(matching_files)
        if not matching_files:
            raise ValueError("Nenhum arquivo correspondente encontrado.")

        dataframes = [self.read_data(f, separator) for f in matching_files]
        combined_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dataframes)

        self.df = combined_df
        return combined_df

    def list_files(self):
        """
        Lista todos os arquivos no diretório de entrada.

        Retorna:
        Uma lista de nomes de arquivos.
        """
        return [f for f in os.listdir(self.input_directory)
                if os.path.isfile(os.path.join(self.input_directory, f))]

    def output_files(self):
        """
        Lista todos os arquivos no diretório de saída.

        Retorna:
        Uma lista de nomes de arquivos.
        """
        return os.listdir(self.output_directory)

    def reset_lineage(self):
        """
        Limpa a lista de arquivos de origem registrada para o log de governança.
        Use ao reaproveitar a mesma instância para uma ingestão sem relação com a anterior.
        """
        self.file_list = []

    def create_temp_view(self, view_name):
        """
        Cria uma view temporária usando o DataFrame atual, pode ser utilizada em consultas SQL.
        
        Parâmetros:
        - view_name: Nome da view desejada


        Ex:
        query = '''
        SELECT * FROM {view_name}
        '''
        processor.get_sql_query(query)
        """
        if self.df is not None:
            self.df.createOrReplaceTempView(view_name)
        else:
            raise ValueError("DataFrame não está definido.")

    def drop_columns(self, df, drop_list):
        """
        Remove colunas de um DataFrame do Spark.

        Parâmetros:
        - df: Dataframe a ser passado.
        - drop_list: Lista de colunas a serem dropadas.

        Retorna:
        Um DataFrame do Spark com colunas seleciondas dropadas.

        EX:
        df = drop_columns(df, ["coluna1", "coluna2")
        """
        return df.drop(*drop_list)

    def reorganize_columns(self, df, column_order):
        """
        Reorganiza as colunas de um DataFrame do Spark.

        Parâmetros:
        - df: Dataframe a ser passado.
        - column_order: Lista de colunas na ordem desejada.

        Retorna:
        Um DataFrame do Spark com as colunas ordenadas.

        EX:
        df = reorganize_columns(df, ["coluna1", "coluna2", "coluna3"])
        """
        return df.select(column_order)

    def rename_columns(self, df, column_mapping):
        """
        Renomeia colunas de um DataFrame do Spark.

        Parâmetros:
        - df: Dataframe a ser passado.
        - column_mapping: Dicionário com mapeamento de nomes de colunas antigos para novos nomes.

        Retorna:
        Um DataFrame do Spark com as colunas renomeadas.

        EX:
        df = rename_columns(df, {"coluna_antiga": "coluna_nova"})
        """
        for old_col, new_col in column_mapping.items():
            df = df.withColumnRenamed(old_col, new_col)
        return df

    def preencher_valores_nulos(self, df, coluna, valor_substituto=""):
        """
        Preenche valores nulos em uma coluna específica.
        
        Parametros:
        - df: DataFrame do Spark.
        - coluna: Coluna a ser preenchida.
        - valor_substituto: Valor a ser usado para substituir os valores nulos.

        Retorna:
        DataFrame do Spark com valores nulos preenchidos.

        EX:
        df = preencher_valores_nulos(df, "coluna", "valor_substituto")
        """
        return df.fillna({coluna: valor_substituto})

    def convert_type(self, df, column_to_convert, new_type):
        """
        Converte o tipo de uma coluna específica em um DataFrame do Spark.

        Parâmetros:
        - df: Dataframe a ser passado.
        - column_to_convert: Coluna a ser convertida.
        - new_type: Novo tipo de dados para a coluna.

        Retorna:
        DataFrame do Spark com a coluna convertida.

        EX:
        df = convert_type(df, "coluna", "int")
        """
        return df.withColumn(column_to_convert, col(column_to_convert).cast(new_type))

    def formatar_nomes_colunas(self, df, substituicoes={}):
        """
        Formata os nomes das colunas substituindo caracteres especificados.
        
        Paramêtros:
        - df: DataFrame do Spark.
        - substituicoes: Dicionário com caracteres a serem substituídos

        Retorna:
        DataFrame do Spark com os nomes das colunas formatados.

        EX:
        df = formatar_nomes_colunas(df, {" ": "_", "-": "_", "(": "", ")": ""})
        """
        for old_char, new_char in substituicoes.items():
            df = df.toDF(*[c.replace(old_char, new_char) for c in df.columns])
        return df

    def ordenar_dataframe(self, df, colunas, ascendentes):
        """
        Ordena um DataFrame do Spark com base em colunas específicas.

        Parâmetros:
        - df: Dataframe a ser passado.
        - colunas: Lista de colunas a serem usadas para ordenamento

        Retorna:
        DataFrame do Spark ordenado.

        EX:
        df = ordenar_dataframe(df, ["coluna1", "coluna2"], [True, False])
        """
        sort_cols = [col(c).asc() if asc else col(c).desc() for c, asc in zip(colunas, ascendentes)]
        return df.orderBy(*sort_cols)

    def drop_duplicates(self, df, subset=None):
        """
        Remove linhas duplicadas de um DataFrame do Spark.

        Parametros:
        - df: DataFrame do Spark.
        - subset: Lista de colunas a serem usadas para detecção de duplicatas.

        Retorna:
        DataFrame do Spark sem duplicatas.

        EX:
        df = drop_duplicates(df, ["coluna1", "coluna2"])
        """
        return df.dropDuplicates(subset)

    def drop_null_rows(self, df):
        """
        Remove linhas inteiramente nulas de um DataFrame do Spark.

        Parâmetros:
        - df: DataFrame do Spark.

        Retorna:
        - DataFrame sem linhas nulas

        EX:
        df = drop_null_rows(df)
        """
        return df.dropna(how="all")

    def sanitize_column_names(self, df):
        """
        Limpa os nomes das colunas removendo acentos, espaços, caracteres especiais e padroniza para minúsculo.

        Parâmetros:
        - df: DataFrame do Spark.

        Retorna:
        - DataFrame com colunas renomeadas.

        EX:
        df = sanitize_column_names(df)
        """
        def clean_string(s):
            # Remove acentos
            s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
            # Substitui espaços e caracteres especiais por _
            s = re.sub(r'[^0-9a-zA-Z]+', '_', s)
            # Remove underscores duplicados
            s = re.sub(r'_+', '_', s)
            # Remove underscores no início/fim e transforma em minúsculo
            return s.strip('_').lower()

        new_columns = [clean_string(c) for c in df.columns]
        return df.toDF(*new_columns)


    def resetar_indice(self, df):
        """
        No Spark, resetar o índice não é necessário, mas pode-se adicionar uma coluna de índice.
        
        Parâmetros:
        - df: DataFrame do Spark.

        Retorna:
        - DataFrame do Spark com uma coluna de índice.

        EX:
        df = resetar_indice(df)
        """
        return df.withColumn("index", monotonically_increasing_id())

    def selecionar_colunas(self, df, colunas):
        """
        Seleciona colunas específicas de um DataFrame do Spark.

        Parâmetros:
        - df: DataFrame do Spark.
        - colunas: Lista de colunas a serem selecionadas.

        Retorna:
        DataFrame do Spark com as colunas selecionadas.

        EX:
        df = selecionar_colunas(df, ["coluna1", "coluna2"])
        """
        return df.select(*colunas)

    def combinar_dataframes(self, df1, df2, coluna_comum, metodo="left"):
        """
        Combina dois DataFrames do Spark com base em uma coluna comum.

        Parâmetros:
        - df1: Primeiro DataFrame do Spark.
        - df2: Segundo DataFrame do Spark.

        Retorna:
        DataFrame do Spark resultante da combinação.

        EX:
        df = combinar_dataframes(df1, df2, "coluna_comum", metodo="left")
        """
        return df1.join(df2, on=coluna_comum, how=metodo)

    def tratar_colunas(self, df, colunas, substituicoes):
        """
        Substitui valores nas colunas especificadas de um DataFrame do Spark.

        Parâmetros:
        - df: DataFrame do Spark.
        - colunas: Lista de colunas a serem tratadas.

        Retorna:
        DataFrame do Spark com as substituições aplicadas.

        EX:
        df = tratar_colunas(df, ["coluna1", "coluna2"], {"antigo": "novo"})
        """
        for coluna in colunas:
            for antigo, novo in substituicoes.items():
                df = df.withColumn(coluna, when(col(coluna) == antigo, novo).otherwise(col(coluna)))
        return df

    def criar_coluna(self, df, nome_coluna, valor_padrao=""):
        """
        Cria uma nova coluna com um valor padrão.

        Parâmetros:
        - df: DataFrame do Spark.
        - nome_coluna: Nome da nova coluna.
        - valor_padrao: Valor padrão para a nova coluna.

        Retorna:
        DataFrame do Spark com a nova coluna criada.

        EX:
        df = criar_coluna(df, "nova_coluna", "")
        """
        return df.withColumn(nome_coluna, lit(valor_padrao))

    def apply_function_to_dataframes(self, function, dataframe_list, *args, **kwargs):
        """
        Aplica uma função a todos os dataframes em uma lista.

        Parâmetros:
        - function: Função a ser aplicada.
        - dataframe_list: Lista de DataFrames do Spark.
        - args: Argumentos posicionais adicionais para a função.
        - kwargs: Argumentos nomeados adicionais para a função.

        Retorna:
        Lista de DataFrames resultantes da aplicação da função.

        EX:
        resultados = apply_function_to_dataframes(func, dataframes, arg1, arg2)
        """
        return [function(df, *args, **kwargs) for df in dataframe_list]

    def set_df(self, df):
        """
        Armazena o DataFrame na classe.

        Parâmetros:
        - df: DataFrame do Spark.

        Retorna:
        DataFrame do Spark armazenado na classe de forma temporária

        EX:
        set_df(df)
        """
        self.df = df

    def get_df(self):
        """
        Retorna o DataFrame armazenado.

        Retorna:
        DataFrame do Spark armazenado na classe.

        EX:
        df = get_df()
        """
        return self.df
    

    def get_sql_query(self, query, exibir=True):
        """
        Executa uma consulta SQL e retorna um DataFrame Spark. Erros de SQL são levantados
        (antes era retornada uma string de erro, o que quebrava o código chamador em silêncio).

        Parametros:
            - query: Consulta SQL a ser executada.
            - exibir: Se True, exibe o resultado (display no Databricks, show fora dele).

        Retorna:
            - df: DataFrame Spark com o resultado da consulta.

        EX:
        df = get_sql_query("SELECT * FROM table")
        """
        df = self.spark.sql(query)
        if exibir:
            _exibir(df)
        return df

    # ------------------------------------------------------------------
    # Exportação e governança
    # ------------------------------------------------------------------

    def _tabela_existe(self, full_table_name):
        try:
            return self.spark.catalog.tableExists(full_table_name)
        except Exception:
            try:
                self.spark.sql(f"DESCRIBE TABLE {_q(*full_table_name.split('.'))}")
                return True
            except Exception:
                return False

    def _ler_propriedades(self, full_table_name):
        try:
            rows = self.spark.sql(f"SHOW TBLPROPERTIES {_q(*full_table_name.split('.'))}").collect()
            return {r["key"]: r["value"] for r in rows}
        except Exception:
            return {}

    def _capturar_comentarios(self, full_table_name):
        """Lê o comentário da tabela e das colunas antes de um overwrite (que pode apagá-los)."""
        comentario_tabela, comentarios_colunas = None, {}
        try:
            linhas = self.spark.sql(f"DESCRIBE TABLE EXTENDED {_q(*full_table_name.split('.'))}").collect()
        except Exception:
            return comentario_tabela, comentarios_colunas
        secao_detalhes = False
        for r in linhas:
            nome = r["col_name"]
            if nome and nome.startswith("# Detailed"):
                # A partir daqui vêm os metadados da tabela (Catalog, Owner, Comment, ...)
                secao_detalhes = True
                continue
            if secao_detalhes:
                if nome == "Comment" and r["data_type"]:
                    comentario_tabela = r["data_type"]
                continue
            if not nome or nome.startswith("#"):
                continue
            if r["comment"]:
                comentarios_colunas[nome] = r["comment"]
        return comentario_tabela, comentarios_colunas

    def _restaurar_comentarios(self, full_table_name, comentario_tabela, comentarios_colunas, colunas_atuais):
        alvo = _q(*full_table_name.split("."))
        atual_tabela, atuais_colunas = self._capturar_comentarios(full_table_name)
        if comentario_tabela and not atual_tabela:
            try:
                self.spark.sql(f"COMMENT ON TABLE {alvo} IS '{_sql_str(comentario_tabela)}'")
            except Exception as e:
                print(f"[ALERTA] Não foi possível restaurar o comentário da tabela: {e}")
        restauradas = 0
        for coluna, comentario in comentarios_colunas.items():
            if coluna in colunas_atuais and not atuais_colunas.get(coluna):
                try:
                    self.spark.sql(f"ALTER TABLE {alvo} ALTER COLUMN {_q(coluna)} COMMENT '{_sql_str(comentario)}'")
                    restauradas += 1
                except Exception as e:
                    print(f"[ALERTA] Não foi possível restaurar o comentário da coluna {coluna}: {e}")
        if restauradas:
            print(f"[INFO] {restauradas} comentário(s) de coluna restaurado(s) após o overwrite.")

    def export_to_db(self, database, table, mode="overwrite", descricao=None, registrar_governanca=True):
        """
        Exporta o DataFrame atual para uma tabela Delta e registra metadados de governança:
        - colunas write_date/write_time na própria tabela;
        - TBLPROPERTIES 'governanca.*' (criador original e última execução: notebook, usuário e data);
        - comentário da tabela (se `descricao` for informada) e preservação dos comentários
          de tabela/colunas já existentes quando o modo é overwrite;
        - uma linha por coluna no buffer de governança (lido depois pelo DataDictionaryBuilder).

        Parâmetros:
        - database: Nome do schema de destino.
        - table: Nome da tabela de destino.
        - mode: "overwrite" (padrão) ou "append".
        - descricao: Comentário da tabela (opcional). Se omitido, o comentário existente é mantido
          e a tabela fica livre para receber descrição gerada por IA no DataDictionaryBuilder.
        - registrar_governanca: Se False, não grava no buffer de governança.

        Retorna:
        - Nome completo da tabela exportada.

        EX:
        processor = BaseDataProcessorSpark(input_directory="/Volumes/cat/raw/in", output_directory="/Volumes/cat/raw/out", catalog="meu_catalogo")
        processor.set_df(meu_dataframe)
        processor.export_to_db(database="meu_banco", table="minha_tabela", mode="append")
        """
        df = self.get_df()
        if df is None:
            raise ValueError("Nenhum DataFrame definido para exportação. Use 'set_df()' ou 'read_data()' primeiro.")
        if not self.catalog:
            raise ValueError("Catálogo não definido. Informe 'catalog' no construtor.")
        if mode not in ("overwrite", "append"):
            raise ValueError("Parâmetro 'mode' deve ser 'overwrite' ou 'append'.")

        agora = datetime.now()
        contexto = _obter_contexto_notebook()
        notebook_path = contexto["path"] or "unknown_notebook"
        notebook_name = notebook_path.split("/")[-1]
        usuario = self._usuario_atual()

        full_table_name = f"{self.catalog}.{database}.{table}"
        self.export_database = database
        self.export_table = table

        # Colunas de horário de escrita
        df = df.withColumn("write_date", lit(agora.strftime("%Y-%m-%d"))) \
               .withColumn("write_time", lit(agora.strftime("%H:%M:%S")))

        # Metadados existentes (antes da escrita, pois um overwrite pode substituí-los)
        tabela_existia = self._tabela_existe(full_table_name)
        props = self._ler_propriedades(full_table_name) if tabela_existia else {}
        comentario_tabela, comentarios_colunas = (None, {})
        if tabela_existia and mode == "overwrite":
            comentario_tabela, comentarios_colunas = self._capturar_comentarios(full_table_name)

        print(f"Exportando para: {full_table_name} (modo={mode})")
        writer = df.write.format("delta").option("userMetadata", notebook_name).mode(mode)
        if mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        else:
            writer = writer.option("mergeSchema", "true")
        writer.saveAsTable(full_table_name)

        # Propriedades de governança e comentários (falhas aqui não invalidam a escrita)
        try:
            self._aplicar_metadados_tabela(full_table_name, props, notebook_path, usuario, agora, descricao)
        except Exception as e:
            print(f"[ALERTA] Dados gravados, mas falhou ao aplicar metadados de governança: {e}")

        if tabela_existia and mode == "overwrite" and (comentario_tabela or comentarios_colunas):
            self._restaurar_comentarios(full_table_name,
                                        None if descricao else comentario_tabela,
                                        comentarios_colunas, set(df.columns))

        if registrar_governanca:
            self._registrar_log_buffer(df, database, table, usuario, agora, contexto)

        return full_table_name

    def _usuario_atual(self):
        try:
            return self.spark.sql("SELECT current_user()").collect()[0][0]
        except Exception:
            try:
                return getpass.getuser()
            except Exception:
                return "unknown_user"

    def _aplicar_metadados_tabela(self, full_table_name, props_existentes, notebook_path, usuario, agora, descricao):
        """Grava TBLPROPERTIES de governança, preservando quem criou a tabela."""
        alvo = _q(*full_table_name.split("."))
        ts = agora.strftime("%Y-%m-%d %H:%M:%S")
        arquivos = list(dict.fromkeys(self.file_list))
        origem = " | ".join(arquivos) if arquivos else "N/A"
        if len(origem) > 1000:
            origem = origem[:997] + "..."

        propriedades = {
            "governanca.notebook_criador": props_existentes.get("governanca.notebook_criador", notebook_path),
            "governanca.usuario_criador": props_existentes.get("governanca.usuario_criador", usuario),
            "governanca.data_criacao": props_existentes.get("governanca.data_criacao", ts),
            "governanca.notebook_ultima_execucao": notebook_path,
            "governanca.usuario_ultima_execucao": usuario,
            "governanca.data_ultima_execucao": ts,
            "governanca.arquivos_origem": origem,
        }
        # Obs.: 'owner' é uma propriedade reservada do Spark e não pode ser definida via TBLPROPERTIES,
        # por isso as chaves usam o prefixo 'governanca.'.
        pares = ",\n                ".join(f"'{_sql_str(k)}' = '{_sql_str(v)}'" for k, v in propriedades.items())
        self.spark.sql(f"ALTER TABLE {alvo} SET TBLPROPERTIES (\n                {pares}\n            )")

        if descricao:
            self.spark.sql(f"COMMENT ON TABLE {alvo} IS '{_sql_str(descricao)}'")
        print(f"[INFO] Propriedades de governança aplicadas em {full_table_name}.")

    def _registrar_log_buffer(self, df, database, table, usuario, agora, contexto):
        """Acrescenta ao buffer uma linha por coluna da tabela exportada."""
        caminho = self.buffer_log_path
        if not caminho or caminho.startswith("/insert_your_path_here"):
            print("[ALERTA] buffer_log_path não configurado; log de governança não gravado.")
            return

        arquivos = list(dict.fromkeys(self.file_list))
        self.file_list = arquivos
        origem = " | ".join(arquivos) if arquivos else "N/A (DataFrame em memória/SQL)"
        notebook_ref = contexto.get("url") or contexto.get("path") or "N/A"

        log_rows = []
        for column, dtype in df.dtypes:
            log_rows.append({
                "catalog": self.catalog,
                "schema": database,
                "table": table,
                "column": column,
                "collumn_dtype": dtype,
                "creation_date": agora.strftime("%Y-%m-%d"),
                "creation_time": agora.strftime("%Y-%m-%d %H:%M:%S"),
                "table_description": "N/A",
                "user": usuario,
                "file_fetched": origem,
                "source_notebook_url": notebook_ref,
            })

        def limpar(v):
            # ';' é o separador do buffer; quebras de linha corromperiam o CSV
            return str(v if v is not None else "").replace(";", ",").replace("\r", " ").replace("\n", " ")

        try:
            pasta = os.path.dirname(caminho)
            if pasta:
                os.makedirs(pasta, exist_ok=True)
            novo_arquivo = not os.path.exists(caminho) or os.path.getsize(caminho) == 0
            with open(caminho, "a", encoding="utf-8") as f:
                if novo_arquivo:
                    f.write(";".join(GOVERNANCE_COLUMNS) + "\n")
                for row in log_rows:
                    f.write(";".join(limpar(row.get(h)) for h in GOVERNANCE_COLUMNS) + "\n")
            print(f"[INFO] {len(log_rows)} linha(s) de governança adicionadas ao buffer: {caminho}")
            if not arquivos:
                print("[INFO] Nenhum arquivo de origem registrado (DataFrame veio de set_df()/SQL); "
                      "file_fetched gravado como N/A.")
        except Exception as e:
            print(f"[ERRO] Falha ao escrever o log de buffer: {e}")

    def preprocess_data_file(self, file_path, separator=None, sheet_name=None):
        """
        Pré-processa o arquivo para sanitizar os nomes das colunas diretamente no arquivo. Se falhar na
        leitura, tenta ler com Pandas, sanitiza as colunas e sobrescreve o arquivo. Funciona para CSV, Excel e Parquet.

        Parametros:
            - file_path: Caminho para o arquivo a ser pré-processado.
            - separator: Separador usado
            - sheet_name: Nome da planilha (padrão: primeira aba)

        EX:
        processor.preprocess_data_file("/Volumes/cat/raw/in/arquivo.csv", separator=';')
        """
        aba = sheet_name if sheet_name is not None else 0
        nome = file_path.lower()
        try:
            if nome.endswith(".csv"):
                pd.read_csv(file_path, sep=separator or ",", nrows=5)  # testa leitura
            elif nome.endswith(".xlsx") or nome.endswith(".xls"):
                pd.read_excel(file_path, sheet_name=aba, nrows=5)
            elif nome.endswith(".parquet"):
                pd.read_parquet(file_path)
            # Teste passou (ou formato não suportado), não precisa processar
            return

        except Exception as e:
            print(f"[PREPROCESS] Erro ao ler o arquivo {file_path}, tentando sanitizar: {e}")

            try:
                if nome.endswith(".csv"):
                    df = pd.read_csv(file_path, sep=separator or ",")
                elif nome.endswith(".xlsx") or nome.endswith(".xls"):
                    df = pd.read_excel(file_path, sheet_name=aba)
                elif nome.endswith(".parquet"):
                    df = pd.read_parquet(file_path)
                else:
                    return

                # Sanitização dos nomes de colunas (sem precisar do Spark)
                df.columns = [self._limpar_nome_coluna(c) for c in df.columns]

                # Sobrescreve o arquivo original mantendo o separador
                if nome.endswith(".csv"):
                    df.to_csv(file_path, index=False, sep=separator or ",")
                elif nome.endswith(".xlsx") or nome.endswith(".xls"):
                    df.to_excel(file_path, index=False)
                elif nome.endswith(".parquet"):
                    df.to_parquet(file_path, index=False)

                print(f"[PREPROCESS] Arquivo {file_path} sanitizado com sucesso.")

            except Exception as err:
                print(f"[PREPROCESS] Falha ao sanitizar o arquivo {file_path}: {err}")

    @staticmethod
    def _limpar_nome_coluna(s):
        s = ''.join(c for c in unicodedata.normalize('NFKD', str(s)) if not unicodedata.combining(c))
        s = re.sub(r'[^0-9a-zA-Z]+', '_', s)
        s = re.sub(r'_+', '_', s)
        return s.strip('_').lower()

    
    def drop_specific_row(self, df, column, target_value):
        """
        Remove linhas de um DataFrame do Spark com base em um valor específico de uma coluna.

        Parâmetros:
        - df: DataFrame do Spark.
        - column: Nome da coluna.
        - target_value: Valor alvo para remoção das linhas.

        Retorna:
        DataFrame do Spark sem as linhas que contêm o valor alvo na coluna especificada.

        EX:
        drop_specific_row(df, "coluna", "valor")
        """
        # eqNullSafe: linhas com valor nulo na coluna são mantidas (com '!=' elas eram descartadas junto)
        return df.filter(~col(column).eqNullSafe(target_value))
    
    def substituir_valores(self, df, valor_antigo, valor_novo):
        """
        Substitui valores em todas as colunas de um DataFrame do Spark.

        Parâmetros:
        - df: DataFrame do Spark.
        - valor_antigo: Valor a ser substituído.
        - valor_novo: Novo valor.

        Retorna:
        DataFrame do Spark com os valores substituídos.

        EX:
        substituir_valores(df, "valor_antigo", "valor_novo")
        """
        for coluna in df.columns:
            df = df.withColumn(coluna, when(col(coluna) == valor_antigo, valor_novo).otherwise(col(coluna)))
        return df

    def replace_values(self, df, column, old_value, new_value):
        """
        Substitui valores em uma coluna específica de um DataFrame usando regexp_replace.

        Parâmetros:
            df (DataFrame): DataFrame do Spark a ser processado.
            column (str): Nome da coluna para aplicar as substituições.
            old_value (str): Valor a ser substituído.
            new_value (str): Valor para substituir.

        Retorna:
            DataFrame: DataFrame atualizado com os valores substituídos.
        
        EX:
        replace_values(df, "coluna", "valor_antigo", "valor_novo")
        """
        df = df.withColumn(column, regexp_replace(col(column), old_value, new_value))
        return df

    def change_data_types(self, df, type_mapping):
        """
        Altera os tipos de dados das colunas de um DataFrame do Spark com base em um dicionário de mapeamento.

        Parâmetros:
        - df: DataFrame do Spark.
        - type_mapping: Dicionário onde as chaves são os nomes das colunas e os valores são os novos tipos de dados.

        Retorna:
        DataFrame do Spark com os tipos de dados alterados.

        EX:
        change_data_types(df, {"coluna1": "int", "coluna2": "string"})
        """
        for column, new_type in type_mapping.items():
            df = df.withColumn(column, col(column).cast(new_type))
        return df

    @staticmethod
    def fill_nan_based_on_dtype(df, valores_padrao=None):
        """
        Preenche valores nulos com base no tipo de dado de cada coluna.

        Padrões: numéricos (int, bigint, double, decimal, ...) → 0; string → "Não identificado";
        boolean → False; date → 1901-01-01; timestamp → 1901-01-01 00:00:00.
        (Na versão anterior, colunas bigint/float/decimal/timestamp ficavam sem preenchimento,
        porque o fillna ignora valores de tipo incompatível.)

        Parametros:
        - df: DataFrame do Spark.
        - valores_padrao: dict opcional para sobrescrever padrões por categoria
          ('numero', 'string', 'boolean', 'date', 'timestamp').

        Retorna:
        - DataFrame com valores nulos preenchidos com base no tipo de dado.

        EX:
        fill_nan_based_on_dtype(df)
        """
        padroes = {
            "numero": 0,
            "string": "Não identificado",
            "boolean": False,
            "date": "1901-01-01",
            "timestamp": "1901-01-01 00:00:00",
        }
        padroes.update(valores_padrao or {})
        numericos = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")

        preenchimento = {}
        for col_name, dtype in df.dtypes:
            if dtype.startswith(numericos):
                preenchimento[col_name] = padroes["numero"]
            elif dtype == "string":
                preenchimento[col_name] = padroes["string"]
            elif dtype == "boolean":
                preenchimento[col_name] = padroes["boolean"]

        if preenchimento:
            df = df.fillna(preenchimento)

        for col_name, dtype in df.dtypes:
            if dtype == "date":
                df = df.withColumn(col_name, F.coalesce(col(col_name), F.to_date(lit(padroes["date"]))))
            elif dtype.startswith("timestamp"):
                df = df.withColumn(col_name, F.coalesce(col(col_name), F.to_timestamp(lit(padroes["timestamp"]))))

        return df

    # === Preprocessing Utilities ===

    def decompress_files(self, compressed_file, output_dir=None):
        """
        Descomprime arquivos e exporta para o diretório atual ou especificado.

        Parâmetros:
        - compressed_file: Nome do arquivo comprimido.
        - output_dir: Diretório de saída para os arquivos descomprimidos (opcional).

        Retorna:
        Lista de arquivos descomprimidos.

        EX:
        decompress_files("arquivo.zip", "/caminho/para/diretorio")
        """
        if output_dir is None:
            output_dir = self.input_directory

        file_path = os.path.join(self.input_directory, compressed_file)
        extracted_files = []

        def extrair_tar(tar_ref):
            try:
                tar_ref.extractall(output_dir, filter="data")  # bloqueia caminhos maliciosos (Python 3.12+)
            except TypeError:
                tar_ref.extractall(output_dir)

        if compressed_file.endswith(".zip"):
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
                extracted_files = zip_ref.namelist()
        elif compressed_file.endswith(".tar.gz") or compressed_file.endswith(".tgz"):
            with tarfile.open(file_path, 'r:gz') as tar_ref:
                extrair_tar(tar_ref)
                extracted_files = tar_ref.getnames()
        elif compressed_file.endswith(".tar"):
            with tarfile.open(file_path, 'r:') as tar_ref:
                extrair_tar(tar_ref)
                extracted_files = tar_ref.getnames()
        elif compressed_file.endswith(".gz"):
            output_file = os.path.join(output_dir, os.path.basename(compressed_file)[:-3])
            with gzip.open(file_path, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            extracted_files.append(output_file)
        else:
            raise ValueError("Formato de arquivo comprimido não suportado. Use arquivos ZIP, TAR ou GZ.")

        return extracted_files

    def pivot_or_unpivot(self, df, mode, index_columns=None, pivot_column=None, value_column=None,
                         value_vars=None, var_name="variable", value_name="value", agg_func="sum"):
        """
        Realiza pivoteamento ou unpivot (melt) em um DataFrame Spark.

        Parâmetros:
        - df (DataFrame): DataFrame do Spark a ser transformado.
        - mode (str): "pivot" para pivoteamento ou "unpivot" para melt.

        Para mode="pivot":
        - index_columns (str ou list): Coluna(s) base para agrupamento (ex: 'cod_municipio').
        - pivot_column (str): Coluna cujos valores se tornarão nomes de colunas (ex: 'ano').
        - value_column (str): Coluna com os valores a agregar (ex: 'qtd').
        - agg_func (str): 'sum', 'avg', 'count', 'min', 'max' ou 'first'. Default: 'sum'.

        Para mode="unpivot":
        - index_columns (str ou list): Colunas fixas (ex: ['cod_municipio', 'nome_municipio']).
        - value_vars (list, opcional): Colunas convertidas em linhas. Se omitido, usa todas as
          colunas que não estão em index_columns. Tipos diferentes são unificados
          (double se todos forem numéricos; string caso contrário).
        - var_name (str): Nome da coluna com os nomes das colunas derretidas (ex: 'ano').
        - value_name (str): Nome da coluna com os valores (ex: 'qtd').

        Retorna:
        - DataFrame transformado.

        EX:
        pivot_or_unpivot(df, "pivot", index_columns="municipio", pivot_column="ano", value_column="qtd")
        pivot_or_unpivot(df, "unpivot", index_columns=["municipio"], value_vars=["2021", "2022"], var_name="ano", value_name="qtd")
        """
        if mode not in ("pivot", "unpivot"):
            raise ValueError("Parâmetro 'mode' deve ser 'pivot' ou 'unpivot'.")

        if isinstance(index_columns, str):
            index_columns = [index_columns]
        if not index_columns:
            raise ValueError("Parâmetro 'index_columns' é obrigatório.")

        if mode == "pivot":
            if not pivot_column or not value_column:
                raise ValueError("Parâmetros 'pivot_column' e 'value_column' são obrigatórios para pivot.")

            agregacoes = {"sum": F.sum, "avg": F.avg, "count": F.count, "min": F.min, "max": F.max, "first": F.first}
            if agg_func not in agregacoes:
                raise ValueError(f"Função de agregação '{agg_func}' não suportada.")

            return df.groupBy(*index_columns).pivot(pivot_column).agg(agregacoes[agg_func](value_column))

        # unpivot
        if value_vars is None:
            value_vars = [c for c in df.columns if c not in index_columns]
        if not isinstance(value_vars, (list, tuple)) or not value_vars:
            raise ValueError("Parâmetro 'value_vars' deve ser uma lista não vazia de colunas.")

        tipos = dict(df.dtypes)
        tipos_valores = {tipos[c] for c in value_vars}
        if len(tipos_valores) > 1:
            numericos = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")
            alvo = "double" if all(t.startswith(numericos) for t in tipos_valores) else "string"
            df = df.select(*[col(c) for c in index_columns],
                           *[col(f"`{c}`").cast(alvo).alias(c) for c in value_vars])

        return df.unpivot(index_columns, list(value_vars), var_name, value_name)

    def convert_to_parquet(self, filename, output_path=None):
        """
        Converte um arquivo CSV ou XLSX em Parquet para otimizar leitura posterior.

        Parâmetros:
        - filename (str): Nome do arquivo no diretório de entrada.
        - output_path (str): Caminho de saída opcional. Se não definido, cria <nome>.parquet ao lado do original.

        Retorna:
        - Caminho do arquivo .parquet criado.

        EX:
        convert_to_parquet("arquivo.csv", "/caminho/para/arquivo.parquet")
        """
        file_path = os.path.join(self.input_directory, filename)
        output_path = output_path or os.path.splitext(file_path)[0] + ".parquet"

        nome = filename.lower()
        if nome.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif nome.endswith(".xlsx") or nome.endswith(".xls"):
            df = pd.read_excel(file_path)
        else:
            raise ValueError("Formato não suportado. Use CSV ou XLSX.")

        df.to_parquet(output_path, index=False)
        print(f"Arquivo convertido para Parquet: {output_path}")
        return output_path

    def write_partitioned_data(self, df, path, partition_cols):
        """
        Salva um DataFrame particionado por colunas para leitura seletiva.

        Parâmetros:
        - df (DataFrame): Spark DataFrame.
        - path (str): Caminho de saída.
        - partition_cols (list): Lista de colunas para particionar.

        Retorna:
        - None. Salva o DataFrame particionado no caminho especificado.

        EX:
        write_partitioned_data(df, "/caminho/para/diretorio", ["coluna1", "coluna2"])
        """
        if isinstance(partition_cols, str):
            partition_cols = [partition_cols]
        df.write.partitionBy(*partition_cols).mode("overwrite").parquet(path)

    def profile_file_sample(self, filename, sampling_ratio=0.01, separator=None):
        """
        Lê um CSV inferindo o schema a partir de uma amostra das linhas (samplingRatio),
        o que acelera a inferência em arquivos grandes.

        Parâmetros:
        - filename (str): Nome do arquivo.
        - sampling_ratio (float): Proporção das linhas usada para inferir o schema.
        - separator (str): Separador (opcional).

        Retorna:
        - Spark DataFrame.

        EX:
        profile_file_sample("arquivo.csv", 0.01)
        """
        path = os.path.join(self.input_directory, filename)
        leitor = self.spark.read.option("header", True).option("inferSchema", True) \
                               .option("samplingRatio", sampling_ratio)
        if separator:
            leitor = leitor.option("sep", separator)
        df = leitor.csv(path)
        df.printSchema()
        df.show(5)
        return df

    def read_csv_with_schema(self, filename, schema, separator=None):
        """
        Lê um CSV usando schema explícito, evitando inferência automática.

        Parâmetros:
        - filename (str): Nome do arquivo CSV.
        - schema (StructType ou str DDL): Schema Spark definido manualmente.
        - separator (str): Separador (opcional).

        Retorna:
        - Spark DataFrame.

        EX:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
        schema = StructType([
            StructField("coluna1", StringType(), True),
            StructField("coluna2", IntegerType(), True),
            StructField("coluna3", FloatType(), True)
        ])
        read_csv_with_schema("arquivo.csv", schema)
        """
        path = os.path.join(self.input_directory, filename)
        leitor = self.spark.read.schema(schema).option("header", True)
        if separator:
            leitor = leitor.option("sep", separator)
        self.file_list.append(path)
        return leitor.csv(path)

    def parallel_read_csv(self, file_list, num_processes=None, separator=None, header=True):
        """
        Lê vários arquivos CSV de uma vez com o Spark (que já paraleliza a leitura entre os executores)
        e retorna um único DataFrame Spark.

        A versão anterior usava Pandas + multiprocessing com uma função local, que não pode ser
        serializada pelo Pool e falhava sempre; além disso rodava só no driver.

        Parâmetros:
        - file_list (list): Lista de nomes de arquivos CSV (relativos ao input_directory ou absolutos).
        - num_processes: Mantido por compatibilidade; ignorado.
        - separator (str): Separador (opcional).
        - header (bool): Se os arquivos têm cabeçalho.

        Retorna:
        - DataFrame Spark com todos os arquivos (também armazenado via set_df).

        EX:
        parallel_read_csv(["arquivo1.csv", "arquivo2.csv"])
        """
        caminhos = [f if f.startswith("/") or f.startswith("dbfs:") else os.path.join(self.input_directory, f)
                    for f in file_list]
        leitor = self.spark.read.option("header", header).option("inferSchema", True)
        if separator:
            leitor = leitor.option("sep", separator)
        self.df = leitor.csv(caminhos)
        self.file_list.extend(caminhos)
        return self.df

    def cache_dataframe(self, df):
        """
        Aplica cache em um DataFrame para acelerar reutilizações.
        Em compute serverless o cache não é suportado: o DataFrame é devolvido sem cache, com aviso.

        Parâmetros:
        - df (DataFrame): Spark DataFrame.

        Retorna:
        - DataFrame Spark (com cache, quando suportado).

        EX:
        df = cache_dataframe(df)
        """
        try:
            df = df.cache()
            df.count()  # força o cache
        except Exception as e:
            print(f"[ALERTA] Cache não suportado neste compute (ex.: serverless). Seguindo sem cache. Detalhe: {e}")
        return df

    def checkpoint_dataframe(self, df):
        """
        Aplica checkpoint em um DataFrame para truncar o plano de execução.
        Tenta, em ordem: checkpoint confiável em checkpoint_dir (clusters clássicos),
        checkpoint direto (Spark Connect), localCheckpoint e, por fim, devolve o DataFrame sem alteração.

        Parâmetros:
        - df (DataFrame): Spark DataFrame.

        Retorna:
        - DataFrame após checkpoint (quando suportado).

        EX:
        df = checkpoint_dataframe(df)
        """
        try:
            self.spark.sparkContext.setCheckpointDir(self.checkpoint_dir)
            return df.checkpoint()
        except Exception:
            pass
        try:
            return df.checkpoint()
        except Exception:
            pass
        try:
            return df.localCheckpoint()
        except Exception as e:
            print(f"[ALERTA] Checkpoint não suportado neste compute. Seguindo sem checkpoint. Detalhe: {e}")
            return df

    def estimate_file_access_times(self):
        """
        Estima o tempo de leitura de cada arquivo no diretório de entrada com base no tipo, tamanho e capacidade da máquina.
        A estimativa considera apenas o driver (onde este código roda), não o cluster.

        Retorna:
        Uma lista de dicionários com:
        - nome do arquivo
        - tipo
        - tamanho em MB
        - estimativa de tempo (segundos)
        - fatores de ajuste (RAM, CPU)

        EX:
        estimate_file_access_times()
        """
        # === 1. Coleta informações da máquina ===
        try:
            import psutil
            freq = psutil.cpu_freq()
            cpu_freq = (freq.max or freq.current) / 1000 if freq else 2.5  # GHz
            ram_available_gb = psutil.virtual_memory().available / (1024 ** 3)
        except Exception:
            cpu_freq, ram_available_gb = 2.5, 8.0  # valores neutros se psutil não estiver disponível

        # === 2. Função de estimativa com ajuste dinâmico ===
        def get_adjusted_estimate(file_type, size_mb, cpu_ghz, ram_gb):
            if file_type == "csv":
                base = size_mb / 100 * 30  # 30s por 100MB como base
            elif file_type == "parquet":
                base = size_mb / 500 * 20  # 20s por 500MB como base
            elif file_type == "xlsx":
                base = size_mb / 50 * 90  # 90s por 50MB como base
            else:
                return -1

            if ram_gb < 8:
                base *= 1.5
            if cpu_ghz < 2.5:
                base *= 1.3

            return round(base)

        # === 3. Itera pelos arquivos ===
        resultados = []
        for file in os.listdir(self.input_directory):
            path = os.path.join(self.input_directory, file)
            if not os.path.isfile(path):
                continue

            size_mb = os.path.getsize(path) / (1024 * 1024)
            nome = file.lower()

            if nome.endswith(".csv"):
                tipo = "csv"
            elif nome.endswith(".parquet"):
                tipo = "parquet"
            elif nome.endswith(".xlsx") or nome.endswith(".xls"):
                tipo = "xlsx"
            else:
                tipo = "desconhecido"

            estimativa = get_adjusted_estimate(tipo, size_mb, cpu_freq, ram_available_gb)

            resultados.append({
                "arquivo": file,
                "tipo": tipo,
                "tamanho_mb": round(size_mb, 2),
                "estimado_segundos": estimativa,
                "cpu_ghz": round(cpu_freq, 2),
                "ram_disponivel_gb": round(ram_available_gb, 2)
            })

        for info in resultados:
            print(f"{info['arquivo']} ({info['tipo']}, {info['tamanho_mb']} MB) → "
                  f"{info['estimado_segundos']}s estimado | CPU: {info['cpu_ghz']}GHz | RAM: {info['ram_disponivel_gb']}GB")

        return resultados
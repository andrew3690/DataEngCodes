# Databricks notebook source
# MAGIC %md
# MAGIC # Build do wheel `dataeng-codes`
# MAGIC Gera o wheel a partir desta Git folder e salva em um Volume.
# MAGIC 1. Dê **Pull** na Git folder. Se o código mudou, aumente `version` no `pyproject.toml` (e faça commit).
# MAGIC 2. Ajuste `VOLUME_LIBS` e rode todas as células.
# MAGIC 3. Use o caminho impresso no final no painel *Environment* (ou no YAML do base environment).

# COMMAND ----------

import os, shutil, subprocess, sys, tempfile

# Volume de destino dos wheels (crie antes: CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.libs)
VOLUME_LIBS = "/Volumes/<catalog>/<schema>/libs"

# Pasta do projeto = pasta deste notebook (a mesma do pyproject.toml)
PROJETO = os.getcwd()
assert os.path.exists(os.path.join(PROJETO, "pyproject.toml")), f"pyproject.toml não encontrado em {PROJETO}"
print("Projeto:", PROJETO)

# COMMAND ----------

# Copia o projeto para uma pasta temporária: o build cria build/ e *.egg-info,
# e assim nada disso aparece como alteração na Git folder.
tmp = tempfile.mkdtemp()
fonte = os.path.join(tmp, "src")
shutil.copytree(PROJETO, fonte, ignore=shutil.ignore_patterns(".git", "build", "dist", "*.egg-info", "__pycache__"))
saida_local = os.path.join(tmp, "dist")

comando = [sys.executable, "-m", "pip", "wheel", "--no-deps", "-w", saida_local, fonte]
try:
    subprocess.run(comando, check=True)
except subprocess.CalledProcessError:
    # Sem acesso ao PyPI para baixar o setuptools do build: usa o setuptools já instalado no ambiente
    print("[INFO] Build isolado falhou; tentando com o setuptools do ambiente (--no-build-isolation).")
    subprocess.run(comando[:4] + ["--no-build-isolation"] + comando[4:], check=True)

wheel = next(f for f in os.listdir(saida_local) if f.endswith(".whl"))
os.makedirs(VOLUME_LIBS, exist_ok=True)
destino = os.path.join(VOLUME_LIBS, wheel)
if os.path.exists(destino):
    print(f"[ALERTA] {wheel} já existia no Volume e foi sobrescrito. "
          "Aumente a versão no pyproject.toml para o cache do ambiente reinstalar o pacote.")
shutil.copy(os.path.join(saida_local, wheel), destino)
shutil.rmtree(tmp, ignore_errors=True)

print("Wheel gerado:", destino)
# Databricks notebook source
# MAGIC %md
# MAGIC # Publicação manual do wheel `dataeng-codes`
# MAGIC Alternativa ao GitHub Actions (`.github/workflows/publish-dataeng-codes.yml`) para publicar direto do workspace.
# MAGIC
# MAGIC 1. Dê **Pull** na Git folder.
# MAGIC 2. Ajuste a configuração abaixo (só na primeira vez) e clique em **Run all**.
# MAGIC
# MAGIC Não é preciso aumentar a versão no `pyproject.toml`: cada build recebe `<versão>.post<timestamp>`,
# MAGIC então o cache do serverless sempre reinstala. O notebook gera o wheel, envia ao Volume,
# MAGIC reescreve o YAML do base environment e dispara o refresh dele.

# COMMAND ----------

import os, re, shutil, subprocess, sys, tempfile, time
from datetime import datetime, timezone

# Volume de destino (crie antes: CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.libs)
VOLUME_LIBS = "/Volumes/<catalog>/<schema>/libs"
# ID do base environment criado em Settings > Compute > Base environments ("" para pular o refresh)
BASE_ENV_ID = "dataeng-codes"
# Versão do ambiente serverless usada no base environment
ENV_VERSION = "5"

assert VOLUME_LIBS.startswith("/Volumes/") and "<" not in VOLUME_LIBS, "Configure VOLUME_LIBS com um Volume real."

# Pasta do projeto = pasta deste notebook (a mesma do pyproject.toml)
PROJETO = os.getcwd()
assert os.path.exists(os.path.join(PROJETO, "pyproject.toml")), f"pyproject.toml não encontrado em {PROJETO}"
print("Projeto:", PROJETO)

# COMMAND ----------

# Copia o projeto para uma pasta temporária: o build cria build/ e *.egg-info, e a versão é
# alterada só na cópia — nada disso aparece como alteração na Git folder.
tmp = tempfile.mkdtemp()
fonte = os.path.join(tmp, "src")
shutil.copytree(PROJETO, fonte, ignore=shutil.ignore_patterns(".git", "build", "dist", "*.egg-info", "__pycache__", "*.whl"))
saida_local = os.path.join(tmp, "dist")

pyproject = os.path.join(fonte, "pyproject.toml")
with open(pyproject, encoding="utf-8") as f:
    texto = f.read()
m = re.search(r'^version\s*=\s*"([^"]+)"', texto, flags=re.M)
assert m, "Linha 'version = \"...\"' não encontrada no pyproject.toml"
VERSAO = f"{m.group(1)}.post{datetime.now(timezone.utc):%Y%m%d%H%M%S}"
with open(pyproject, "w", encoding="utf-8") as f:
    f.write(texto[:m.start(1)] + VERSAO + texto[m.end(1):])
print("Versão do build:", VERSAO)

comando = [sys.executable, "-m", "pip", "wheel", "--no-deps", "-w", saida_local, fonte]
try:
    subprocess.run(comando, check=True)
except subprocess.CalledProcessError:
    # Sem acesso ao PyPI para baixar o setuptools do build: usa o setuptools já instalado no ambiente
    print("[INFO] Build isolado falhou; tentando com o setuptools do ambiente (--no-build-isolation).")
    subprocess.run(comando[:4] + ["--no-build-isolation"] + comando[4:], check=True)

wheel = next(f for f in os.listdir(saida_local) if f.endswith(".whl"))
destino = os.path.join(VOLUME_LIBS, wheel)
shutil.copy(os.path.join(saida_local, wheel), destino)
shutil.rmtree(tmp, ignore_errors=True)
print("Wheel publicado:", destino)

# COMMAND ----------

# YAML do base environment, sempre no mesmo caminho: o base environment aponta para este arquivo
yaml_path = os.path.join(VOLUME_LIBS, "dataeng_base_env.yml")
with open(yaml_path, "w", encoding="utf-8") as f:
    f.write(f"environment_version: '{ENV_VERSION}'\ndependencies:\n  - {destino}\n")
print("Base environment YAML:", yaml_path)
print(open(yaml_path, encoding="utf-8").read())

# COMMAND ----------

# Refresh do base environment (equivale a Settings > Compute > Base environments > ⋮ > Refresh)
if BASE_ENV_ID:
    from databricks.sdk import WorkspaceClient
    api = WorkspaceClient().api_client
    op = api.do("POST", f"/api/environments/v1/workspace-base-environments/{BASE_ENV_ID}/refresh", body={})
    nome_op = (op or {}).get("name")
    print("Refresh disparado:", nome_op)
    for _ in range(60):
        if not nome_op:
            break
        st = api.do("GET", f"/api/environments/v1/{nome_op}")
        if st.get("done"):
            erro = (st.get("error") or {})
            if erro.get("message") or erro.get("error_code"):
                raise RuntimeError(f"Falha ao materializar o base environment: {erro}")
            print(f"[OK] Base environment '{BASE_ENV_ID}' pronto com {VERSAO}. "
                  "Notebooks abertos precisam reiniciar a sessão para pegar a versão nova.")
            break
        time.sleep(15)
    else:
        print("[ALERTA] Refresh ainda em andamento; acompanhe em Settings > Compute > Base environments.")
else:
    print("[INFO] BASE_ENV_ID vazio: crie o base environment apontando para o YAML acima.")

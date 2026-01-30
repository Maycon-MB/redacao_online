import requests
import psycopg2  # Biblioteca para PostgreSQL
import logging
import time
from datetime import datetime

# Lógica para o nome da tabela
ANO_SUFIXO = str(datetime.now().year)[2:] 
TABELA_ALUNOS = f"alunos_{ANO_SUFIXO}_geral"
# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Configuração da API
HEADERS = {
    "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4",
    "Accept": "application/json"
}

# Configuração do banco de dados PostgreSQL
db_config = {
    "dbname": "BOLETOS",
    "user": "postgres",
    "password": "teste",
    "host": "192.168.1.163",
    "port": "5432"
}

unidades_api = {
    "Bento Ribeiro": 35022, "Madureira": 35023, "Santa Cruz": 35024, "Cascadura": 35025,
    "Taquara": 35026, "Nilopolis": 35027, "Seropédica": 35028, "Barra da Tijuca": 35029,
    "Campo Grande": 35030, "Maricá": 35031, "Ilha do Governador": 35032,
    "Freguesia": 35033, "Recreio dos Bandeirantes": 35034
}

siglas_unidades = {
    "Bento Ribeiro": "BR", "Madureira": "MA", "Santa Cruz": "SC", "Cascadura": "CD",
    "Taquara": "TQ", "Nilopolis": "NP", "Seropédica": "SP", "Barra da Tijuca": "BT",
    "Campo Grande": "CG", "Maricá": "MC", "Ilha do Governador": "IG",
    "Freguesia": "FG", "Recreio dos Bandeirantes": "RB"
}

codigo_para_unidade = {
    "01": "Bento Ribeiro", "02": "Madureira", "03": "Santa Cruz", "04": "Cascadura",
    "05": "Taquara", "06": "Nilopolis", "09": "Seropédica", "10": "Barra da Tijuca",
    "11": "Campo Grande", "14": "Maricá", "15": "Ilha do Governador",
    "16": "Freguesia", "17": "Recreio dos Bandeirantes"
}

turmas_cache = {}

# 🔹 Turmas API com cache
def obter_turmas_api(unit_id, nome_turma):
    chave = (unit_id, nome_turma)
    if chave in turmas_cache:
        return turmas_cache[chave]

    try:
        resp = requests.get(
            "https://app.redacaonline.com.br/api/classes",
            headers=HEADERS,
            params={"name": nome_turma, "unit_id": unit_id},
            timeout=10
        )
        turmas = resp.json()
        if isinstance(turmas, list):
            for turma in turmas:
                if turma["name"] == nome_turma:
                    turmas_cache[chave] = turma["id"]
                    return turma["id"]
        logging.warning(f"Turma '{nome_turma}' não encontrada na unidade {unit_id}.")
    except Exception as e:
        logging.exception(f"Erro ao buscar turma: {e}")
    return None

# 🔹 Listar alunos com paginação
def listar_alunos():
    url = "https://app.redacaonline.com.br/api/students"
    alunos_api = {}
    page = 1
    while True:
        try:
            resp = requests.get(url, headers=HEADERS, params={"page": page}, timeout=30)
            if resp.status_code != 200:
                logging.error(f"Erro ao listar alunos: {resp.text}")
                break
            data = resp.json()
            for aluno in data.get("data", []):
                alunos_api[aluno["external_id"]] = aluno
            if not data.get("next_page_url"):
                break
            page += 1
            time.sleep(0.3)
        except Exception as e:
            logging.exception(f"Erro na listagem de alunos: {e}")
            break
    return alunos_api

# 🔹 CRUD alunos
def remover_aluno(student_id, nome):
    url = f"https://app.redacaonline.com.br/api/students/{student_id}"
    r = requests.delete(url, headers=HEADERS)
    if r.status_code == 204:
        logging.info(f"❌ Removido: {nome}")
    else:
        logging.warning(f"Erro ao remover {nome}: {r.status_code} - {r.text}")

def atualizar_aluno(student_id, nome, email, class_id, external_id):
    url = f"https://app.redacaonline.com.br/api/students/{student_id}"
    payload = {"name": nome, "email": email, "class_id": class_id, "external_id": str(external_id)}
    r = requests.put(url, headers=HEADERS, json=payload)
    if r.status_code == 200:
        logging.info(f"🔄 Atualizado: {nome}")
    else:
        logging.warning(f"Erro ao atualizar {nome}: {r.status_code} - {r.text}")

def inserir_aluno(nome, matricula, class_id):
    url = "https://app.redacaonline.com.br/api/students"
    email = f"{matricula}@alunos.smrede.com.br"
    payload = {"name": nome, "email": email, "class_id": class_id, "external_id": str(matricula)}
    r = requests.post(url, headers=HEADERS, json=payload)
    if r.status_code == 200:
        logging.info(f"✅ Inserido: {nome}")
    else:
        logging.warning(f"Erro ao inserir {nome}: {r.status_code} - {r.text}")

# 🔹 Processo principal
def processar_alunos():
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT unidade, sit, matricula, nome, turma
                FROM {TABELA_ALUNOS}
                WHERE turma::NUMERIC >= 11900::NUMERIC
            """)
            alunos = cursor.fetchall()

    alunos_api_dict = listar_alunos()
    alteracoes_feitas = False

    for unidade_cod, sit, matricula, nome, turma in alunos:
        unidade_cod = unidade_cod.strip().zfill(2)
        nome_unidade = codigo_para_unidade.get(unidade_cod)
        if not nome_unidade:
            continue

        sigla = siglas_unidades.get(nome_unidade)
        unit_id = unidades_api.get(nome_unidade)
        if not sigla or not unit_id:
            continue

        nome_turma = f"{sigla} {turma}"
        class_id = obter_turmas_api(unit_id, nome_turma)
        if not class_id:
            continue

        aluno_api = alunos_api_dict.get(str(matricula))

        if int(sit) in [2, 4]:
            if aluno_api:
                remover_aluno(aluno_api["id"], aluno_api["name"])
                alteracoes_feitas = True
        elif not aluno_api:
            inserir_aluno(nome, matricula, class_id)
            alteracoes_feitas = True
        elif aluno_api["class_id"] != class_id or aluno_api["name"] != nome:
            atualizar_aluno(aluno_api["id"], nome, aluno_api["email"], class_id, matricula)
            alteracoes_feitas = True

    if not alteracoes_feitas:
        logging.info("✅ Todos os alunos já estão corretos na API. Nenhuma alteração necessária.")
    else:
        logging.info("🔄 Alterações concluídas com sucesso.")

# Execução
def main():
    processar_alunos()

if __name__ == "__main__":
    main()
import requests
import psycopg2
import logging
import time
from datetime import datetime
from collections import defaultdict # Para contar agrupamentos facilmente

# ================= CONFIGURAÇÕES =================
ANO_SUFIXO = str(datetime.now().year)[2:] 
TABELA_ALUNOS = f"alunos_{ANO_SUFIXO}_geral"

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# SEU TOKEN
HEADERS = {
    "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4",
    "Accept": "application/json"
}

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

# Cache para busca rápida: (unit_id, nome) -> class_id
turmas_cache = {}
# Cache REVERSO para relatório: class_id -> nome
turmas_reversas = {} 

# Dicionário para contar as trocas: { ("Turma A", "Turma B"): quantidade }
stats_trocas = defaultdict(int)

# 🔹 Carregar TODAS as turmas
def carregar_todas_turmas():
    logging.info("🏗️ Construindo mapa de turmas (Cache)...")
    url = "https://app.redacaonline.com.br/api/classes"
    
    for nome_unidade, unit_id in unidades_api.items():
        try:
            resp = requests.get(url, headers=HEADERS, params={"unit_id": unit_id}, timeout=15)
            if resp.status_code == 200:
                turmas = resp.json()
                for t in turmas:
                    # Guardamos TODAS no reverso para saber o nome das turmas antigas/deletadas no relatório
                    turmas_reversas[t["id"]] = t["name"]

                    # Mas no cache de USO (para onde enviamos alunos), SÓ entram as ativas
                    if t.get("deleted_at") is not None:
                        continue 
                        
                    nome = t["name"].strip()
                    chave = (unit_id, nome)
                    turmas_cache[chave] = t["id"]
        except Exception as e:
            logging.error(f"Erro ao carregar turmas de {nome_unidade}: {e}")
            
    logging.info(f"✅ Cache construído com {len(turmas_cache)} turmas ATIVAS.")

def obter_class_id_cache(unit_id, nome_turma):
    return turmas_cache.get((unit_id, nome_turma))

# 🔹 Listar alunos
def listar_alunos():
    url = "https://app.redacaonline.com.br/api/students"
    alunos_api = {}
    page = 1
    logging.info("⏳ Carregando alunos da API...")
    while True:
        try:
            resp = requests.get(url, headers=HEADERS, params={"page": page}, timeout=20)
            if resp.status_code != 200:
                logging.error(f"Erro ao listar alunos: {resp.text}")
                break
            
            data = resp.json()
            lista = data.get("data", [])
            
            if not lista:
                break

            for aluno in lista:
                if aluno.get("external_id"):
                    chave = str(aluno["external_id"]).strip()
                    alunos_api[chave] = aluno
            
            if not data.get("next_page_url"):
                break
            page += 1
        except Exception as e:
            logging.exception(f"Erro na listagem de alunos: {e}")
            break
    
    logging.info(f"✅ {len(alunos_api)} alunos carregados da API.")
    return alunos_api

# 🔹 CRUD
def remover_aluno(student_id, nome):
    url = f"https://app.redacaonline.com.br/api/students/{student_id}"
    try:
        r = requests.delete(url, headers=HEADERS, timeout=10)
        if r.status_code == 204:
            logging.info(f"❌ Removido: {nome}")
    except:
        pass

def atualizar_aluno(student_id, nome, email, class_id, external_id, old_class_id):
    url = f"https://app.redacaonline.com.br/api/students/{student_id}"
    payload = {
        "name": nome, 
        "email": email, 
        "class_id": int(class_id), 
        "external_id": str(external_id)
    }
    try:
        r = requests.put(url, headers=HEADERS, json=payload, timeout=10)
        if r.status_code == 200:
            if int(old_class_id) != int(class_id):
                # LÓGICA DE RELATÓRIO
                nome_antiga = turmas_reversas.get(int(old_class_id), f"ID_{old_class_id}")
                nome_nova = turmas_reversas.get(int(class_id), f"ID_{class_id}")
                
                logging.info(f"🔄 TROCA DE TURMA: {nome} | {nome_antiga} -> {nome_nova}")
                
                # Incrementa contador para o relatório final
                stats_trocas[(nome_antiga, nome_nova)] += 1
            else:
                logging.info(f"🔄 Atualizado dados cadastrais: {nome}")
        else:
            logging.warning(f"Erro ao atualizar {nome}: {r.status_code}")
    except Exception as e:
        logging.error(f"Exceção update: {e}")

def inserir_aluno(nome, matricula, class_id):
    url = "https://app.redacaonline.com.br/api/students"
    email = f"{matricula}@alunos.smrede.com.br"
    payload = {
        "name": nome, 
        "email": email, 
        "class_id": int(class_id), 
        "external_id": str(matricula)
    }
    try:
        r = requests.post(url, headers=HEADERS, json=payload, timeout=10)
        if r.status_code in [200, 201]:
            logging.info(f"✅ Inserido: {nome}")
        else:
            logging.warning(f"Erro ao inserir {nome}: {r.status_code}")
    except Exception as e:
        logging.error(f"Exceção insert: {e}")

# 🔹 Processo principal
def processar_alunos():
    carregar_todas_turmas()

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT unidade, sit, matricula, nome, turma
                    FROM {TABELA_ALUNOS}
                    WHERE turma ~ '^[0-9]+$' AND turma::NUMERIC >= 11900
                """)
                alunos = cursor.fetchall()
    except Exception as e:
        logging.critical(f"Erro banco: {e}")
        return

    alunos_api_dict = listar_alunos()
    alteracoes_feitas = False

    for unidade_cod, sit, matricula, nome, turma in alunos:
        unidade_cod = str(unidade_cod).strip().zfill(2)
        matricula = str(matricula).strip()
        sit = str(sit).strip()
        nome = str(nome).strip()
        
        try:
            sit_int = int(sit)
        except:
            continue

        nome_unidade = codigo_para_unidade.get(unidade_cod)
        if not nome_unidade: continue

        sigla = siglas_unidades.get(nome_unidade)
        unit_id = unidades_api.get(nome_unidade)
        if not sigla or not unit_id: continue

        nome_turma = f"{sigla} {turma}"
        
        # Cache só tem turmas ATIVAS
        class_id = obter_class_id_cache(unit_id, nome_turma)
        
        if not class_id:
            continue

        aluno_api = alunos_api_dict.get(matricula)

        if sit_int in [2, 4]:
            if aluno_api:
                remover_aluno(aluno_api["id"], aluno_api["name"])
                alteracoes_feitas = True
        elif not aluno_api:
            inserir_aluno(nome, matricula, class_id)
            alteracoes_feitas = True
        else:
            api_class_id = int(aluno_api.get("class_id", 0))
            api_nome = str(aluno_api.get("name", "")).strip()
            
            if api_class_id != int(class_id) or api_nome != nome:
                atualizar_aluno(aluno_api["id"], nome, aluno_api["email"], class_id, matricula, api_class_id)
                alteracoes_feitas = True

    # ================= RELATÓRIO FINAL =================
    print("\n" + "="*50)
    print("📊 RESUMO DE MIGRAÇÃO DE ALUNOS")
    print("="*50)
    
    if not stats_trocas:
        print("Nenhuma troca de turma foi necessária nesta execução.")
    else:
        total_movidos = 0
        # Ordena para ficar bonito no log
        for (origem, destino), qtd in sorted(stats_trocas.items()):
            print(f"🔸 {origem:<15} ➡️  {destino:<15} : {qtd} alunos")
            total_movidos += qtd
        
        print("-" * 50)
        print(f"✅ TOTAL DE ALUNOS MOVIDOS: {total_movidos}")
    print("="*50 + "\n")

    if not alteracoes_feitas:
        logging.info("Sincronização finalizada sem alterações.")
    else:
        logging.info("Sincronização finalizada com sucesso.")

def main():
    processar_alunos()

if __name__ == "__main__":
    main()
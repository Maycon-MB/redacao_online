import requests
import psycopg2
from psycopg2.extras import execute_values
import logging
import time
import hashlib
from datetime import datetime
from collections import defaultdict

# ================= CONFIGURAÇÕES =================
ANO_ATUAL = datetime.now().year
TABELA_ORIGEM = f"alunos_{str(ANO_ATUAL)[2:]}_geral"

# Formatação limpa para o log tabular
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

class RedacaoOnlineManager:
    def __init__(self):
        self.headers = {
            "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4",
            "Accept": "application/json"
        }
        self.db_config = {
            "dbname": "BOLETOS", "user": "postgres", "password": "teste",
            "host": "192.168.1.163", "port": "5432"
        }

        self.unidades_api = {
            "Bento Ribeiro": 35022, "Madureira": 35023, "Santa Cruz": 35024, "Cascadura": 35025,
            "Taquara": 35026, "Nilopolis": 35027, "Seropédica": 35028, "Barra da Tijuca": 35029,
            "Campo Grande": 35030, "Maricá": 35031, "Ilha do Governador": 35032,
            "Freguesia": 35033, "Recreio dos Bandeirantes": 35034
        }
        self.siglas = {
            "Bento Ribeiro": "BR", "Madureira": "MA", "Santa Cruz": "SC", "Cascadura": "CD",
            "Taquara": "TQ", "Nilopolis": "NP", "Seropédica": "SP", "Barra da Tijuca": "BT",
            "Campo Grande": "CG", "Maricá": "MC", "Ilha do Governador": "IG",
            "Freguesia": "FG", "Recreio dos Bandeirantes": "RB"
        }
        self.codigos_unidade = {
            "01": "Bento Ribeiro", "02": "Madureira", "03": "Santa Cruz", "04": "Cascadura",
            "05": "Taquara", "06": "Nilopolis", "09": "Seropédica", "10": "Barra da Tijuca",
            "11": "Campo Grande", "14": "Maricá", "15": "Ilha do Governador",
            "16": "Freguesia", "17": "Recreio dos Bandeirantes"
        }

        self.turmas_cache = {}
        self.turmas_reversas = {}
        self.stats_trocas = defaultdict(int)

    def criar_tabelas(self):
        queries = [
            """CREATE TABLE IF NOT EXISTS turmas_redacao (
                id_api INTEGER PRIMARY KEY,
                nome TEXT NOT NULL,
                unit_id_api INTEGER,
                ativo BOOLEAN DEFAULT TRUE,
                ultima_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );""",
            """CREATE TABLE IF NOT EXISTS alunos_redacao (
                matricula TEXT,
                ano_letivo INTEGER,
                id_api_aluno INTEGER,
                nome TEXT,
                id_api_turma INTEGER REFERENCES turmas_redacao(id_api),
                hash_estado TEXT,
                data_sync TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (matricula, ano_letivo)
            );"""
        ]
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                for q in queries: cur.execute(q)

    def atualizar_mapa_turmas(self):
        logging.info("🏗️  Sincronizando mapa de turmas...")
        url = "https://app.redacaonline.com.br/api/classes"
        dados_upsert = []

        for nome_unid, unit_id in self.unidades_api.items():
            try:
                r = requests.get(url, headers=self.headers, params={"unit_id": unit_id}, timeout=5)
                if r.status_code == 200:
                    turmas = r.json()
                    for t in turmas:
                        ativa = t.get("deleted_at") is None
                        dados_upsert.append((t["id"], t["name"], unit_id, ativa, datetime.now()))
                        self.turmas_reversas[t["id"]] = t["name"]
                        if ativa:
                            self.turmas_cache[(unit_id, t["name"].strip())] = t["id"]
            except Exception:
                pass

        if dados_upsert:
            sql = """
                INSERT INTO turmas_redacao (id_api, nome, unit_id_api, ativo, ultima_atualizacao)
                VALUES %s ON CONFLICT (id_api) DO UPDATE SET 
                nome=EXCLUDED.nome, ativo=EXCLUDED.ativo, ultima_atualizacao=EXCLUDED.ultima_atualizacao
            """
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, dados_upsert)

    def gerar_hash(self, nome, id_turma):
        return hashlib.md5(f"{nome.strip()}|{id_turma}".encode('utf-8')).hexdigest()

    def processar(self):
        self.criar_tabelas()
        self.atualizar_mapa_turmas()

        logging.info("📥 Carregando estado do banco local e banco de produção...")
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT matricula, id_api_aluno, id_api_turma, hash_estado FROM alunos_redacao WHERE ano_letivo = %s", (ANO_ATUAL,))
                estado_local = {r[0]: {"id_api": r[1], "id_turma": r[2], "hash": r[3]} for r in cur.fetchall()}
                
                cur.execute(f"SELECT unidade, sit, matricula, nome, turma FROM {TABELA_ORIGEM} WHERE turma ~ '^[0-9]+$' AND turma::NUMERIC >= 11900")
                alunos_origem = cur.fetchall()

        logging.info("⚙️  Iniciando comparação de hashes e envio para API...")
        alteracoes = False
        upsert_banco_local = []
        delete_banco_local = []

        for unid_cod, sit, mat, nome, turma_n in alunos_origem:
            mat = str(mat).strip()
            nome = str(nome).strip()
            unid_cod = str(unid_cod).zfill(2)
            
            nome_unid = self.codigos_unidade.get(unid_cod)
            if not nome_unid: continue
            
            unit_id = self.unidades_api[nome_unid]
            sigla = self.siglas[nome_unid]
            nome_turma = f"{sigla} {turma_n}"
            id_turma_alvo = self.turmas_cache.get((unit_id, nome_turma))
            
            if not id_turma_alvo: continue

            aluno_api = estado_local.get(mat)
            novo_hash = self.gerar_hash(nome, id_turma_alvo)

            if int(sit) in [2, 4]:
                if aluno_api:
                    turma_antiga = self.turmas_reversas.get(aluno_api["id_turma"], "N/A")
                    logging.info(f"❌ DELETE | {sigla} | Mat: {mat:<9} | {nome:<35} | Removido da turma {turma_antiga}")
                    if self.api_delete(aluno_api["id_api"], nome):
                        delete_banco_local.append((mat, ANO_ATUAL))
                        alteracoes = True
            
            elif not aluno_api:
                logging.info(f"➕ INSERT | {sigla} | Mat: {mat:<9} | {nome:<35} | Inserido na turma {nome_turma}")
                res = self.api_insert(nome, mat, id_turma_alvo)
                if res:
                    upsert_banco_local.append((mat, ANO_ATUAL, res["id"], nome, id_turma_alvo, novo_hash, datetime.now()))
                    alteracoes = True
            
            elif str(aluno_api["hash"]) != novo_hash:
                turma_antiga = self.turmas_reversas.get(aluno_api["id_turma"], "N/A")
                
                # Diferenciar se foi troca de turma ou apenas correção de nome
                if aluno_api["id_turma"] != id_turma_alvo:
                    motivo = f"{turma_antiga} -> {nome_turma}"
                else:
                    motivo = "Nome Atualizado"
                    
                logging.info(f"🔄 UPDATE | {sigla} | Mat: {mat:<9} | {nome:<35} | {motivo}")
                
                if self.api_update(aluno_api["id_api"], nome, mat, id_turma_alvo, aluno_api["id_turma"]):
                    upsert_banco_local.append((mat, ANO_ATUAL, aluno_api["id_api"], nome, id_turma_alvo, novo_hash, datetime.now()))
                    alteracoes = True

        if upsert_banco_local or delete_banco_local:
            logging.info(f"💾 Persistindo estado local ({len(upsert_banco_local)} upserts, {len(delete_banco_local)} deletes)...")
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    if upsert_banco_local:
                        sql_upsert = """
                            INSERT INTO alunos_redacao (matricula, ano_letivo, id_api_aluno, nome, id_api_turma, hash_estado, data_sync)
                            VALUES %s ON CONFLICT (matricula, ano_letivo) 
                            DO UPDATE SET nome=EXCLUDED.nome, id_api_turma=EXCLUDED.id_api_turma, hash_estado=EXCLUDED.hash_estado, data_sync=EXCLUDED.data_sync
                        """
                        execute_values(cur, sql_upsert, upsert_banco_local)
                    if delete_banco_local:
                        cur.executemany("DELETE FROM alunos_redacao WHERE matricula = %s AND ano_letivo = %s", delete_banco_local)

        self.exibir_relatorio(alteracoes)

    def api_insert(self, nome, mat, id_turma):
        url = "https://app.redacaonline.com.br/api/students"
        payload = {"name": nome, "email": f"{mat}@alunos.smrede.com.br", "class_id": id_turma, "external_id": str(mat)}
        r = requests.post(url, headers=self.headers, json=payload, timeout=10)
        return r.json() if r.status_code in [200, 201] else None

    def api_update(self, id_api, nome, mat, id_turma, id_antiga):
        url = f"https://app.redacaonline.com.br/api/students/{id_api}"
        payload = {"name": nome, "email": f"{mat}@alunos.smrede.com.br", "class_id": id_turma, "external_id": str(mat)}
        r = requests.put(url, headers=self.headers, json=payload, timeout=10)
        if r.status_code == 200:
            if id_turma != id_antiga:
                self.stats_trocas[(self.turmas_reversas.get(id_antiga, "Antiga"), self.turmas_reversas.get(id_turma, "Nova"))] += 1
            return True
        return False

    def api_delete(self, id_api, nome):
        r = requests.delete(f"https://app.redacaonline.com.br/api/students/{id_api}", headers=self.headers, timeout=10)
        return r.status_code == 204

    def exibir_relatorio(self, alteracoes):
        print("\n" + "="*70 + "\n📊 RESUMO DE SINCRONIZAÇÃO (API REDAÇÃO ONLINE)\n" + "="*70)
        if not self.stats_trocas and not alteracoes:
            print("Nenhuma alteração detectada no banco de origem.")
        else:
            for (orig, dest), qtd in sorted(self.stats_trocas.items()):
                print(f"🔸 Troca de Turma: {orig:<15} ➡️  {dest:<15} : {qtd} alunos afetados")
        print("="*70)
        logging.info("🏁 Sincronização concluída.")

if __name__ == "__main__":
    manager = RedacaoOnlineManager()
    manager.processar()
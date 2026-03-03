import requests
import psycopg2
import logging

# ================= CONFIGURAÇÕES =================
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}
DB_CONFIG = {"dbname": "BOLETOS", "user": "postgres", "password": "teste", "host": "192.168.1.163", "port": "5432"}
ANO_LETIVO = 2026

def popular_banco_local():
    url = "https://app.redacaonline.com.br/api/students"
    page = 1
    total_processado = 0
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print(f"📡 Iniciando migração da API para o local (Ano {ANO_LETIVO})...")

    while True:
        try:
            resp = requests.get(url, headers=HEADERS, params={"page": page}, timeout=20)
            
            if resp.status_code != 200:
                print(f"❌ Erro na página {page}: Status {resp.status_code}")
                break
            
            data = resp.json()
            alunos = data.get("data", [])
            
            if not alunos:
                print(f"ℹ️ Fim dos dados na página {page}.")
                break

            for a in alunos:
                # O external_id é a nossa matrícula
                matricula = str(a.get("external_id", "")).strip()
                
                # Se não tiver matrícula, não ignoramos o loop, só pulamos o aluno
                if not matricula or matricula == "None":
                    continue
                
                # Geramos o hash conforme o envio_api.py fará
                nome = str(a.get("name", "")).strip()
                id_turma = a.get("class_id")
                hash_val = hash(f"{nome}|{id_turma}")
                
                cur.execute("""
                    INSERT INTO alunos_redacao (matricula, ano_letivo, id_api_aluno, nome, id_api_turma, hash_estado)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (matricula, ano_letivo) DO UPDATE SET
                        id_api_aluno = EXCLUDED.id_api_aluno,
                        id_api_turma = EXCLUDED.id_api_turma,
                        hash_estado = EXCLUDED.hash_estado
                """, (matricula, ANO_LETIVO, a['id'], nome, id_turma, str(hash_val)))
                total_processado += 1

            print(f"✅ Página {page} processada. ({len(alunos)} alunos encontrados)")
            
            # Verificação de próxima página conforme padrão da API
            if not data.get("next_page_url"):
                break
                
            page += 1
            
        except Exception as e:
            print(f"💥 Erro crítico no processamento: {e}")
            break

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n🏁 Migração concluída! Total de registros salvos/atualizados: {total_processado}")

if __name__ == "__main__":
    popular_banco_local()
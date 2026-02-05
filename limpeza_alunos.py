import requests
import logging
import time

# ================= CONFIGURAÇÕES =================
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

api_para_sigla = {
    35022: "BR", 35023: "MA", 35024: "SC", 35025: "CD",
    35026: "TQ", 35027: "NP", 35028: "SP", 35029: "BT",
    35030: "CG", 35031: "MC", 35032: "IG",
    35033: "FG", 35034: "RB"
}

logging.basicConfig(level=logging.INFO, format="%(message)s")

def contar_alunos_por_turma():
    url = "https://app.redacaonline.com.br/api/students"
    
    # Dicionário para contar: { class_id: quantidade }
    contagem = {}
    total_alunos = 0
    page = 1
    
    print("⏳ Baixando e contando TODOS os alunos da API (isso pode levar 1 minuto)...")
    
    while True:
        try:
            resp = requests.get(url, headers=HEADERS, params={"page": page}, timeout=20)
            if resp.status_code != 200:
                print(f"Erro ao ler página {page}")
                break
                
            data = resp.json()
            lista = data.get("data", [])
            if not lista:
                break
                
            for aluno in lista:
                c_id = aluno.get("class_id")
                if c_id:
                    # Garante que c_id seja int para chave do dict
                    c_id = int(c_id)
                    contagem[c_id] = contagem.get(c_id, 0) + 1
                    total_alunos += 1
            
            if not data.get("next_page_url"):
                break
            page += 1
            print(f"Página {page-1} processada...", end="\r")
            
        except Exception as e:
            print(f"Erro de conexão: {e}")
            break
            
    print(f"\n✅ Total de Alunos Ativos na API: {total_alunos}")
    return contagem

def listar_turmas_com_contagem(contagem_alunos):
    url = "https://app.redacaonline.com.br/api/classes"
    
    print("\nRELATÓRIO DE DISCREPÂNCIA (API)")
    print(f"{'UNID.':<6} | {'NOME TURMA':<15} | {'ID TURMA':<8} | {'QTD API':<8} | {'STATUS'}")
    print("-" * 65)

    for unit_id, sigla in api_para_sigla.items():
        try:
            resp = requests.get(url, headers=HEADERS, params={"unit_id": unit_id}, timeout=10)
            if resp.status_code == 200:
                turmas = resp.json()
                if not turmas:
                    continue

                turmas_ordenadas = sorted(turmas, key=lambda x: x['name'])

                for t in turmas_ordenadas:
                    t_id = int(t['id'])
                    nome = t['name']
                    qtd = contagem_alunos.get(t_id, 0)
                    
                    # Análise visual rápida
                    status = "OK"
                    if qtd == 0:
                        status = "VAZIA ⚠️"
                    elif qtd < 5:
                        status = "SUSPEITA ⚠️"
                        
                    print(f"{sigla:<6} | {nome:<15} | {t_id:<8} | {qtd:<8} | {status}")
            time.sleep(0.1)
        except:
            pass

if __name__ == "__main__":
    contagem = contar_alunos_por_turma()
    listar_turmas_com_contagem(contagem)
import requests
import logging

# ================= CONFIGURAÇÕES =================
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json"
}

# IDs identificados no relatório de auditoria como VAZIOS (0 alunos)
# TQ 11912, CG 11912, SC 11921, CD 11921, CG 11921, CG 22312, MC 22213
TURMAS_ALVO = [
    39857, 
    39858, 
    35209, 
    35223, 
    35245, 
    39733, 
    35257
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def executar_limpeza():
    print(f"🔪 Iniciando remoção de {len(TURMAS_ALVO)} turmas vazias...")
    
    for class_id in TURMAS_ALVO:
        url = f"https://app.redacaonline.com.br/api/classes/{class_id}"
        
        try:
            # DELETE https://app.redacaonline.com.br/api/classes/{class_id}
            response = requests.delete(url, headers=HEADERS, timeout=10)
            
            if response.status_code == 204:
                logging.info(f"✅ [SUCESSO] Turma {class_id} removida.")
            elif response.status_code == 404:
                logging.warning(f"⚠️ [IGNORADO] Turma {class_id} já não existia.")
            elif response.status_code == 400:
                logging.error(f"❌ [ERRO] Turma {class_id} não pode ser removida (pode ter alunos ocultos).")
            else:
                logging.error(f"❌ [FALHA] Erro {response.status_code} ao remover {class_id}: {response.text}")
                
        except Exception as e:
            logging.critical(f"Erro de conexão com ID {class_id}: {e}")

if __name__ == "__main__":
    executar_limpeza()
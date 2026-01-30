import requests
import csv
import logging

# Configuração
URL = "https://app.redacaonline.com.br/api/students"
HEADERS = {
    "Accept": "application/json",
    "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4"
}
CSV_FILE = "alunos_matricula_id.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def fetch_students():
    """Busca todos os alunos paginando a API"""
    students = []
    page = 1

    with requests.Session() as session:
        session.headers.update(HEADERS)

        while True:
            try:
                logging.info(f"🔄 Página {page}")
                resp = session.get(URL, params={"page": page, "limit": 100}, timeout=30)
                resp.raise_for_status()

                data = resp.json()
                alunos = data.get("data", [])
                logging.info(f"Total recebidos: {len(alunos)}")

                for st in alunos:
                    students.append({
                        "matricula": st.get("external_id", ""),
                        "aluno_id": st.get("id", "")
                    })

                if not data.get("next_page_url"):
                    break
                page += 1

            except requests.RequestException as e:
                logging.error(f"Erro na requisição: {e}")
                break
            except ValueError:
                logging.error("Erro ao decodificar JSON")
                break
    return students

def save_to_csv(students, file_path):
    """Salva lista de alunos em CSV"""
    if not students:
        logging.warning("Nenhum aluno encontrado.")
        return
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["matricula", "aluno_id"])
        writer.writeheader()
        writer.writerows(students)
    logging.info(f"✅ Dados salvos em {file_path}")

if __name__ == "__main__":
    alunos = fetch_students()
    logging.info(f"Total de alunos: {len(alunos)}")
    save_to_csv(alunos, CSV_FILE)
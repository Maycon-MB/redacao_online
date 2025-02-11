import requests

# Configuração da API
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4"
headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

# 6. Inserir aluno
def inserir_aluno(nome, matricula, class_id):
    url = "https://app.redacaonline.com.br/api/students"
    email = f"{matricula}@alunos.smrede.com.br"  # Gera o e-mail automaticamente
    payload = {"name": nome, "email": email, "class_id": class_id, "external_id": str(matricula)}
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        # Obter os dados do aluno retornados pela API
        aluno_data = response.json()
        aluno_id = aluno_data.get("id", "ID não encontrado")
        print(f"Aluno {nome} inserido com sucesso! ID: {aluno_id}")
    elif response.status_code == 400:
        print(f"Erro ao inserir aluno: {response.text}")
    else:
        print(f"Erro inesperado: {response.status_code} - {response.text}")

# 7. Obter o class_id para uma turma específica
def obter_class_id(nome_turma, unit_id):
    url = "https://app.redacaonline.com.br/api/classes"
    payload = {"name": nome_turma, "unit_id": unit_id}
    response = requests.get(url, headers=headers, params=payload)
    turmas = response.json()
    if turmas:
        return turmas[0]["id"]
    return None

# Teste de cadastro de um aluno
def cadastrar_aluno_teste():
    # Hardcode do aluno
    nome_aluno = "Thiago"
    matricula_aluno = 9999999
    unit_id = 34993  # Santa Monica (como exemplo)

    # Obter o class_id para a turma "CONCEITO A"
    class_id = obter_class_id("CONCEITO A", unit_id)
    
    if class_id:
        inserir_aluno(nome_aluno, matricula_aluno, class_id)
    else:
        print("Turma 'CONCEITO A' não encontrada.")

# Executar o cadastro de aluno de teste
if __name__ == "__main__":
    cadastrar_aluno_teste()

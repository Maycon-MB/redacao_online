import requests

# Configuração do token de autorização
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4"
HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

# Buscar todos os alunos com suporte à paginação
def listar_alunos():
    url = "https://app.redacaonline.com.br/api/students"
    alunos = []
    page = 1

    while True:
        response = requests.get(url, headers=HEADERS, params={"page": page})
        if response.status_code != 200:
            print(f"Erro ao listar alunos: {response.text}")
            return []
        
        data = response.json()
        alunos.extend(data.get("data", []))
        
        if "next_page_url" not in data or not data["next_page_url"]:
            break  # Sai do loop quando não há mais páginas
        
        page += 1
    
    return alunos

# Obter student_id pelo nome do aluno
def obter_student_id(nome_aluno):
    alunos = listar_alunos()
    for aluno in alunos:
        if aluno["name"].lower() == nome_aluno.lower():
            return aluno["id"]
    print(f"Aluno '{nome_aluno}' não encontrado.")
    return None

# Obter ID da turma pelo nome
def obter_class_id(nome_turma, unit_id):
    url = "https://app.redacaonline.com.br/api/classes"
    params = {"name": nome_turma, "unit_id": unit_id}
    response = requests.get(url, headers=HEADERS, params=params)

    if response.status_code == 200:
        turmas = response.json()
        return turmas[0]["id"] if turmas else None
    else:
        print(f"Erro ao buscar turmas: {response.text}")
        return None

# Atualizar aluno na API
def atualizar_aluno(student_id, nome, email, class_id, external_id):
    url = f"https://app.redacaonline.com.br/api/students/{student_id}"
    payload = {
        "name": nome,
        "email": email,
        "class_id": class_id,
        "external_id": str(external_id)
    }
    response = requests.put(url, headers=HEADERS, json=payload)

    if response.status_code == 200:
        print(f"Aluno {nome} atualizado com sucesso!")
    else:
        print(f"Erro ao atualizar aluno: {response.status_code} - {response.text}")

# Buscar aluno pelo student_id
def buscar_aluno_por_student_id(student_id):
    url = f"https://app.redacaonline.com.br/api/students/{student_id}"
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao buscar aluno: {response.text}")
        return None

# Atualizar turma do aluno
def atualizar_turma_aluno():
    nome_aluno = "Thiago"
    unit_id = 34993  # ID da unidade

    # Buscar student_id dinamicamente
    student_id = obter_student_id(nome_aluno)
    if not student_id:
        return

    email_aluno = f"{student_id}@alunos.smrede.com.br"

    # Obter class_id da turma
    class_id = obter_class_id("CONCEITO B", unit_id)
    if not class_id:
        print("Turma 'CONCEITO B' não encontrada.")
        return

    # Buscar aluno pelo student_id
    aluno_api = buscar_aluno_por_student_id(student_id)
    if aluno_api:
        aluno_id = aluno_api["id"]
        
        if aluno_api["class_id"] != class_id:
            print(f"Aluno {nome_aluno} está na turma errada. Atualizando...")
            atualizar_aluno(aluno_id, nome_aluno, email_aluno, class_id, student_id)
        else:
            print(f"Aluno {nome_aluno} já está na turma correta.")
    else:
        print("Aluno não encontrado.")

# Executar atualização
if __name__ == "__main__":
    atualizar_turma_aluno()
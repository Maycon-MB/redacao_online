import requests
import psycopg2  # Conexão com o PostgreSQL

# Configuração da API
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4"
HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

# Configuração do banco de dados PostgreSQL
db_config = {
    "dbname": "BOLETOS",
    "user": "postgres",
    "password": "teste",
    "host": "192.168.1.163",
    "port": "5432"
}

# Mapeamento manual das unidades (Banco → API)
unidades_api = {
    "01": 35022, "02": 35023, "03": 35024, "04": 35025, "05": 35026,
    "06": 35027, "09": 35028, "10": 35029, "11": 35030, "14": 35031,
    "15": 35032, "16": 35033, "17": 35034
}

# Função para criar turma na API
def criar_turma(unit_id, nome_turma):
    url = "https://app.redacaonline.com.br/api/classes"
    
    # Formar o external_id concatenando unit_id e nome_turma
    external_id = f"{unit_id}_{nome_turma}"  # Exemplo: "35022_Turma1"
    
    payload = {
        "name": nome_turma,  # Nome da turma
        "unit_id": unit_id,  # ID da unidade na API
        "external_id": external_id  # External ID concatenado
    }
    
    response = requests.post(url, headers=HEADERS, json=payload)
    
    if response.status_code == 200:
        turma_data = response.json()
        turma_id = turma_data.get("id", "ID não encontrado")
        print(f"✅ Turma '{nome_turma}' criada com sucesso! ID: {turma_id}")
        return turma_id
    else:
        print(f"❌ Erro ao criar turma '{nome_turma}': {response.status_code} - {response.text}")
        return None

# Função para listar turmas existentes na API
def listar_turmas(unit_id):
    url = "https://app.redacaonline.com.br/api/classes"
    response = requests.get(url, headers=HEADERS, params={"unit_id": unit_id})
    
    if response.status_code == 200:
        turmas = response.json()
        return {turma["name"]: turma["id"] for turma in turmas}
    else:
        print(f"❌ Erro ao listar turmas da unidade {unit_id}: {response.status_code} - {response.text}")
        return {}

# Função principal para processar alunos e criar turmas se necessário
def processar_alunos():
    # Conectar ao banco de dados
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    # Buscar alunos com turmas maiores que 11900 e garantir que seja única a combinação unidade + turma
    cursor.execute(""" 
        SELECT DISTINCT unidade, turma FROM alunos_25_geral 
        WHERE CAST(turma AS INTEGER) > 11900
    """)
    alunos = cursor.fetchall()
    conn.close()

    # Criar um dicionário para armazenar as turmas por unidade (para evitar chamadas repetidas à API)
    turmas_por_unidade = {}

    # Processar cada aluno
    for unidade, nome_turma in alunos:
        unidade_str = str(unidade).zfill(2)  # Converte a unidade para formato com 2 dígitos ('01', '02', ...)
        unit_id = unidades_api.get(unidade_str)
        
        if not unit_id:
            print(f"❌ Unidade {unidade} não encontrada no mapeamento.")
            continue

        # Verificar se já consultamos as turmas dessa unidade
        if unidade_str not in turmas_por_unidade:
            # Se não, consultar as turmas da unidade na API
            turmas_por_unidade[unidade_str] = listar_turmas(unit_id)

        turmas_api = turmas_por_unidade[unidade_str]  # Obter as turmas já existentes para a unidade

        # Se a turma não existir na API, cria a turma
        if nome_turma not in turmas_api:
            turma_id = criar_turma(unit_id, nome_turma)
            if turma_id:
                turmas_api[nome_turma] = turma_id  # Atualiza o dicionário local com o ID da turma
        
        # Aqui a turma já foi criada ou já existe
        print(f"🔹 Turma '{nome_turma}' já existe ou foi criada com ID: {turmas_api.get(nome_turma, 'Erro ao obter ID')}")

# Executar o processo
if __name__ == "__main__":
    processar_alunos()

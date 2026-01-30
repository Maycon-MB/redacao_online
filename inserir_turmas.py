import requests
import psycopg2  # Conexão com o PostgreSQL
from datetime import datetime

ANO_SUFIXO = str(datetime.now().year)[2:]
TABELA_ALUNOS = f"alunos_{ANO_SUFIXO}_geral"

# Configuração da API
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4"
HEADERS = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

# Configuração do banco de dados PostgreSQL local
db_config = {
    "dbname": "BOLETOS",
    "user": "postgres",
    "password": "teste",
    "host": "192.168.1.163",
    "port": "5432"
}

# Mapeamento Unidade (Banco) -> ID Unidade (API)
unidades_api = {
    "01": 35022, "02": 35023, "03": 35024, "04": 35025, "05": 35026,
    "06": 35027, "09": 35028, "10": 35029, "11": 35030, "14": 35031,
    "15": 35032, "16": 35033, "17": 35034
}

# Mapeamento Unidade (Banco) -> Sigla (Para o nome da turma)
siglas_unidades = {
    "01": "BR", "02": "MA", "03": "SC", "04": "CD",
    "05": "TQ", "06": "NP", "09": "SP", "10": "BT",
    "11": "CG", "14": "MC", "15": "IG",
    "16": "FG", "17": "RB"
}

def listar_turmas_existentes(unit_id):
    """Obtém todas as turmas já cadastradas em uma unidade na API"""
    url = "https://app.redacaonline.com.br/api/classes"
    try:
        response = requests.get(url, headers=HEADERS, params={"unit_id": unit_id}, timeout=15)
        if response.status_code == 200:
            turmas = response.json()
            # Retorna um dicionário { "Nome da Turma": id_da_turma }
            return {t["name"]: t["id"] for t in turmas}
    except Exception as e:
        print(f"❌ Erro ao listar turmas da unidade {unit_id}: {e}")
    return {}

def criar_turma_na_api(unit_id, nome_turma_formatado):
    """Envia o comando POST para criar a turma com o nome correto"""
    url = "https://app.redacaonline.com.br/api/classes"
    
    # O external_id ajuda a API a não duplicar se você rodar o script de novo
    external_id = f"{unit_id}_{nome_turma_formatado.replace(' ', '_')}"

    payload = {
        "name": nome_turma_formatado,
        "unit_id": unit_id,
        "external_id": external_id
    }

    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=15)
        if response.status_code in [200, 201]:
            data = response.json()
            print(f"✅ Turma '{nome_turma_formatado}' criada com sucesso! ID: {data.get('id')}")
            return data.get('id')
        else:
            print(f"⚠️ Falha ao criar '{nome_turma_formatado}': {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Erro de conexão ao criar turma: {e}")
    return None

def processar_e_sincronizar_turmas():
    print(f"⏳ Iniciando sincronização de turmas para {TABELA_ALUNOS}...")
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Pegamos apenas combinações únicas de Unidade e Turma da View de 2026
        cursor.execute(f"""
            SELECT DISTINCT unidade, turma FROM {TABELA_ALUNOS}
            WHERE turma ~ '^[0-9]+$' AND CAST(turma AS INTEGER) >= 11900
        """)
        turmas_banco = cursor.fetchall()
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao acessar o Banco de Dados: {e}")
        return

    # Cache local para não bater na API toda hora pedindo a mesma unidade
    cache_api_unidades = {}

    for cod_unidade, num_turma in turmas_banco:
        cod_unidade = str(cod_unidade).zfill(2)
        unit_id_api = unidades_api.get(cod_unidade)
        sigla = siglas_unidades.get(cod_unidade)

        if not unit_id_api or not sigla:
            print(f"⚠️ Unidade {cod_unidade} não mapeada. Pulando...")
            continue

        # Formata o nome como o outro script espera: "NP 11913"
        nome_correto = f"{sigla} {num_turma}"

        # Se ainda não baixamos as turmas dessa unidade nesta execução, baixamos agora
        if cod_unidade not in cache_api_unidades:
            print(f"🔄 Consultando turmas existentes na unidade {sigla}...")
            cache_api_unidades[cod_unidade] = listar_turmas_existentes(unit_id_api)

        turmas_na_api = cache_api_unidades[cod_unidade]

        # Verifica se a turma (com sigla) já existe na API
        if nome_correto not in turmas_na_api:
            print(f"🚀 Turma '{nome_correto}' não encontrada. Criando...")
            novo_id = criar_turma_na_api(unit_id_api, nome_correto)
            if novo_id:
                # Adiciona ao cache local para evitar tentativas duplicadas
                cache_api_unidades[cod_unidade][nome_correto] = novo_id
        else:
            print(f"✔️ Turma '{nome_correto}' já existe na API.")

    print("🏁 Processo de criação de turmas concluído!")

if __name__ == "__main__":
    processar_e_sincronizar_turmas()

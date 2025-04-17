import requests

# Configuração da API
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}
URL_API = "https://app.redacaonline.com.br/api/essays"

def get_redacoes_com_nota():
    """Obtém todas as redações com nota da API"""
    pagina = 1
    redacoes_com_nota = []
    
    while True:
        print(f"Buscando redações - página {pagina}...")
        
        params = {
            "page": pagina,
            "limit": 100,  # Máximo permitido por página
            "order": "DESC"  # Ordena das mais recentes para as mais antigas
        }
        
        try:
            response = requests.get(URL_API, headers=HEADERS, params=params)
            response.raise_for_status()
            dados = response.json()
            
            if not dados.get('data'):
                break  # Sai do loop quando não houver mais dados
                
            # Filtra apenas redações com nota
            for redacao in dados['data']:
                if redacao.get('grade') is not None:  # Se tem nota
                    redacoes_com_nota.append({
                        'id': redacao['id'],
                        'aluno': redacao.get('student_name'),
                        'matricula': redacao.get('student_external_id'),
                        'turma': redacao.get('class_name'),
                        'tema': redacao.get('theme'),
                        'data': redacao.get('created_at'),
                        'nota': redacao.get('grade')
                    })
            
            pagina += 1
            
        except Exception as e:
            print(f"Erro ao buscar redações: {e}")
            break
    
    return redacoes_com_nota

def mostrar_resultados(redacoes):
    """Exibe as redações com nota no console"""
    print("\nREDAÇÕES CORRIGIDAS (COM NOTA)")
    print("=" * 50)
    
    for i, redacao in enumerate(redacoes, 1):
        print(f"\n📝 Redação #{i}")
        print(f"ID: {redacao['id']}")
        print(f"Aluno: {redacao['aluno']} (Matrícula: {redacao['matricula']})")
        print(f"Turma: {redacao['turma']}")
        print(f"Tema: {redacao['tema']}")
        print(f"Data: {redacao['data']}")
        print(f"Nota: {redacao['nota']}")
        print("-" * 40)
    
    print(f"\nTotal de redações corrigidas encontradas: {len(redacoes)}")

if __name__ == "__main__":
    print("Obtendo redações corrigidas...")
    redacoes_com_nota = get_redacoes_com_nota()
    mostrar_resultados(redacoes_com_nota)
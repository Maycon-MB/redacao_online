import requests
import pandas as pd
from datetime import datetime

def get_all_corrected_essays():
    base_url = "https://app.redacaonline.com.br/api/essays"
    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4"  # Substitua pelo token real
    }
    
    all_essays = []
    page = 1
    limit = 100  # Máximo por página
    
    while True:
        try:
            # Faz a requisição com paginação
            params = {"page": page, "limit": limit, "order": "DESC"}
            response = requests.get(base_url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Verifica se há dados
            if not data.get('data'):
                break
                
            # Processa cada redação
            for essay in data['data']:
                if essay.get('corrections') and len(essay['corrections']) > 0:
                    # Organiza os dados principais
                    essay_data = {
                        'ID': essay.get('id'),
                        'Aluno': essay.get('student', {}).get('name') if essay.get('student') else None,
                        'Aluno_ID': essay.get('student_id'),
                        'Unidade': essay.get('student', {}).get('class', {}).get('unit', {}).get('name') if essay.get('student') else None,
                        'Turma': essay.get('student', {}).get('class', {}).get('name') if essay.get('student') else None,
                        'Tema': essay.get('theme'),
                        'Tema_ID': essay.get('theme_text_id'),
                        'Criado_em': essay.get('created_at'),
                        'Corrigido_em': essay.get('corrected_at'),
                        'Notificado_em': essay.get('notified_at'),
                        'Arquivo': essay.get('file'),
                        'Total_Correcoes': len(essay.get('corrections', [])),
                        'is_corrected': essay.get('is_corrected'),
                        'Nota': essay.get('grade')
                    }
                    
                    # Adiciona detalhes das competências se existirem
                    if essay.get('corrections'):
                        for i, correction in enumerate(essay['corrections'], 1):
                            essay_data[f'Correcao_{i}_Tipo'] = correction.get('type')
                            essay_data[f'Correcao_{i}_Comentario'] = correction.get('comment')
                            essay_data[f'Correcao_{i}_Competencia'] = correction.get('competence')
                            essay_data[f'Correcao_{i}_Nota'] = correction.get('grade')
                            essay_data[f'Correcao_{i}_Data'] = correction.get('created_at')
                    
                    all_essays.append(essay_data)
            
            # Verifica se chegou ao final
            if len(data['data']) < limit:
                break
                
            page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"Erro na página {page}: {str(e)}")
            break
        except Exception as e:
            print(f"Erro inesperado: {str(e)}")
            break
    
    return all_essays

def generate_spreadsheet(essays):
    if not essays:
        print("Nenhuma redação com correções encontrada.")
        return
    
    # Cria DataFrame
    df = pd.DataFrame(essays)
    
    # Ordena por data de correção (mais recente primeiro)
    if 'Corrigido_em' in df.columns:
        df['Corrigido_em'] = pd.to_datetime(df['Corrigido_em'])
        df = df.sort_values('Corrigido_em', ascending=False)
    
    # Gera nome do arquivo com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"redacoes_corrigidas_{timestamp}.xlsx"
    
    # Salva como Excel
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Redações Corrigidas')
    
    # Ajusta largura das colunas
    worksheet = writer.sheets['Redações Corrigidas']
    for i, col in enumerate(df.columns):
        max_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(i, i, min(max_len, 50))
    
    writer.close()
    print(f"Planilha gerada com sucesso: {filename}")

if __name__ == "__main__":
    print("Iniciando busca por redações corrigidas...")
    start_time = datetime.now()
    
    try:
        corrected_essays = get_all_corrected_essays()
        generate_spreadsheet(corrected_essays)
        
        total_time = (datetime.now() - start_time).total_seconds()
        print(f"Processo concluído em {total_time:.2f} segundos")
        print(f"Total de redações com correções encontradas: {len(corrected_essays)}")
        
    except KeyboardInterrupt:
        print("\nProcesso interrompido pelo usuário.")
    except Exception as e:
        print(f"\nErro inesperado: {str(e)}")
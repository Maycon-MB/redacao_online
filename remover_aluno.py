# remover aluno
import requests

url = "https://app.redacaonline.com.br/api/students/{student_id}"

payload  = {}

headers = {
  'Accept': 'application/json',
  'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiIxMzIiLCJqdGkiOiI1MTNhMDdhMDdmMmY4OTI5MWUyMzQ5MTVmMDY5ODIyYWZhNjkxZDAwZGQ2YzZiNjVkM2IyMmY1N2QwNzNmZTEzN2NhMjMxZDc1MmQwMTg3YSIsImlhdCI6MTczNzA1Mjk4Ny40MTIwNDgsIm5iZiI6MTczNzA1Mjk4Ny40MTIwNTIsImV4cCI6MTg5NDgxOTM4Ny40MDM0MDcsInN1YiI6IjMxOSIsInNjb3BlcyI6WyIqIl19.UP9ThFiYOvtjr2JOQuu0nSWzroOz6WmWCnfNK4yhU-j_02pcDT1hukXrgV_FnsCqy37kGgAZVXT-uk2TLr8RWxMww4JXtqLDCdH6uSmQvO2uK1HAutxkoJNFDurIjTcfX2RmbQ4_TD0QUc2pyyHZ9lB9C4nOlOvRLkJIOoyGAa3gUlTHgX8GN5x2ZS_2bxrYPy4ioFDMNTwCi5UG_fXbApmVuXvhc35muacC1TVmuExvGQLpliDsZqNNJgZSVZsqdhFQS4ZrqYMz-pkd0_W0AuCSYxjMKyEAjcd6aEQfkLiGfVzI0EJ19e1Yc-vBG1SxC4Iv7FfhL6H9hahMZ-sQK07Ilbt9vD7k-I-93vvHBmlYIT4A4wC9QqH-z-I0hg64JjYsPLBezqmZOUEmcsan30OFZSlSp2iRkgd4iEnvia41KdkllERkoPPu5o4fq8hQ6dfVvGsqSQ0ZdMRWy0ukWTdAvgExFCh3i7Q6hadvdePrSzNUIrESp4tKpTg5qtVtbaseZNz7IkzpuYbk8tJUolNIterF20nc0leBbk2Qx0cjrrw6Cyzu7AElUxG-vALI5FRmR9jsWT_knrSQaWZaRo1tHrCF0yG19odOjS8Scd-97JTzN-gStji6XyDl5fTQV0uljHS3CqyTFDcWl9ZRk3wI5ITjFgYaiVEIkZ7U5p4'
}

response = requests.request("DELETE", url, headers=headers, data = payload)

print(response.text.encode('utf8'))

# Path Params
# student_id
# int32
# required
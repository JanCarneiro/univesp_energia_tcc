from datetime import date
from pyspark.sql import Row
import pandas as pd
import requests
import os

def extract(url, url_bronze):

    response = requests.get(url)

    with open(url_bronze, 'wb') as f:
        f.write(response.content)

        print(f'Gerado arquivo {url_bronze}')


def prepara_extracao(base_url,nm_base,ano_inicio, ano_fim, ano_mes = None):

    url_arquivo = f'/home/jan/Documentos/tcc/datasets/bronze/{nm_base}'
    os.makedirs(url_arquivo, exist_ok=True)
    
    for ano in range(ano_inicio,ano_fim+1):
        
        if ano_mes is None or ano < ano_mes:

            url = f'{base_url}{ano}.parquet'
            url_bronze = f'{url_arquivo}/{nm_base}_{ano}.parquet'
            print(url)

            extract(url,url_bronze)

        else:
            mes_atual = (date.today().month) + 1
            mes_base = mes_atual if ano >= ano_fim else 13

            for mes in range(1,mes_base):

                mes = f'0{mes}' if mes < 10 else mes
                
                url = f'{base_url}{ano}_{mes}.parquet'
                url_bronze = f'{url_arquivo}/{nm_base}_{ano}_{mes}.parquet'
                print(url)

                extract(url,url_bronze)


def merge_files(base_url,nm_base, spark):

    path_arquivos = f'{base_url}{nm_base}'
    path_prata = f'/home/jan/Documentos/tcc/datasets/prata/{nm_base}'

    os.makedirs(path_prata, exist_ok=True)

    df = [spark.read.parquet(f"{path_arquivos}/{nm_file}") for nm_file in os.listdir(path_arquivos)]
    combined_df = df[0]
    for df in df[1:]:
        combined_df = combined_df.union(df)

    combined_df.write.parquet(f"{path_prata}/{nm_base}_consolidada.parquet", mode="overwrite")
    print(f"Criado arquivo: {path_prata}/{nm_base}_consolidada.parquet ")
    return combined_df


def extrai_bases_excel(tipo, path, spark):
    dfs = pd.read_excel(path, sheet_name=f'{tipo}', dtype=str, engine='openpyxl', skiprows=3)
    dfs = spark.createDataFrame(dfs)

    listas = dfs.filter((dfs['Unnamed: 0'] == 'Sao Paulo') | (dfs['Unnamed: 1'] == 'JAN') | (dfs['Unnamed: 1'] == '2004')).collect()

    tipo = tipo.replace('.', '')
    tipo = tipo.replace(' ', '_')

    lista = []
    for ano,mes,totais in zip(listas[0], listas[1], listas[2]):

        lista.append(Row(ano = ano, mes = mes, consumo_total= totais, nm_base = tipo))
        df = spark.createDataFrame(lista)
    
    df.write.parquet(f'/home/jan/Documentos/tcc/datasets/prata/CONSUMO_ENERGIA/{tipo}.parquet', mode="overwrite")
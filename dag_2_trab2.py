import pandas as pd
import functools as ft

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args={
    'owner':'Keylom',
    'dependes_on_past':False,
    'start_date':datetime(2002,9,1)
}

@dag(default_args=default_args,schedule_interval='@once',catchup=False, tags=['Titanic Keylom trabalho 2 - DAG 2'])
def trabalho_2_dag_2(): 

    @task
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/tabela_unica.csv"
        df = pd.read_csv(NOME_DO_ARQUIVO, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        print(df)
        return NOME_DO_ARQUIVO

    @task
    def ind_media_passageiros(nome_do_arquivo):
        NOME_DO_ARQUIVO = "/tmp/tabela_media_total.csv"
        df = pd.read_csv(nome_do_arquivo, sep=';')
        res = df.agg({
            "total_passageiros":"mean",
            "preco_medio":"mean",
            "total_sibsp_parch":"mean"
        }).reset_index()  
        res.rename(columns = {'total_passageiros':'media_passageiros'}, inplace = True)
        res.rename(columns = {'preco_medio':'media_preco'}, inplace = True)
        res.rename(columns = {'total_sibsp_parch':'media_sibsp_parch'}, inplace = True)  
        print(res)
        res.to_csv(NOME_DO_ARQUIVO, index=False, sep=';')
        return NOME_DO_ARQUIVO

    @task
    def resultados(path1):
        PATH_SAIDA = "/tmp/resultados.csv"
        df_final = pd.read_csv(path1, sep=';')
        print(df_final)
        df_final.to_csv(PATH_SAIDA, index=False, sep=';')
        return PATH_SAIDA


    fim = DummyOperator(task_id="fim")

    ing = ingestao()
    ind = ind_media_passageiros(ing)
    p = resultados(ind)
    
    ind >> p >> fim

execucao = trabalho_2_dag_2()
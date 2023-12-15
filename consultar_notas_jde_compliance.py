import cx_Oracle
import os
from dotenv import load_dotenv

def criar_conexao_oracle(usuario, senha, tns_name):
    try:
        # Tenta estabelecer uma conexão Oracle usando cx_Oracle
        conexao = cx_Oracle.connect(f'{usuario}/{senha}@{tns_name}')
        return conexao
    except cx_Oracle.Error as e:
        # Lança uma exceção com uma mensagem personalizada se houver um erro
        raise ValueError(f"Erro ao conectar ao Oracle: {e}")

def executar_query(conexao, query, parametros):
    resultados = []
    with conexao.cursor() as cursor:
        # Executa a consulta SQL usando o cursor
        cursor.execute(query, parametros)
        # Obtém todos os resultados da consulta
        resultados = cursor.fetchall()
    return resultados

def comparar_notas(conexao, lista_de_notas, tabela, campo_nota, campo_serie):
    for nota in lista_de_notas:
        # Extrai o número da nota e a série
        numero_nota = ''.join(filter(str.isdigit, nota))
        serie = nota[-2:] if '/' in nota[:-2] else nota[-1]

        # Constrói a consulta SQL usando formatação de string
        query = f"SELECT {campo_nota} FROM {tabela} WHERE {campo_nota} = :nota_numero AND {campo_serie} = :serie"
        parametros_para_consulta = {'nota_numero': numero_nota, 'serie': serie}
        
        try:
            # Executa a consulta usando a função definida anteriormente
            resultados = executar_query(conexao, query, parametros_para_consulta)
            if not resultados:
                print(f"A nota {nota} não está presente em {tabela}.")
            else:
                print(f"A nota {nota} está presente em {tabela}.")
        except Exception as e:
            # Manipula exceções durante a execução da consulta
            print(f"Erro ao executar a consulta para a nota {nota}: {str(e)}")

# Carrega o ambiente virtual
with open('notas_para_comparar.txt', 'r') as notas_para_comparar:
    # Lê as notas do arquivo e cria uma lista
    lista_de_notas = [linha.strip() for linha in notas_para_comparar]

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Cria a conexão Oracle usando as variáveis de ambiente
with criar_conexao_oracle(os.getenv("ORACLE_USUARIO"), os.getenv("ORACLE_SENHA"), os.getenv("ORACLE_TNS_NAME")) as conexao:
    # Consulta para o Compliance
    comparar_notas(conexao, lista_de_notas, 'CSF_OWN.NOTA_FISCAL', 'NRO_NF', 'SERIE')

    # Consulta para o JDE
    comparar_notas(conexao, lista_de_notas, 'DBRDTA.f47011', 'SYCACT', 'SYAUTN')

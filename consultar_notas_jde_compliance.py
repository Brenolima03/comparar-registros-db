import cx_Oracle
import os
from dotenv import load_dotenv

# Função para criar uma conexão com o Oracle.
def criar_conexao_oracle(usuario, senha, tns_name):
    try:
        # Tenta estabelecer a conexão.
        conexao = cx_Oracle.connect(f'{usuario}/{senha}@{tns_name}')
        return conexao
    
    except cx_Oracle.Error as e:
        # Levanta uma exceção se houver um erro ao conectar.
        raise ValueError(f"Erro ao conectar ao Oracle: {e}")

# Função para executar uma consulta e formatar o resultado.
def executar_query(conexao, query):
    with conexao.cursor() as cursor:
        # Executa a consulta.
        cursor.execute(query)
        # Obtém a primeira linha do resultado.
        resultado = cursor.fetchone()
        if resultado is not None:
            # Formatando o retorno para remover os espaços e transformar em tupla.
            resultado = tuple(map(str.strip, resultado))

    return resultado

# Função para comparar notas entre dois sistemas.
def comparar_notas(conexao, lista_de_notas, tabela, campo_nota, campo_serie):
    notas_faltando_jde = []
    # Faz a consulta apenas se a linha contém uma nota válida.
    for nota in [nota for nota in lista_de_notas if '/' in nota]:
        # Formatando tanto o número quanto a série da nota para definir onde termina ou começa cada um, dependendo de onde está o '/'.
        query = f"SELECT {campo_nota} FROM {tabela} WHERE {campo_nota} = '{nota[:-3] if '/' in nota[:-2] else nota[:-2]}' \
        AND {campo_serie} = '{nota[-2:] if '/' in nota[:-2] else nota[-1]}'"
        
        try:
            # Executa a consulta e verifica se há resultado.
            resultado = executar_query(conexao, query)
            if not resultado:
                # Adiciona a nota à lista se não houver resultado.
                notas_faltando_jde.append(nota)

        except Exception as e:
            print(f"Erro ao executar a consulta para a nota {nota}: {str(e)}")

    print("Notas faltando no JDE:")
    for nota in notas_faltando_jde:
        print(nota)

with open('notas_para_comparar.txt', 'r') as notas_para_comparar:
    # Lê as notas do arquivo e remove espaços em branco.
    lista_de_notas = [linha.strip() for linha in notas_para_comparar]

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Cria a conexão Oracle usando as variáveis de ambiente.
with criar_conexao_oracle(os.getenv("ORACLE_USUARIO"), os.getenv("ORACLE_SENHA"), os.getenv("ORACLE_TNS_NAME")) as conexao:
    notas_faltando_jde = comparar_notas(conexao, lista_de_notas, 'DBRDTA.f47011', 'SYCACT', 'SYAUTN')
import pandas as pd
import sqlite3

funcionarios = pd.read_csv('./funcionarios.csv')
cargos = pd.read_csv('./cargos.csv')
dptos = pd.read_csv('./departamentos.csv')
dependentes = pd.read_csv('./dependentes.csv')
historico = pd.read_csv('./historico_salarios.csv')

conexao = sqlite3.connect('TP3.db')
cursor = conexao.cursor()

cursor.execute('DELETE FROM cargos;')
cursor.execute('DELETE FROM departamentos;')
cursor.execute('DELETE FROM funcionarios;')
cursor.execute('DELETE FROM dependentes;')
cursor.execute('DELETE FROM salarios_funcionarios;')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS cargos (
        id_cargo INT PRIMARY KEY,
        descricao_cargo VARCHAR(200) NOT NULL,
        salario_base DOUBLE NOT NULL,
        nivel_cargo VARCHAR(20) NOT NULL,
        escolaridade VARCHAR(50) NOT NULL,
        CONSTRAINT chk_nivel_cargo CHECK (nivel_cargo IN ('estagiário', 'técnico', 'analista', 'gerente', 'diretor'))
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS departamentos (
        id_departamento INT PRIMARY KEY,
        nome_departamento VARCHAR(200) NOT NULL,
        id_gerente INT,
        andar_departamento INT NOT NULL,
        qtd_funcionarios INT NOT NULL,
        FOREIGN KEY (id_gerente) REFERENCES funcionarios(id_funcionario)
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS funcionarios (
        id_funcionario INT PRIMARY KEY,
        nome_funcionario VARCHAR(100) NOT NULL,
        id_cargo INT,
        id_departamento INT,
        salario_funcionario DOUBLE NOT NULL,
        ctps_funcionario VARCHAR(30) NOT NULL,
        FOREIGN KEY (id_cargo) REFERENCES cargos(id_cargo),
        FOREIGN KEY (id_departamento) REFERENCES departamentos(id_departamento)
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS dependentes (
        id_dependente INT PRIMARY KEY,
        id_funcionario INT,
        nome_funcionario VARCHAR(100),
        nome_dependente VARCHAR(100),
        relacao VARCHAR(50),
        idade INT,
        FOREIGN KEY (id_funcionario) REFERENCES funcionarios(id_funcionario)
    );
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS salarios_funcionarios (
        id_funcionario INT,
        mes1 DOUBLE(10,2),
        mes2 DOUBLE(10,2),
        mes3 DOUBLE(10,2),
        mes4 DOUBLE(10,2),
        mes5 DOUBLE(10,2),
        mes6 DOUBLE(10,2),
        FOREIGN KEY (id_funcionario) REFERENCES funcionarios(id_funcionario)
    );
''')

print("Tabela criada com sucesso")

cursor.executemany('''
    INSERT INTO cargos (id_cargo, descricao_cargo, salario_base, nivel_cargo, escolaridade)
    VALUES (:id_cargo, :descricao_cargo, :salario_base, :nivel_cargo, :escolaridade)
''', cargos.to_dict(orient='records'))

cursor.executemany('''
    INSERT INTO departamentos (id_departamento, nome_departamento, id_gerente, andar_departamento, qtd_funcionarios)
    VALUES (:id_departamento, :nome_departamento, :id_gerente, :andar_departamento, :qtd_funcionarios)
''', dptos.to_dict(orient='records'))

cursor.executemany('''
    INSERT INTO funcionarios (id_funcionario, nome_funcionario, id_cargo, id_departamento, salario_funcionario, ctps_funcionario)
    VALUES (:id_funcionario, :nome_funcionario, :id_cargo, :id_departamento, :salario_funcionario, :ctps_funcionario)
''', funcionarios.to_dict(orient='records'))

cursor.executemany('''
    INSERT INTO dependentes (id_dependente, id_funcionario, nome_funcionario, nome_dependente, relacao, idade)
    VALUES (:id_dependente, :id_funcionario, :nome_funcionario, :nome_dependente, :relacao, :idade)
''', dependentes.to_dict(orient='records'))

cursor.executemany('''
    INSERT INTO salarios_funcionarios (id_funcionario, mes1, mes2, mes3, mes4, mes5, mes6)
    VALUES (:id_funcionario, :mes1, :mes2, :mes3, :mes4, :mes5, :mes6)
''', historico.to_dict(orient='records'))

print("Dados adicionados com sucesso!")

# 1.Listar individualmente as tabelas de: Funcionários, Cargos, Departamentos, Histórico de Salários e Dependentes em ordem crescente.
consultas = {
    "Funcionários": "SELECT * FROM funcionarios ORDER BY nome_funcionario;",
    "Cargos": "SELECT * FROM cargos ORDER BY descricao_cargo;",
    "Departamentos": "SELECT * FROM departamentos ORDER BY nome_departamento;",
    "Dependentes": "SELECT * FROM dependentes ORDER BY nome_dependente;",
    "Histórico de Salários": "SELECT * FROM salarios_funcionarios ORDER BY id_funcionario;"
}

for tabela, query in consultas.items():
    print(f"\n{tabela}:")
    cursor.execute(query)
    resultados = cursor.fetchall()
    for linha in resultados:
        print(linha)

# 2. Listar os funcionários, com seus cargos, departamentos e os respectivos dependentes.
consulta = '''
SELECT funcionarios.id_funcionario, funcionarios.nome_funcionario, cargos.descricao_cargo, departamentos.nome_departamento, dependentes.nome_dependente, dependentes.relacao
FROM funcionarios
JOIN dependentes ON funcionarios.id_funcionario = dependentes.id_funcionario
JOIN cargos ON funcionarios.id_cargo = cargos.id_cargo
JOIN departamentos ON funcionarios.id_departamento = departamentos.id_departamento
'''

print(f"\nLista dos funcionários, com seus cargos, departamentos e os respectivos dependentes")
cursor.execute(consulta)
resultados = cursor.fetchall()
for linha in resultados:
    print(linha)

# 3. Listar os funcionários que tiveram aumento salarial nos últimos 3 meses.
funcs = funcionarios[(funcionarios['id_funcionario'] == historico['id_funcionario']) &
                    ((funcionarios['salario_funcionario'] > historico['mes4']) |
                    (funcionarios['salario_funcionario'] > historico['mes5']) |
                    (funcionarios['salario_funcionario'] > historico['mes6']))]

print(f"\nFuncionários que tiveram aumento nos ultimos 3 meses")
print(funcs)

# 4. Listar a média de idade dos filhos dos funcionários por departamento.
for i in range(1, 6):
    funcs = funcionarios[(funcionarios['id_departamento'] == i)]
    
    total_idade = 0
    total_filhos = 0

    for idx, func in funcs.iterrows():
        filhos = dependentes[(dependentes['id_funcionario'] == func['id_funcionario']) &
                            ((dependentes['relacao'] == 'Filho') | (dependentes['relacao'] == 'Filha'))]
        
        for _, filho in filhos.iterrows():
            total_idade += filho['idade']
            total_filhos += 1

    if total_filhos > 0:
        media = total_idade / total_filhos
    else:
        media = 0

    nome_departamento = dptos[dptos['id_departamento'] == i]['nome_departamento'].values[0]
    print(f"\nMédia de idade dos filhos do departamento {nome_departamento}:")
    print(media)

# 5. Listar qual estagiário possui filho.
consulta = '''
SELECT funcionarios.id_funcionario, funcionarios.nome_funcionario, dependentes.nome_dependente, dependentes.relacao
FROM funcionarios
JOIN dependentes ON funcionarios.id_funcionario = dependentes.id_funcionario
WHERE funcionarios.id_cargo = 1 AND (dependentes.relacao = 'Filha' OR dependentes.relacao = 'Filho')
'''

print(f"\nEstagiário que possui filho")
cursor.execute(consulta)
resultados = cursor.fetchall()
for linha in resultados:
    print(linha)

# 6. Listar o funcionário que teve o salário médio mais alto.
media_alta = 0
funcionario_mais_alto = 0
for idx, i in funcionarios.iterrows():
    funcionario = historico[historico['id_funcionario'] == i['id_funcionario']]
    total_salário = (funcionario['mes1'].values[0] + 
                    funcionario['mes2'].values[0] + 
                    funcionario['mes3'].values[0] + 
                    funcionario['mes4'].values[0] + 
                    funcionario['mes5'].values[0] + 
                    funcionario['mes6'].values[0])
    media = total_salário/6

    if media > media_alta:
        media_alta = media
        funcionario_mais_alto = i 

print(f"\nFuncionário com o salário médio mais alto (R${media_alta}):")
print(funcionario_mais_alto)

# 7. Listar o analista que é pai de 2 (duas) meninas.
consulta = '''
SELECT funcionarios.id_funcionario, funcionarios.nome_funcionario, COUNT(dependentes.id_dependente) AS num_filhas
FROM funcionarios
JOIN dependentes ON funcionarios.id_funcionario = dependentes.id_funcionario
WHERE funcionarios.id_cargo = 3 AND dependentes.relacao = 'Filha'
GROUP BY funcionarios.id_funcionario, funcionarios.nome_funcionario
HAVING COUNT(dependentes.id_dependente) = 2;
'''

print(f"\nAnalista que é pai de 2 (duas) meninas:")
cursor.execute(consulta)
resultados = cursor.fetchall()
for linha in resultados:
    print(linha)

# 8. Listar o analista que tem o salário mais alto, e que ganhe entre 5000 e 9000.
consulta = '''
SELECT * 
FROM funcionarios
WHERE id_cargo = 3 AND (salario_funcionario >= 5000 AND salario_funcionario <= 9000)
ORDER BY salario_funcionario DESC
LIMIT 1;
'''

print(f"\nAnalista com o maior salário")
cursor.execute(consulta)
resultados = cursor.fetchall()
for linha in resultados:
    print(linha)

# 9. Listar qual departamento possui o maior número de dependentes.
maior_num_dependentes = 0
departamento_com_mais_dependentes = ""
for idx, departamento in dptos.iterrows():
    funcs = funcionarios[funcionarios['id_departamento'] == departamento['id_departamento']]
    
    total_dependentes = 0

    for idx, func in funcs.iterrows():
        dependentes_func = dependentes[dependentes['id_funcionario'] == func['id_funcionario']]
        total_dependentes += len(dependentes_func)

    if total_dependentes > maior_num_dependentes:
        maior_num_dependentes = total_dependentes
        departamento_com_mais_dependentes = departamento['nome_departamento']

print(f"\nDepartamento com o maior número de dependentes: {departamento_com_mais_dependentes}")
print(f"Número de dependentes: {maior_num_dependentes}")

# 10. Listar a média de salário por departamento em ordem decrescente.
salarios_medio_departamento = []
for idx, departamento in dptos.iterrows():
    funs = funcionarios[funcionarios['id_departamento'] == departamento['id_departamento']]
    
    total_departamento = 0
    total_funcs = 0

    for idx, func in funs.iterrows():
        funcionario = historico[historico['id_funcionario'] == func['id_funcionario']]

        if not funcionario.empty:
            total_salário = (funcionario['mes1'].values[0] + 
                            funcionario['mes2'].values[0] + 
                            funcionario['mes3'].values[0] + 
                            funcionario['mes4'].values[0] + 
                            funcionario['mes5'].values[0] + 
                            funcionario['mes6'].values[0])
            total_departamento += total_salário
            total_funcs += 1
    
    if total_funcs > 0:
        media_departamento = total_departamento / total_funcs
        salarios_medio_departamento.append([departamento['nome_departamento'], media_departamento])

salarios_medio_departamento.sort(key=lambda x: x[1], reverse=True)

for departamento, media_salario in salarios_medio_departamento:
    print(f"\nDepartamento: {departamento}, Média Salarial: {media_salario:.2f}")


conexao.commit()
conexao.close()


from fastapi import FastAPI
from models import User, UserInfo, OpenBilling, PaidBilling, PhoneLine, PhoneServices, Service
import databases
import os

DB_HOST = os.environ.get("DB_HOST", "bdhost0061.servidorwebfacil.com")
DB_PORT = os.environ.get("DB_PORT", "3306")
DB_NAME = os.environ.get("DB_NAME", "aptoubat_teleconta")
DB_USER = os.environ.get("DB_USER", "aptoubat")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "River@2304")


DB2_HOST = os.environ.get("DB2_HOST", "mysql.pasvoluntariado.com.br")
DB2_PORT = os.environ.get("DB2_PORT", "3306")
DB2_NAME = os.environ.get("DB2_NAME", "pasvoluntariad")
DB2_USER = os.environ.get("DB2_USER", "pasvoluntariad")
DB2_PASSWORD = os.environ.get("DB2_PASSWORD", "NjGGOt0lCTl0QzXZ6dSs")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DATABASE_URL2 = f"mysql+pymysql://{DB2_USER}:{DB2_PASSWORD}@{DB2_HOST}:{DB2_PORT}/{DB2_NAME}"

database = databases.Database(DATABASE_URL)

database2 = databases.Database(DATABASE_URL2)

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get("/connect")
async def connect():
    try:
        await database.connect()
        print("Database connection established")
    except Exception as e:
        print(f"Error connecting to the database: {e}")


@app.get("/login/{cpf}")
async def login(cpf: str):
    if not database.is_connected:
        await database.connect()
        print("Database connection established")

    query = '''
        select u.id_empresa,u.usuario,u.nome_usuario,u.nick,u.senha, e.razao_social, u.cpf
        from usuario u,empresa e
        where u.id_empresa = e.id_empresa and
        u.ind_ativo = 'SIM' and cpf = :cpf;
    '''

    result = await database.fetch_one(query, values={"cpf": cpf})

    user = User(
        cpf=result[6],
        id=result[6],
        name=result[2],
        nick=result[4],
        username=result[1]
    )

    return user


@app.get("/open-billings/{cpf}")
async def open_billings(cpf: str):
    if not database.is_connected:
        await database.connect()
        print("Database connection established")
    print(cpf)

    query = '''
        select vw.nosso_numero,vw.id_terminal,vw.data_venc,vw.vlrreceber,vw.status,cr.linha_digitavel_cod_barras
        from 79_titulos_a_receber vw, 79_conta_receber cr
        where vw.id_usuario = :cpf  and vw.nosso_numero <> 0 and
        vw.nosso_numero = cr.nosso_numero and vw.referencia = cr.referencia and
        vw.id_usuario = cr.id_usuario and cr.ind_liberacao = 'S'
        order by vw.seunumero desc,vw.id_terminal
    '''

    result = await database.fetch_all(query, values={"cpf": cpf})

    billings = []

    for r in result:
        billing = OpenBilling(
            id=r[0],
            idTerminal=r[1],
            dateVenc=r[2],
            status=r[4],
            code=r[5],
            value=r[3],
        )

        billings.append(billing)

    return billings


@app.get("/paid-billings/{cpf}")
async def get_paid_billings(cpf):
    if not database.is_connected:
        await database.connect()
        print("Database connection established")
    print(cpf)

    query = '''
        select nosso_numero,id_terminal,data_venc,valor_conta,status,data_quit,valor_quitado, valor_juros
        from 79_conta_receber
        where id_usuario = :cpf and
        (valor_quitado <> 0.00 or data_quit = null) order by seunumero desc limit 6;
    '''

    result = await database.fetch_all(query, values={"cpf": cpf})

    billings = []

    for r in result:
        billing = PaidBilling(
            ourNumber=r[0],
            idTerminal=r[1],
            vencDate=r[2],
            value=r[3],
            status=r[4],
            dataQuit=r[5],
            paidValue=r[6],
            feesValue=r[7]
        )

        billings.append(billing)

    return billings


@app.get("/user-info/{user_id}")
async def get_terminal_info(user_id: int):
    if not database.is_connected:
        await database.connect()
        print("Database connection established")

    query = '''
        select ta.id_terminal,u.nome_usuario,u.usuario,u.perfil_usuario, tp.perfil, le.logradouro,le.bairro,le.cidade,le.uf,le.cep
        from 79_terminal_acesso ta, usuario u,linha_endereco le, tb_perfil tp
        where u.id_usuario = ta.id_usuario and
        le.id_operadora = ta.id_operadora and
        le.id_terminal = ta.id_terminal and
        u.perfil_usuario = tp.id_perfil and
        ta.id_usuario = :cpf limit 1;
    '''

    result = await database.fetch_one(query, values={"cpf": user_id})

    user_info = UserInfo(
        name=result[1],
        user=result[2],
        idTerminal=result[0],
        cpf=result[3],
        address=f"{result[5]}, {result[6]}, {result[7]}-{result[8]}",
        cep=result[9]
    )

    return user_info


@app.get("/user-dependents/{cpf}")
async def user_dependents(cpf: str):
    if not database.is_connected:
        await database.connect()
        print("Database connection established")

    query = '''
        select sum(vlrreceber) as recieveTotal
        from 79_titulos_a_receber
        where id_titular = :cpf
    '''

    result = await database.fetch_one(query, values={"cpf": cpf})

    return result


@app.get("/solicitations/{cpf}")
async def solicitations(cpf: str):
    if not database2.is_connected:
        await database2.connect()
        print("Database connection established")

    query = '''
        select s.solicitacao_id,s.id_operadora,s.linha_numero,s.solicitacao_tipo,
        s.solicitacao_data,s.solicitacao_status,o.operadora
        from solicitacao s, operadora o
        where s.id_operadora = o.id_operadora and associado_cpf = :cpf
        order by s.solicitacao_data desc limit 10
    '''

    result = await database2.fetch_all(query, values={"cpf": cpf})

    return result


@app.get("/user-phones/{cpf}")
async def user_phones(cpf: str):
    if not database.is_connected:
        await database.connect()
        print("Database connection established")

    query = '''
        select ta.id_terminal,o.id_operadora,o.operadora
        from 79_terminal_acesso ta, operadora o
        where ta.id_usuario = :cpf and
        ta.id_operadora = o.id_operadora and
        (ta.id_plano_coop <> 0)
        order by operadora,id_terminal
    '''

    result = await database.fetch_all(query, values={"cpf": cpf})

    user_phones = []

    for r in result:
        phone = PhoneLine(r[0], r[1], r[2])

        user_phones.append(phone)

    return user_phones


@app.get("/internet-use/{line}")
async def internet_use(line: str):
    if not database.is_connected:
        await database.connect()
        print("Database connection established")

    query = """
        SELECT tta.referencia, 
               CONCAT(SUBSTRING(tta.referencia,4,4),SUBSTRING(tta.referencia,1,2)) AS ultima_ref 
        FROM 79_total_terminal_acesso tta, 79_fatura_conta fa 
        WHERE tta.id_terminal = :line 
            AND tta.fatura = fa.fatura 
            AND fa.id_operadora = 3 
            AND fa.referencia = tta.referencia 
            AND fa.ind_liberacao = "S" 
        ORDER BY 1 DESC 
        LIMIT 1
    """

    result = await database.fetch_one(query, values={"line": line})

    table_name = f"79_{result[1]}_detalhe_conta"

    query2 = f'''
        SELECT d.referencia, d.id_operadora, d.id_terminal,
            SUM(REPLACE(REPLACE(REPLACE(d.num_telefone_destino, ".", ""), "kb", ""), ",", "")/1000) AS quantidade_dur_kb,
            SUM(d.duracao) AS quantidade_dur
        FROM {table_name} d
        WHERE d.referencia = :ref
            AND d.id_operadora = 3
            AND d.id_terminal = :line
            AND (d.classificacao = "DDS" OR d.classificacao = "INTERNET")
            AND (d.tipo_servico = "USO" OR d.tipo_servico = "DADOS")
    '''

    result2 = await database.fetch_one(query2, values={"ref": result[0], "line": line})

    query3 = f'''
        select d.descricao_servico,sum(d.valor_retarifado) as valor
        from {table_name} d
        where d.referencia = :ref and d.id_terminal = :line and
        d.id_operadora = 3
        group by d.descricao_servico order by 2 desc
    '''

    result3 = await database.fetch_all(query3, values={"ref": result[0], "line": line})

    other_services = []

    for service in result3:
        aux = Service(service[0], service[1])

        other_services.append(aux)

    data = PhoneServices(result2[3], other_services)

    return data

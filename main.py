from urllib import response

from fastapi import FastAPI
from sqlalchemy import MetaData, Table, Column, Integer, String, select, ForeignKey, Float, DateTime, and_, or_
from models import User, UserInfo, OpenBilling, PaidBilling, PhoneLine
import databases
import os

DB_HOST = os.environ.get("DB_HOST", "bdhost0061.servidorwebfacil.com")
DB_PORT = os.environ.get("DB_PORT", "3306")
DB_NAME = os.environ.get("DB_NAME", "aptoubat_teleconta")
DB_USER = os.environ.get("DB_USER", "aptoubat")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "River@2304")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

database = databases.Database(DATABASE_URL)
metadata = MetaData()


users = Table(
    "usuario",
    metadata,
    Column("id_empresa", Integer),
    Column("usuario", String(50)),
    Column("nome_usuario", String(50)),
    Column("nick", String(50)),
    Column("senha", String(50)),
    Column("ind_ativo", String(3)),
    Column("cpf", String(11)),
    Column("id_usuario", Integer, primary_key=True),
    Column("perfil_usuario", Integer),
)

company = Table(
    "empresa",
    metadata,
    Column("id_empresa", Integer),
    Column("razao_social", String(50)),
)

title_to_recieve = Table(
    "79_titulos_a_receber",
    metadata,
    Column("nosso_numero", String),
    Column("id_terminal", String),
    Column("data_venc", DateTime),
    Column("vlrreceber", Float),
    Column("status", String),
    Column("id_usuario", String),
    Column("referencia", String),
    Column("seunumero", String),
)

bill_to_recieve = Table(
    "79_conta_receber",
    metadata,
    Column("linha_digitavel_cod_barras", String),
    Column("nosso_numero", String),
    Column("id_terminal", String),
    Column("referencia", String),
    Column("data_venc", DateTime),
    Column("id_usuario", String),
    Column("ind_liberacao", String),
    Column("valor_conta", Float),
    Column("status", String),
    Column("data_quit", DateTime),
    Column("valor_quitado", Float),
    Column("valor_juros", Float),
    Column("seunumero", String),
)


terminal_acesso = Table(
    "79_terminal_acesso",
    metadata,
    Column("id_terminal", Integer, primary_key=True),
    Column("id_usuario", Integer, ForeignKey("usuario.id_usuario")),
    Column("id_operadora", Integer),
    Column("id_plano_coop", Integer)
)

linha_endereco = Table(
    "linha_endereco",
    metadata,
    Column("id_operadora", Integer),
    Column("id_terminal", Integer),
    Column("logradouro", String),
    Column("bairro", String),
    Column("cidade", String),
    Column("uf", String),
    Column("cep", String),
)

tb_perfil = Table(
    "tb_perfil",
    metadata,
    Column("id_perfil", Integer, primary_key=True),
    Column("perfil", String),
)

tb_operadora = Table(
    "operadora",
    metadata,
    Column("operadora", String),
    Column("id_operadora", Integer)
)

total_terminal_access = Table(
    "79_total_terminal_acesso",
    metadata,
    Column("referencia", String),
    Column("id_terminal", String),
    Column("fatura", String),
)

bill_fature = Table(
    "79_fatura_conta",
    metadata,
    Column("id_operadora", String),
    Column("referencia", String),
    Column("id_operadora", String),
    Column("ind_liberacao", String)
)

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
    print(cpf)
    query = select(
        users.c.cpf,
        users.c.usuario,
        users.c.nome_usuario,
        users.c.senha,
        users.c.nick,
        company.c.razao_social
    ).where(
        (users.c.id_empresa == company.c.id_empresa)
        & (users.c.ind_ativo == "SIM")
        & (users.c.cpf == cpf)
    )

    result = await database.fetch_one(query)

    user = User(
        cpf=result[0],
        id=result[0],
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

    query = (
        select(
            title_to_recieve.c.nosso_numero,
            title_to_recieve.c.id_terminal,
            title_to_recieve.c.data_venc,
            title_to_recieve.c.vlrreceber,
            title_to_recieve.c.status,
            bill_to_recieve.c.linha_digitavel_cod_barras,
        )
        .where(
            and_(
                title_to_recieve.c.id_usuario == cpf,
                title_to_recieve.c.nosso_numero != 0,
                title_to_recieve.c.nosso_numero == bill_to_recieve.c.nosso_numero,
                title_to_recieve.c.referencia == bill_to_recieve.c.referencia,
                title_to_recieve.c.id_usuario == bill_to_recieve.c.id_usuario,
                bill_to_recieve.c.ind_liberacao == 'S',
            )
        )
        .order_by(title_to_recieve.c.seunumero.desc(), title_to_recieve.c.id_terminal)
    )

    result = await database.fetch_all(query)

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

    query = (
        select(
            bill_to_recieve.c.nosso_numero,
            bill_to_recieve.c.id_terminal,
            bill_to_recieve.c.data_venc,
            bill_to_recieve.c.valor_conta,
            bill_to_recieve.c.status,
            bill_to_recieve.c.data_quit,
            bill_to_recieve.c.valor_quitado,
            bill_to_recieve.c.valor_juros,
        )
        .where(
            and_(
                bill_to_recieve.c.id_usuario == cpf,
                or_(
                    bill_to_recieve.c.valor_quitado > 0,
                    bill_to_recieve.c.valor_quitado < 0,
                    bill_to_recieve.c.data_quit.is_not(None),
                ),
            )
        )
        .order_by(bill_to_recieve.c.seunumero.desc())
        .limit(6)
    )

    result = await database.fetch_all(query)

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

    query = (
        select(
            terminal_acesso.c.id_terminal,
            users.c.nome_usuario,
            users.c.usuario,
            users.c.perfil_usuario,
            tb_perfil.c.perfil,
            linha_endereco.c.logradouro,
            linha_endereco.c.bairro,
            linha_endereco.c.cidade,
            linha_endereco.c.uf,
            linha_endereco.c.cep,
        )
        .where(
            (users.c.id_usuario == terminal_acesso.c.id_usuario)
            & (linha_endereco.c.id_operadora == terminal_acesso.c.id_operadora)
            & (linha_endereco.c.id_terminal == terminal_acesso.c.id_terminal)
            & (users.c.perfil_usuario == tb_perfil.c.id_perfil)
            & (terminal_acesso.c.id_usuario == user_id)
        )
        .limit(1)
    )

    result = await database.fetch_one(query)

    user_info = UserInfo(
        name=result[1],
        user=result[2],
        idTerminal=result[0],
        cpf=result[3],
        address=f"{result[5]}, {result[6]}, {result[7]}-{result[8]}",
        cep=result[9]
    )

    return user_info


@app.get("/user-phones/{cpf}")
async def user_phones(cpf):
    if not database.is_connected:
        await database.connect()
        print("Database connection established")

    query = (
        select(
            terminal_acesso.c.id_terminal,
            tb_operadora.c.id_operadora,
            tb_operadora.c.operadora
        )
        .where(
            and_(
                terminal_acesso.c.id_usuario == cpf,
                terminal_acesso.c.id_operadora == tb_operadora.c.id_operadora,
                terminal_acesso.c.id_plano_coop != 0
            )
        )
        .order_by(tb_operadora.c.operadora, terminal_acesso.c.id_terminal)
    )

    result = await database.fetch_all(query)

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

    print(result[0])
    print(result[1])

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

    result2 = await database.fetch_all(query2, values={"ref": result[0], "line": line})

    return result2

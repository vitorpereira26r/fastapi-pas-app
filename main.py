from urllib import response

from fastapi import FastAPI
from sqlalchemy import MetaData, Table, Column, Integer, String, select, ForeignKey, Float, DateTime, and_, or_
from models import User, UserInfo, OpenBilling, PaidBilling
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
    await database.connect()
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
    await database.connect()
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
async def get_paid_billings(cpf: str):
    await database.connect()
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
    await database.connect()

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

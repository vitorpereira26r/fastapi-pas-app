class User:
    def __init__(self, id, name, nick, username, cpf):
        self.id = id
        self.name = name
        self.nick = nick
        self.username = username
        self.cpf = cpf


class UserInfo:
    def __init__(self, name, user, idTerminal, cpf, address, cep):
        self.name = name
        self.user = user
        self.idTerminal = idTerminal
        self.cpf = cpf
        self.address = address
        self.cep = cep


class PaidBilling:
    def __init__(self, ourNumber, idTerminal, vencDate, value, status, dataQuit, paidValue, feesValue):
        self.ourNumber = ourNumber
        self.idTerminal = idTerminal
        self.vencDate = vencDate
        self.value = value
        self.status = status
        self.dataQuit = dataQuit
        self.paidValue = paidValue
        self.feesValue = feesValue


class OpenBilling:
    def __init__(self, id, idTerminal, dateVenc, value, status, code):
        self.id = id
        self.idTerminal = idTerminal
        self.dateVenc = dateVenc
        self.status = status
        self.code = code
        self.value = value
